package memory

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
	"k8s.io/client-go/util/workqueue"
)

const ttlRetryInterval = 250 * time.Millisecond

func init() {
	drivers.Register("memory", New)
}

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("using in-memory backend")
	return false, NewBackend(), nil
}

type entry struct {
	revision       int64
	key            string
	value          []byte
	createRevision int64
	version        int64
	prevRevision   int64
	lease          int64
	created        bool
	deleted        bool
	prev           *entry
}

func (e *entry) toKeyValue() *server.KeyValue {
	return &server.KeyValue{
		Key:            e.key,
		Value:          e.value,
		CreateRevision: e.createRevision,
		ModRevision:    e.revision,
		Version:        e.version,
		Lease:          e.lease,
	}
}

type ttlEventKV struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

type Backend struct {
	mu              sync.RWMutex
	currentRevision int64
	compactRevision int64

	log  []*entry
	keys *btree.Map[string, []*entry]

	notifyMu sync.Mutex
	notifyCh chan struct{}
}

var _ server.Backend = (*Backend)(nil)

func NewBackend() *Backend {
	return &Backend{
		keys:     btree.NewMap[string, []*entry](0),
		notifyCh: make(chan struct{}),
	}
}

func (b *Backend) Start(ctx context.Context) error {
	// Seed the same startup entries that SQL-backed and NATS backends do:
	//   1. compact_rev_key — written by SQLLog.compactStart; gives the backend
	//      a non-zero starting revision (apiserver rejects rev=0).
	//   2. /registry/health — written by LogStructured.Start; the apiserver
	//      uses it as a liveness probe.
	b.mu.Lock()
	rev := b.nextRevision()
	b.appendEntry(&entry{
		revision:       rev,
		key:            "compact_rev_key",
		createRevision: rev,
		version:        1,
		created:        true,
	})
	rev = b.nextRevision()
	b.appendEntry(&entry{
		revision:       rev,
		key:            "/registry/health",
		value:          []byte(`{"health":"true"}`),
		createRevision: rev,
		version:        1,
		created:        true,
	})
	b.mu.Unlock()

	go b.ttl(ctx)
	return nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.currentRevision, nil
}

func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var size int64
	for _, e := range b.log {
		size += int64(len(e.key) + len(e.value) + 64)
	}
	return size, nil
}

func (b *Backend) WaitForSyncTo(revision int64) {}

func (b *Backend) nextRevision() int64 {
	b.currentRevision++
	return b.currentRevision
}

func (b *Backend) appendEntry(e *entry) {
	b.log = append(b.log, e)
	hist, _ := b.keys.Get(e.key)
	if len(hist) > 0 {
		e.prev = hist[len(hist)-1]
	}
	b.keys.Set(e.key, append(hist, e))
}

func (b *Backend) broadcast() {
	b.notifyMu.Lock()
	close(b.notifyCh)
	b.notifyCh = make(chan struct{})
	b.notifyMu.Unlock()
}

func (b *Backend) getNotifyCh() <-chan struct{} {
	b.notifyMu.Lock()
	ch := b.notifyCh
	b.notifyMu.Unlock()
	return ch
}

func (b *Backend) latest(key string) *entry {
	hist, ok := b.keys.Get(key)
	if !ok || len(hist) == 0 {
		return nil
	}
	return hist[len(hist)-1]
}

func (b *Backend) atRevision(key string, revision int64) *entry {
	hist, ok := b.keys.Get(key)
	if !ok {
		return nil
	}
	var result *entry
	for _, e := range hist {
		if e.revision <= revision {
			result = e
		} else {
			break
		}
	}
	return result
}

// logIndexAfter returns the index of the first log entry with revision > rev.
// Caller must hold at least a read lock on b.mu.
func (b *Backend) logIndexAfter(rev int64) int {
	return sort.Search(len(b.log), func(i int) bool {
		return b.log[i].revision > rev
	})
}

// ttl runs a long-lived goroutine that watches the backend for entries with
// a non-zero Lease and schedules them for deletion once their TTL expires.
// It mirrors logstructured.LogStructured.ttl: an initial list seeds the
// delaying workqueue with any pre-existing leased keys, then a watch picks
// up new ones. A handler goroutine consumes the queue and calls Delete when
// each entry reaches its expiration time.
func (b *Backend) ttl(ctx context.Context) {
	queue := workqueue.NewTypedDelayingQueue[string]()
	var rwMu sync.RWMutex
	store := make(map[string]*ttlEventKV)

	go func() {
		for b.handleTTLEvent(ctx, &rwMu, queue, store) {
		}
	}()

	rev, kvs, err := b.List(ctx, "/", "", 0, 0, false)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Errorf("TTL initial list failed: %v", err)
		}
		queue.ShutDown()
		return
	}
	for _, kv := range kvs {
		if kv.Lease <= 0 {
			continue
		}
		expires := storeTTLEventKV(&rwMu, store, kv)
		logrus.Tracef("TTL add event key=%v, modRev=%v, ttl=%v", kv.Key, kv.ModRevision, expires)
		queue.AddAfter(kv.Key, expires)
	}

	// Watch from rev+1 to avoid replaying entries we just observed via
	// List. Anything appended after List ran has a strictly greater
	// revision and will be picked up here.
	wr := b.Watch(ctx, "/", rev+1)
	if wr.CompactRevision != 0 {
		logrus.Errorf("TTL event watch failed: %v", server.ErrCompacted)
		queue.ShutDown()
		return
	}

	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		case events, ok := <-wr.Events:
			if !ok {
				queue.ShutDown()
				return
			}
			for _, event := range events {
				if event.Delete || event.KV.Lease <= 0 {
					continue
				}
				kv := event.KV
				stored := loadTTLEventKV(&rwMu, store, kv.Key)
				if stored == nil {
					expires := storeTTLEventKV(&rwMu, store, kv)
					logrus.Tracef("TTL add event key=%v, modRev=%v, ttl=%v", kv.Key, kv.ModRevision, expires)
					queue.AddAfter(kv.Key, expires)
				} else if kv.ModRevision > stored.modRevision {
					expires := storeTTLEventKV(&rwMu, store, kv)
					logrus.Tracef("TTL update event key=%v, modRev=%v, ttl=%v", kv.Key, kv.ModRevision, expires)
					queue.AddAfter(kv.Key, expires)
				}
			}
		}
	}
}

func (b *Backend) handleTTLEvent(ctx context.Context, mu *sync.RWMutex, queue workqueue.TypedDelayingInterface[string], store map[string]*ttlEventKV) bool {
	key, shutdown := queue.Get()
	if shutdown {
		logrus.Info("TTL events work queue has shut down")
		return false
	}
	defer queue.Done(key)

	kv := loadTTLEventKV(mu, store, key)
	if kv == nil {
		logrus.Errorf("TTL event not found for key=%v", key)
		return true
	}

	if expires := time.Until(kv.expiredAt); expires > 0 {
		logrus.Tracef("TTL has not expired for key=%v, ttl=%v, requeuing", key, expires)
		queue.AddAfter(key, expires)
		return true
	}

	logrus.Tracef("TTL delete key=%v, modRev=%v", kv.key, kv.modRevision)
	if _, _, _, err := b.Delete(ctx, kv.key, kv.modRevision); err != nil && !errors.Is(err, context.Canceled) {
		logrus.Errorf("TTL delete trigger failed for key=%v: %v, requeuing", kv.key, err)
		queue.AddAfter(kv.key, ttlRetryInterval)
		return true
	}

	mu.Lock()
	defer mu.Unlock()
	delete(store, kv.key)
	return true
}

func loadTTLEventKV(mu *sync.RWMutex, store map[string]*ttlEventKV, key string) *ttlEventKV {
	mu.RLock()
	defer mu.RUnlock()
	return store[key]
}

func storeTTLEventKV(mu *sync.RWMutex, store map[string]*ttlEventKV, kv *server.KeyValue) time.Duration {
	mu.Lock()
	defer mu.Unlock()
	expires := time.Duration(kv.Lease) * time.Second
	store[kv.Key] = &ttlEventKV{
		key:         kv.Key,
		modRevision: kv.ModRevision,
		expiredAt:   time.Now().Add(expires),
	}
	return expires
}

// Get returns the current revision and the KeyValue for the given key.
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	if strings.HasSuffix(key, "/") && rangeEnd == "" {
		key = key[:len(key)-1]
	}
	rev, kvs, err := b.List(ctx, key, rangeEnd, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}
	if len(kvs) == 0 {
		return rev, nil, nil
	}
	return rev, kvs[0], nil
}

func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	latest := b.latest(key)
	if latest != nil && !latest.deleted {
		return b.currentRevision, server.ErrKeyExists
	}

	rev := b.nextRevision()
	var prevRev int64
	if latest != nil {
		prevRev = latest.revision
	}

	b.appendEntry(&entry{
		revision:       rev,
		key:            key,
		value:          value,
		createRevision: rev,
		version:        1,
		prevRevision:   prevRev,
		lease:          lease,
		created:        true,
	})
	b.broadcast()
	return rev, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	latest := b.latest(key)
	if latest == nil || latest.deleted {
		return b.currentRevision, nil, false, nil
	}
	if latest.revision != revision {
		return b.currentRevision, latest.toKeyValue(), false, nil
	}

	rev := b.nextRevision()
	e := &entry{
		revision:       rev,
		key:            key,
		value:          value,
		createRevision: latest.createRevision,
		version:        latest.version + 1,
		prevRevision:   latest.revision,
		lease:          lease,
	}
	b.appendEntry(e)
	b.broadcast()
	return rev, e.toKeyValue(), true, nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	latest := b.latest(key)
	if latest == nil || latest.deleted {
		return b.currentRevision, nil, false, nil
	}
	if revision != 0 && latest.revision != revision {
		return b.currentRevision, latest.toKeyValue(), false, nil
	}

	rev := b.nextRevision()
	b.appendEntry(&entry{
		revision:       rev,
		key:            key,
		value:          latest.value,
		createRevision: latest.createRevision,
		version:        latest.version,
		prevRevision:   latest.revision,
		lease:          latest.lease,
		deleted:        true,
	})
	b.broadcast()
	return rev, latest.toKeyValue(), true, nil
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rev := b.currentRevision
	if revision > 0 {
		if revision > b.currentRevision {
			return rev, nil, server.ErrFutureRev
		}
		if revision < b.compactRevision {
			return rev, nil, server.ErrCompacted
		}
		rev = revision
	}

	seek, exact := seekKey(prefix, startKey)
	iter := b.keys.Iter()
	if !iter.Seek(seek) {
		return rev, nil, nil
	}

	var kvs []*server.KeyValue
	for {
		k := iter.Key()
		if exact && k != seek {
			break
		}
		if !strings.HasPrefix(k, prefix) {
			break
		}

		var e *entry
		if revision > 0 {
			e = b.atRevision(k, revision)
		} else {
			e = b.latest(k)
		}

		if e != nil && !e.deleted {
			kv := e.toKeyValue()
			if keysOnly {
				kv.Value = nil
			}
			kvs = append(kvs, kv)
		}

		if !iter.Next() {
			break
		}
	}
	return rev, kvs, nil
}

func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	rev, kvs, err := b.List(ctx, prefix, startKey, 0, revision, true)
	if err != nil {
		return rev, 0, err
	}
	return rev, int64(len(kvs)), nil
}

func (b *Backend) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	b.mu.RLock()
	rev := b.currentRevision
	compactRev := b.compactRevision
	b.mu.RUnlock()

	events := make(chan []*server.Event, 100)

	if startRevision > 0 && startRevision <= compactRev {
		close(events)
		return server.WatchResult{
			CurrentRevision: rev,
			CompactRevision: compactRev,
			Events:          events,
		}
	}

	go func() {
		defer close(events)

		// lastSeen is the highest revision already delivered to the caller.
		// Subsequent passes start from the first log entry whose revision is
		// greater than lastSeen, looked up via logIndexAfter — direct array
		// indexing by revision no longer holds once compaction trims b.log.
		lastSeen := startRevision - 1
		if lastSeen < 0 {
			lastSeen = rev
		}

		for {
			notifyCh := b.getNotifyCh()

			b.mu.RLock()
			var batch []*server.Event
			for i := b.logIndexAfter(lastSeen); i < len(b.log); i++ {
				e := b.log[i]
				if !strings.HasPrefix(e.key, prefix) {
					lastSeen = e.revision
					continue
				}

				event := &server.Event{
					Create: e.created,
					Delete: e.deleted,
					KV:     e.toKeyValue(),
					PrevKV: &server.KeyValue{ModRevision: e.prevRevision},
				}
				if e.prev != nil {
					event.PrevKV = e.prev.toKeyValue()
				}

				batch = append(batch, event)
				lastSeen = e.revision
			}
			b.mu.RUnlock()

			if len(batch) > 0 {
				select {
				case events <- batch:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-notifyCh:
			case <-ctx.Done():
				return
			}
		}
	}()

	watchRev := startRevision
	if watchRev <= 0 {
		watchRev = rev
	}

	return server.WatchResult{
		CurrentRevision: watchRev,
		Events:          events,
	}
}

// Compact discards history below the given revision. For each key the latest
// entry with revision <= the compact target is preserved as the floor (so
// reads at the compact boundary still resolve), unless that entry is a
// tombstone — in which case the entire key history is removed. Entries with
// revision > the compact target are always retained. The log slice is
// rebuilt to drop any entries that are no longer referenced.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if revision > b.currentRevision {
		return b.currentRevision, nil
	}
	if revision <= b.compactRevision {
		return b.currentRevision, nil
	}

	keep := make(map[*entry]struct{})
	var toRemove []string

	b.keys.Scan(func(key string, hist []*entry) bool {
		floorIdx := -1
		for i, e := range hist {
			if e.revision <= revision {
				floorIdx = i
			} else {
				break
			}
		}

		var newHist []*entry
		switch {
		case floorIdx < 0:
			// No entry at or below the compact boundary; keep everything.
			newHist = hist
		case hist[floorIdx].deleted:
			// Floor is a tombstone — drop it and everything before; readers
			// at the compact boundary correctly see the key as absent.
			newHist = hist[floorIdx+1:]
		default:
			newHist = hist[floorIdx:]
		}

		if len(newHist) == 0 {
			toRemove = append(toRemove, key)
		} else {
			if len(newHist) != len(hist) {
				newHist[0].prev = nil
				b.keys.Set(key, newHist)
			}
			for _, e := range newHist {
				keep[e] = struct{}{}
			}
		}
		return true
	})

	for _, k := range toRemove {
		b.keys.Delete(k)
	}

	filtered := b.log[:0]
	for _, e := range b.log {
		if _, ok := keep[e]; ok {
			filtered = append(filtered, e)
		}
	}
	for i := len(filtered); i < len(b.log); i++ {
		b.log[i] = nil
	}
	b.log = filtered

	b.compactRevision = revision
	return b.currentRevision, nil
}

// seekKey returns the BTree seek target for a List/Range query and a flag
// indicating whether the iteration should match the seek key exactly. When
// startKey is empty (or equal to prefix), iteration begins at prefix; if
// prefix has no trailing slash this is treated as an exact-key lookup.
// Otherwise startKey is normalized into the prefix's namespace and used as
// the resume point for paginated range scans.
func seekKey(prefix, startKey string) (string, bool) {
	exact := !strings.HasSuffix(prefix, "/") && startKey == ""
	if startKey == "" || startKey == prefix {
		return prefix, exact
	}
	base := strings.TrimSuffix(prefix, "/")
	startKey = strings.TrimPrefix(startKey, base)
	startKey = strings.TrimPrefix(startKey, "/")
	if startKey != "" {
		return base + "/" + startKey, false
	}
	return prefix, exact
}
