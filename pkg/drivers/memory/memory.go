package memory

import (
	"container/heap"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

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

type Backend struct {
	mu              sync.RWMutex
	currentRevision int64
	compactRevision int64

	log  []*entry
	keys *btree.Map[string, []*entry]

	notifyMu sync.Mutex
	notifyCh chan struct{}

	expireMu   sync.Mutex
	expires    expireHeap
	expireWake chan struct{}
}

var _ server.Backend = (*Backend)(nil)

func NewBackend() *Backend {
	return &Backend{
		keys:       btree.NewMap[string, []*entry](0),
		notifyCh:   make(chan struct{}),
		expires:    make(expireHeap, 0),
		expireWake: make(chan struct{}, 1),
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

	go b.expireLoop(ctx)
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

func (b *Backend) scheduleExpire(key string, rev, lease int64) {
	if lease <= 0 {
		return
	}
	b.expireMu.Lock()
	heap.Push(&b.expires, &expireEntry{
		key:     key,
		rev:     rev,
		expires: time.Now().Add(time.Duration(lease) * time.Second),
	})
	b.expireMu.Unlock()
	select {
	case b.expireWake <- struct{}{}:
	default:
	}
}

func (b *Backend) expireLoop(ctx context.Context) {
	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	for {
		b.expireMu.Lock()
		if b.expires.Len() == 0 {
			b.expireMu.Unlock()
			stopTimer()
			select {
			case <-ctx.Done():
				return
			case <-b.expireWake:
				continue
			}
		}

		next := b.expires[0]
		wait := time.Until(next.expires)
		if wait <= 0 {
			heap.Pop(&b.expires)
			b.expireMu.Unlock()
			b.Delete(ctx, next.key, next.rev)
			continue
		}
		b.expireMu.Unlock()

		if timer == nil {
			timer = time.NewTimer(wait)
			timerC = timer.C
		} else {
			timer.Reset(wait)
		}

		select {
		case <-ctx.Done():
			stopTimer()
			return
		case <-b.expireWake:
			stopTimer()
		case <-timerC:
		}
	}
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

	latest := b.latest(key)
	if latest != nil && !latest.deleted {
		rev := b.currentRevision
		b.mu.Unlock()
		return rev, server.ErrKeyExists
	}

	rev := b.nextRevision()
	var prevRev int64
	if latest != nil {
		prevRev = latest.revision
	}

	e := &entry{
		revision:       rev,
		key:            key,
		value:          value,
		createRevision: rev,
		version:        1,
		prevRevision:   prevRev,
		lease:          lease,
		created:        true,
	}
	b.appendEntry(e)
	b.broadcast()
	b.mu.Unlock()

	b.scheduleExpire(key, rev, lease)
	return rev, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	b.mu.Lock()

	latest := b.latest(key)
	if latest == nil || latest.deleted {
		rev := b.currentRevision
		b.mu.Unlock()
		return rev, nil, false, nil
	}
	if latest.revision != revision {
		rev := b.currentRevision
		kv := latest.toKeyValue()
		b.mu.Unlock()
		return rev, kv, false, nil
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
	kv := e.toKeyValue()
	b.mu.Unlock()

	b.scheduleExpire(key, rev, lease)
	return rev, kv, true, nil
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
	e := &entry{
		revision:       rev,
		key:            key,
		value:          latest.value,
		createRevision: latest.createRevision,
		version:        latest.version,
		prevRevision:   latest.revision,
		lease:          latest.lease,
		deleted:        true,
	}
	b.appendEntry(e)
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

		lastSeen := startRevision - 1
		if lastSeen < 0 {
			lastSeen = rev
		}

		for {
			notifyCh := b.getNotifyCh()

			b.mu.RLock()
			var batch []*server.Event
			startIdx := lastSeen
			if startIdx < 0 {
				startIdx = 0
			}
			for i := startIdx; i < int64(len(b.log)); i++ {
				e := b.log[i]
				if !strings.HasPrefix(e.key, prefix) {
					continue
				}

				event := &server.Event{
					Create: e.created,
					Delete: e.deleted,
					KV:     e.toKeyValue(),
					PrevKV: &server.KeyValue{ModRevision: e.prevRevision},
				}

				if e.prevRevision > 0 && int(e.prevRevision) <= len(b.log) {
					event.PrevKV = b.log[e.prevRevision-1].toKeyValue()
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

func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if revision > b.currentRevision {
		return b.currentRevision, nil
	}
	b.compactRevision = revision
	return b.currentRevision, nil
}

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

// expireEntry is a key scheduled for TTL expiration.
type expireEntry struct {
	key     string
	rev     int64
	expires time.Time
}

// expireHeap is a min-heap ordered by expiration time.
type expireHeap []*expireEntry

func (h expireHeap) Len() int            { return len(h) }
func (h expireHeap) Less(i, j int) bool   { return h[i].expires.Before(h[j].expires) }
func (h expireHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *expireHeap) Push(x any)          { *h = append(*h, x.(*expireEntry)) }
func (h *expireHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
