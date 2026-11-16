package beads

import (
	"bytes"
	"context"
	"encoding/json"

	beads "github.com/steveyegge/beads"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// k8sProtoMagic prefixes every object the apiserver stores in etcd as protobuf:
// the bytes "k8s\x00", then a protobuf-encoded runtime.Unknown wrapping the
// object. (A value without it — a kine-internal key — is not a k8s object.)
var k8sProtoMagic = []byte{0x6b, 0x38, 0x73, 0x00}

// parseK8sMeta cracks a stored object's protobuf open just far enough to read
// its ObjectMeta: the object's own UID and the UIDs of its ownerReferences. It
// decodes ONLY metadata (via PartialObjectMetadata), so it needs no per-Kind
// schema and ignores spec/status/data entirely. ok is false for anything that
// is not a k8s protobuf object.
func parseK8sMeta(value []byte) (uid string, ownerUIDs []string, ok bool) {
	// Built-in types are stored as protobuf (k8s\x00 magic); custom resources
	// (CRDs — Cluster, Machine, MirageCluster, cert-manager/crossplane/… CRs) are
	// stored as JSON. Both carry metadata.uid and metadata.ownerReferences.
	if bytes.HasPrefix(value, k8sProtoMagic) {
		var unk runtime.Unknown
		if err := unk.Unmarshal(value[len(k8sProtoMagic):]); err != nil {
			return "", nil, false
		}
		var pom metav1.PartialObjectMetadata
		if err := pom.Unmarshal(unk.Raw); err != nil {
			return "", nil, false
		}
		uid = string(pom.UID)
		for i := range pom.OwnerReferences {
			if u := string(pom.OwnerReferences[i].UID); u != "" {
				ownerUIDs = append(ownerUIDs, u)
			}
		}
		return uid, ownerUIDs, true
	}
	if len(value) > 0 && value[0] == '{' {
		var jm struct {
			Metadata struct {
				UID             string `json:"uid"`
				OwnerReferences []struct {
					UID string `json:"uid"`
				} `json:"ownerReferences"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(value, &jm); err != nil {
			return "", nil, false
		}
		for _, o := range jm.Metadata.OwnerReferences {
			if o.UID != "" {
				ownerUIDs = append(ownerUIDs, o.UID)
			}
		}
		return jm.Metadata.UID, ownerUIDs, true
	}
	return "", nil, false
}

// wireOwnership records key's object UID and draws a beads parent-child edge
// from this bead (the owned object, the child) to each owner's bead. Owners that
// have not been seen yet are deferred and wired when they arrive. Must be called
// under b.mu.
func (b *beadsLog) wireOwnership(ctx context.Context, key, beadID string, value []byte) {
	uid, ownerUIDs, ok := parseK8sMeta(value)
	if !ok {
		return
	}
	if uid != "" {
		b.uidToKey[uid] = key
		// Any children that arrived before this owner can now be wired.
		if waiters := b.pending[uid]; len(waiters) > 0 {
			for _, childID := range waiters {
				b.addParentChild(ctx, childID, beadID)
			}
			delete(b.pending, uid)
		}
	}
	for _, ownerUID := range ownerUIDs {
		if ownerKey, found := b.uidToKey[ownerUID]; found {
			if oe, ok := b.index[ownerKey]; ok {
				b.addParentChild(ctx, beadID, oe.id)
			}
		} else {
			b.pending[ownerUID] = append(b.pending[ownerUID], beadID)
		}
	}
}

// addParentChild adds a parent-child dependency: the child bead (IssueID) is a
// child of the parent/owner bead (DependsOnID). Edges are best-effort graph
// decoration, so an error (including an edge that already exists on a restart)
// is logged at trace and otherwise ignored.
func (b *beadsLog) addParentChild(ctx context.Context, childID, parentID string) {
	if childID == "" || parentID == "" || childID == parentID {
		return
	}
	err := b.store.AddDependency(ctx, &beads.Dependency{
		IssueID:     childID,
		DependsOnID: parentID,
		Type:        beads.DepParentChild,
	}, actor)
	if err != nil {
		logrus.Tracef("beads: add parent-child %s->%s: %v", childID, parentID, err)
	}
}

// backfillOwnership wires the ownership graph for the beads already in the index
// at Start, so a restart lights up the whole existing cluster's edges (nothing
// is otherwise backfilled). Two passes: learn every object's UID, then draw the
// edges. Must be called under b.mu.
func (b *beadsLog) backfillOwnership(ctx context.Context) {
	for key, ent := range b.index {
		if uid, _, ok := parseK8sMeta(ent.r.Value); ok && uid != "" {
			b.uidToKey[uid] = key
		}
	}
	edges := 0
	for _, ent := range b.index {
		_, ownerUIDs, ok := parseK8sMeta(ent.r.Value)
		if !ok {
			continue
		}
		for _, ownerUID := range ownerUIDs {
			if ownerKey, found := b.uidToKey[ownerUID]; found {
				b.addParentChild(ctx, ent.id, b.index[ownerKey].id)
				edges++
			}
		}
	}
	if edges > 0 {
		logrus.Infof("Kine beads driver: backfilled %d ownerReference edges", edges)
	}
}
