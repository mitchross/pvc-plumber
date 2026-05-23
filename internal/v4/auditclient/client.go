// Package auditclient provides a controller-runtime client.Client wrapper
// that gates all write operations on the operator's mode. In audit mode
// every Create / Update / Patch / Delete / DeleteAllOf becomes a no-op
// returning nil; the wrapped call is logged as "would-write" and tracked
// in counters so the operator can report what it would have done.
//
// Phase 2.5 of docs/pvc-plumber-v4-prd.md in the talos-argocd-proxmox repo.
//
// Design intent: the Reconciler embeds client.Client without modification.
// When the operator binary is constructed in audit mode, main.go wraps
// mgr.GetClient() with auditclient.New(...) and passes the wrapper into the
// Reconciler. The reconciler logic runs unchanged; the wrapper intercepts
// every write at the lowest possible layer. This means even unenumerated
// write paths (future controllers, accidental Patch calls, etc.) cannot
// reach the API server while audit mode is active.
//
// Read operations (Get, List, Scheme, RESTMapper, GroupVersionKindFor,
// IsObjectNamespaced) are inherited unchanged from the embedded client.
//
// What this wrapper does NOT gate (Phase 2.5 known exceptions):
//
//   - record.EventRecorder writes. Kubernetes Events emitted via the
//     client-go EventRecorder go through a separate sink that does not
//     route through client.Client. The current operator does not use an
//     EventRecorder anywhere in production code (grep'd: zero hits), so
//     this is a defensive note for future contributors. If a future phase
//     wires an EventRecorder, it must be gated by mode independently (e.g.
//     by passing a nil EventRecorder when mode == audit, or wrapping it
//     similarly). The TestAuditMode_NoEventCreation test exercises the
//     direct-Create path on a corev1.Event so accidental Create calls on
//     Event objects through client.Client are caught by the wrapper.
//
//   - Manager leader-election Leases. controller-runtime's leader election
//     writes to a coordination.k8s.io/v1 Lease via an internal lock client
//     constructed from mgr.GetConfig(), not from mgr.GetClient(). The Phase
//     2.5 main.go forcibly sets --leader-elect=false when mode is audit so
//     this path is never reached. If a future caller constructs the
//     manager differently, document the bypass and gate accordingly.
package auditclient

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mitchross/pvc-plumber/internal/v4/mode"
)

// Client wraps a controller-runtime client.Client and gates writes by Mode.
//
// In Mode == mode.Audit:
//   - Create / Update / Patch / Delete / DeleteAllOf return nil without
//     calling the wrapped client. They emit a structured log entry and
//     increment the "would" counters.
//   - Status() and SubResource(...) return wrappers that apply the same
//     gating to sub-resource writes.
//   - Read methods (inherited from the embedded client) pass through.
//
// In all other modes the wrapper is a pure pass-through that ALSO records
// counters under the "did" namespace, so operators can compare expected
// (audit) vs actual (live) write volume from the same metric set.
type Client struct {
	client.Client

	// Mode is the operator's effective mode. Set once at construction;
	// changing it at runtime is not supported (and not needed — the
	// operator restarts to change modes).
	Mode mode.Mode

	// Log is the structured logger used for "would" / "did" entries. Use
	// slog.Default() if unset. The wrapper never logs at Error level on
	// audit no-ops — those are expected and routine.
	Log *slog.Logger

	stats *Stats
}

// Compile-time: ensure we satisfy the full client.Client interface.
var _ client.Client = (*Client)(nil)

// Stats records counter totals. Safe for concurrent use.
type Stats struct {
	wouldCreate      atomic.Int64
	wouldUpdate      atomic.Int64
	wouldPatch       atomic.Int64
	wouldDelete      atomic.Int64
	wouldDeleteAllOf atomic.Int64

	didCreate      atomic.Int64
	didUpdate      atomic.Int64
	didPatch       atomic.Int64
	didDelete      atomic.Int64
	didDeleteAllOf atomic.Int64

	byKindMu sync.Mutex
	byKind   map[string]int64 // "verb/kind" → count (audit "would" only)
}

// New wraps the given controller-runtime client. The Mode parameter is
// captured at construction; pass mode.Audit to disable writes.
func New(wrapped client.Client, m mode.Mode, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		Client: wrapped,
		Mode:   m,
		Log:    log,
		stats: &Stats{
			byKind: map[string]int64{},
		},
	}
}

// WouldWriteTotals returns the running counts of "would-write" calls per
// verb. Safe to call concurrently; values are point-in-time atomic snapshots.
func (c *Client) WouldWriteTotals() WouldWriteSnapshot {
	return WouldWriteSnapshot{
		Create:      c.stats.wouldCreate.Load(),
		Update:      c.stats.wouldUpdate.Load(),
		Patch:       c.stats.wouldPatch.Load(),
		Delete:      c.stats.wouldDelete.Load(),
		DeleteAllOf: c.stats.wouldDeleteAllOf.Load(),
	}
}

// DidWriteTotals is the analog for non-audit modes. Reads return all-zero
// when the wrapper has been in audit mode for its entire lifetime.
func (c *Client) DidWriteTotals() WouldWriteSnapshot {
	return WouldWriteSnapshot{
		Create:      c.stats.didCreate.Load(),
		Update:      c.stats.didUpdate.Load(),
		Patch:       c.stats.didPatch.Load(),
		Delete:      c.stats.didDelete.Load(),
		DeleteAllOf: c.stats.didDeleteAllOf.Load(),
	}
}

// WouldWriteByKind returns a sorted snapshot of per-(verb/kind) counters
// (audit "would" only).
func (c *Client) WouldWriteByKind() []KindCount {
	c.stats.byKindMu.Lock()
	defer c.stats.byKindMu.Unlock()
	out := make([]KindCount, 0, len(c.stats.byKind))
	for k, v := range c.stats.byKind {
		out = append(out, KindCount{VerbKind: k, Count: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VerbKind < out[j].VerbKind })
	return out
}

// WouldWriteSnapshot is a point-in-time view of the counters.
type WouldWriteSnapshot struct {
	Create      int64
	Update      int64
	Patch       int64
	Delete      int64
	DeleteAllOf int64
}

// Total returns the sum of all verbs.
func (s WouldWriteSnapshot) Total() int64 {
	return s.Create + s.Update + s.Patch + s.Delete + s.DeleteAllOf
}

// KindCount is one row of the per-kind histogram.
type KindCount struct {
	VerbKind string
	Count    int64
}

// auditing reports whether the wrapper is currently in audit mode.
func (c *Client) auditing() bool {
	return c.Mode == mode.Audit || c.Mode == mode.Unspecified
}

// kindKey returns the GVK-flavored key used for logs and counters.
// Resolves "PersistentVolumeClaim", "ReplicationSource", etc. from the
// embedded client's scheme.
func (c *Client) kindKey(obj client.Object) string {
	if obj == nil {
		return "<nil>"
	}
	if c.Client == nil {
		return "<unknown>"
	}
	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		// Best-effort: fall back to the runtime type name.
		return "<unknown>"
	}
	if gvk.Group == "" {
		return gvk.Kind
	}
	return gvk.Kind + "." + gvk.Group
}

func (c *Client) incByKind(verb, kind string) {
	c.stats.byKindMu.Lock()
	c.stats.byKind[verb+"/"+kind]++
	c.stats.byKindMu.Unlock()
}

// Create gates the operation by Mode.
func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.auditing() {
		kind := c.kindKey(obj)
		c.stats.wouldCreate.Add(1)
		c.incByKind("create", kind)
		c.Log.Info("audit-mode would-write",
			"verb", "create",
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := c.Client.Create(ctx, obj, opts...)
	if err == nil {
		c.stats.didCreate.Add(1)
	}
	return err
}

// Update gates the operation by Mode.
func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.auditing() {
		kind := c.kindKey(obj)
		c.stats.wouldUpdate.Add(1)
		c.incByKind("update", kind)
		c.Log.Info("audit-mode would-write",
			"verb", "update",
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := c.Client.Update(ctx, obj, opts...)
	if err == nil {
		c.stats.didUpdate.Add(1)
	}
	return err
}

// Patch gates the operation by Mode.
func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.auditing() {
		kind := c.kindKey(obj)
		c.stats.wouldPatch.Add(1)
		c.incByKind("patch", kind)
		c.Log.Info("audit-mode would-write",
			"verb", "patch",
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
			"patchType", string(patch.Type()),
		)
		return nil
	}
	err := c.Client.Patch(ctx, obj, patch, opts...)
	if err == nil {
		c.stats.didPatch.Add(1)
	}
	return err
}

// Delete gates the operation by Mode.
func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if c.auditing() {
		kind := c.kindKey(obj)
		c.stats.wouldDelete.Add(1)
		c.incByKind("delete", kind)
		c.Log.Info("audit-mode would-write",
			"verb", "delete",
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := c.Client.Delete(ctx, obj, opts...)
	if err == nil {
		c.stats.didDelete.Add(1)
	}
	return err
}

// Apply gates the operation by Mode. Server-Side Apply is a write.
func (c *Client) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if c.auditing() {
		// ApplyConfiguration doesn't expose kind/namespace/name via a stable
		// interface — log at a coarser granularity than typed verbs.
		c.stats.wouldUpdate.Add(1) // count Apply under update for the snapshot total
		c.incByKind("apply", "<applyConfig>")
		c.Log.Info("audit-mode would-write",
			"verb", "apply",
			"kind", "<applyConfig>",
		)
		return nil
	}
	err := c.Client.Apply(ctx, obj, opts...)
	if err == nil {
		c.stats.didUpdate.Add(1)
	}
	return err
}

// DeleteAllOf gates the operation by Mode.
func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if c.auditing() {
		kind := c.kindKey(obj)
		c.stats.wouldDeleteAllOf.Add(1)
		c.incByKind("deleteAllOf", kind)
		c.Log.Info("audit-mode would-write",
			"verb", "deleteAllOf",
			"kind", kind,
		)
		return nil
	}
	err := c.Client.DeleteAllOf(ctx, obj, opts...)
	if err == nil {
		c.stats.didDeleteAllOf.Add(1)
	}
	return err
}

// Status returns a SubResourceWriter that applies the same audit gating to
// status sub-resource writes (commonly used by reconcilers to update
// `.status` on a watched object).
func (c *Client) Status() client.SubResourceWriter {
	return &subResourceWriter{
		wrapped: c.Client.Status(),
		parent:  c,
		sub:     "status",
	}
}

// SubResource returns a SubResourceClient that applies the same audit gating
// to non-status sub-resource writes (e.g., /scale, /eviction).
func (c *Client) SubResource(subResource string) client.SubResourceClient {
	return &subResourceClient{
		wrapped: c.Client.SubResource(subResource),
		parent:  c,
		sub:     subResource,
	}
}

// subResourceWriter wraps a client.SubResourceWriter to gate sub-resource
// Create / Update / Patch by Mode.
type subResourceWriter struct {
	wrapped client.SubResourceWriter
	parent  *Client
	sub     string
}

func (s *subResourceWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	if s.parent.auditing() {
		kind := s.parent.kindKey(obj)
		s.parent.stats.wouldCreate.Add(1)
		s.parent.incByKind("create:"+s.sub, kind)
		s.parent.Log.Info("audit-mode would-write",
			"verb", "create",
			"subresource", s.sub,
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := s.wrapped.Create(ctx, obj, subResource, opts...)
	if err == nil {
		s.parent.stats.didCreate.Add(1)
	}
	return err
}

func (s *subResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.parent.auditing() {
		kind := s.parent.kindKey(obj)
		s.parent.stats.wouldUpdate.Add(1)
		s.parent.incByKind("update:"+s.sub, kind)
		s.parent.Log.Info("audit-mode would-write",
			"verb", "update",
			"subresource", s.sub,
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := s.wrapped.Update(ctx, obj, opts...)
	if err == nil {
		s.parent.stats.didUpdate.Add(1)
	}
	return err
}

func (s *subResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if s.parent.auditing() {
		kind := s.parent.kindKey(obj)
		s.parent.stats.wouldPatch.Add(1)
		s.parent.incByKind("patch:"+s.sub, kind)
		s.parent.Log.Info("audit-mode would-write",
			"verb", "patch",
			"subresource", s.sub,
			"kind", kind,
			"namespace", obj.GetNamespace(),
			"name", obj.GetName(),
		)
		return nil
	}
	err := s.wrapped.Patch(ctx, obj, patch, opts...)
	if err == nil {
		s.parent.stats.didPatch.Add(1)
	}
	return err
}

// Apply gates SSA writes to a sub-resource by Mode.
func (s *subResourceWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if s.parent.auditing() {
		s.parent.stats.wouldUpdate.Add(1)
		s.parent.incByKind("apply:"+s.sub, "<applyConfig>")
		s.parent.Log.Info("audit-mode would-write",
			"verb", "apply",
			"subresource", s.sub,
		)
		return nil
	}
	err := s.wrapped.Apply(ctx, obj, opts...)
	if err == nil {
		s.parent.stats.didUpdate.Add(1)
	}
	return err
}

// subResourceClient implements client.SubResourceClient: it embeds the
// wrapped subResourceWriter for writes and passes through Get for reads.
type subResourceClient struct {
	wrapped client.SubResourceClient
	parent  *Client
	sub     string
}

func (s *subResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	return s.wrapped.Get(ctx, obj, subResource, opts...)
}

func (s *subResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	w := &subResourceWriter{wrapped: s.wrapped, parent: s.parent, sub: s.sub}
	return w.Create(ctx, obj, subResource, opts...)
}

func (s *subResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w := &subResourceWriter{wrapped: s.wrapped, parent: s.parent, sub: s.sub}
	return w.Update(ctx, obj, opts...)
}

func (s *subResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w := &subResourceWriter{wrapped: s.wrapped, parent: s.parent, sub: s.sub}
	return w.Patch(ctx, obj, patch, opts...)
}

func (s *subResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	w := &subResourceWriter{wrapped: s.wrapped, parent: s.parent, sub: s.sub}
	return w.Apply(ctx, obj, opts...)
}
