// Package controller contains controller-runtime reconcilers for pvc-plumber.
//
// Phase 1 scaffold: PVCReconciler is a stub that compiles cleanly and wires
// itself into a manager, but performs no real work. Reconcile logic (port of
// the Kyverno generate rules for ExternalSecret / ReplicationSource /
// ReplicationDestination) lands in Phase 2.
package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PVCReconciler reconciles PersistentVolumeClaim objects that carry a
// `backup: hourly|daily` label. It owns the lifecycle of the companion
// ExternalSecret, ReplicationSource, and ReplicationDestination resources.
//
// Phase 1: struct + stubs only. Real fields (KopiaClient, NFSServer, etc.)
// are added in Phase 3 alongside the reconcile implementation.
type PVCReconciler struct {
	client.Client
}

// Reconcile is the stub entry point. Phase 1 returns no-op so the manager
// can register and start cleanly.
func (r *PVCReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler with a controller-runtime
// manager. Phase 1 wires the watch on PersistentVolumeClaim so the
// controller machinery is exercised end-to-end; the Reconcile body remains
// a no-op until Phase 2.
func (r *PVCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Named("pvc-plumber-pvc").
		Complete(r)
}
