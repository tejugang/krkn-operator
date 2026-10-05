package api

import (
	"context"
	"fmt"

	krknv1alpha1 "github.com/krkn-chaos/krkn-operator/api/v1alpha1"
	"github.com/krkn-chaos/krkn-operator/internal/kubeconfig"
	"github.com/krkn-chaos/krkn-operator/pkg/provider"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// refreshRestoredTargetStatuses is intentionally kept outside the shared
// archive package. Target readiness is operator-specific and depends on the
// provider liveness implementation; the shared package only restores admin
// configuration through the Kubernetes API.
func refreshRestoredTargetStatuses(ctx context.Context, k8sClient client.Client, namespace string) error {
	logger := log.FromContext(ctx)
	targets := &krknv1alpha1.KrknOperatorTargetList{}
	if err := k8sClient.List(ctx, targets, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list restored targets: %w", err)
	}

	for i := range targets.Items {
		target := &targets.Items[i]
		ready := false

		secret := &corev1.Secret{}
		err := k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      target.Spec.SecretUUID,
		}, secret)
		if err == nil {
			if secretData, ok := secret.Data["kubeconfig"]; ok {
				kubeconfigBase64, decodeErr := kubeconfig.UnmarshalSecretData(secretData)
				if decodeErr == nil {
					if livenessErr := provider.CheckClusterLiveness(ctx, kubeconfigBase64, 0); livenessErr == nil {
						ready = true
					} else {
						logger.Info("Restored target liveness check failed", "clusterName", target.Spec.ClusterName, "error", livenessErr)
					}
				} else {
					logger.Info("Restored target kubeconfig could not be decoded", "clusterName", target.Spec.ClusterName, "error", decodeErr)
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get Secret for restored target %q: %w", target.Spec.ClusterName, err)
		} else {
			logger.Info("Restored target Secret was not found", "clusterName", target.Spec.ClusterName)
		}

		target.Status.Ready = ready
		target.Status.LastUpdated = metav1.Now()
		if err := k8sClient.Status().Update(ctx, target); err != nil {
			return fmt.Errorf("failed to update restored target %q status: %w", target.Spec.ClusterName, err)
		}
	}
	return nil
}
