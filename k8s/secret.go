package k8s

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const secretRequestTimeout = 10 * time.Second

func (k8s *K8S) GetSecret(namespace, name string, keys ...string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), secretRequestTimeout)
	defer cancel()
	secret, err := k8s.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsForbidden(err) {
			return nil, ErrForbidden
		}
		if apierrors.IsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	res := make(map[string]string, len(keys))
	for _, key := range keys {
		res[key] = string(secret.Data[key])
	}
	return res, nil
}
