package cmd

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	errorsv1 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"math"
	"math/rand"
	"time"
)

type k8sConfigMapClient interface {
	UpdateConfigMap(ctx context.Context, resourceVersion string, annotations map[string]string) error
	GetConfigMap(ctx context.Context) (*corev1.ConfigMap, error)
	EnsureConfigMapExists(ctx context.Context)
}

type k8sConfigMapClientImpl struct {
	configMapsClient typedcorev1.ConfigMapInterface
	namespace        string
	configMapName    string
	maxRetries       int
}

func backoff(attempt int, backoffTimeMs float64) {
	durationMs := math.Pow(2, float64(attempt)) * backoffTimeMs
	withJitter := rand.Intn(int(durationMs))
	time.Sleep(time.Duration(withJitter) * time.Millisecond)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	return errorsv1.IsTooManyRequests(err) || errorsv1.IsServiceUnavailable(err) || errorsv1.IsServerTimeout(err)
}

func (cli *k8sConfigMapClientImpl) EnsureConfigMapExists(ctx context.Context) {
	var objectMeta = metav1.ObjectMeta{Name: cli.configMapName, Namespace: cli.namespace}
	var configMap = &corev1.ConfigMap{ObjectMeta: objectMeta}
	_, err := cli.configMapsClient.Create(ctx, configMap, metav1.CreateOptions{})
	if err != nil && !errorsv1.IsAlreadyExists(err) {
		panic(err)
	}
}

func (cli *k8sConfigMapClientImpl) UpdateConfigMap(ctx context.Context, resourceVersion string, annotations map[string]string) error {
	configMapUpdated := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       cli.namespace,
			Name:            cli.configMapName,
			ResourceVersion: resourceVersion,
			Annotations:     annotations,
		},
	}
	_, err := cli.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
	retries := 0
	for retries < cli.maxRetries && isRetryable(err) {
		backoff(retries, 1000)
		_, err = cli.configMapsClient.Update(ctx, configMapUpdated, metav1.UpdateOptions{})
		retries += 1
	}
	return err
}

func (cli *k8sConfigMapClientImpl) GetConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	configMap, err := cli.configMapsClient.Get(ctx, cli.configMapName, metav1.GetOptions{})
	retries := 0
	for retries < cli.maxRetries && isRetryable(err) {
		backoff(retries, 1000)
		configMap, err = cli.configMapsClient.Get(ctx, cli.configMapName, metav1.GetOptions{})
		retries += 1
	}
	return configMap, err
}
