package cmd

import (
	"context"
	"fmt"
	"github.com/minio/minio/pkg/auth"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"testing"
	"time"
)

type k8sConfigMapClientMock struct {
	kv map[string]string
}

func (cli *k8sConfigMapClientMock) EnsureConfigMapExists(ctx context.Context) {}

func (cli *k8sConfigMapClientMock) UpdateConfigMap(ctx context.Context, resourceVersion string, annotations map[string]string) error {
	cli.kv = annotations
	return nil
}

func (cli *k8sConfigMapClientMock) GetConfigMap(ctx context.Context) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Annotations:     cli.kv,
			ResourceVersion: "1",
		},
	}
	return configMap, nil
}

func clear(ctx context.Context, client *IAMK8sStore) {
	_ = client.configMapsClient.UpdateConfigMap(ctx, "", map[string]string{})
}

func TestIamK8sStore(t *testing.T) {
	iamK8sStore := &IAMK8sStore{
		configMapsClient: &k8sConfigMapClientMock{
			kv: make(map[string]string),
		},
		maxConflictRetries:  10,
		ttlPurgeFrequencyMs: 120 * 1000,
	}
	ctx := context.Background()

	t.Run("test user identities", func(t *testing.T) {
		userName := "testUser123"
		creds, _ := auth.CreateNewCredentialsWithMetadata("dummy", "dummy", nil, "dummy")

		for _, userType := range []IAMUserType{regularUser, stsUser, srvAccUser} {
			clear(ctx, iamK8sStore)

			userIdentity := newUserIdentity(creds)
			err := iamK8sStore.saveUserIdentity(ctx, userName, userType, userIdentity)
			if err != nil {
				t.Fatal("Error occurred when saving user identity: ", err)
			}

			loaded := map[string]auth.Credentials{}
			err = iamK8sStore.loadUser(ctx, userName, userType, loaded)
			if err != nil {
				t.Fatal("Error occurred when loading user identity: ", err)
			}
			if _, ok := loaded[userName]; !ok {
				t.Fatal(fmt.Sprintf("Could not load user of type '%d'", userType))
			}

			err = iamK8sStore.deleteUserIdentity(ctx, userName, userType)
			if err != nil {
				t.Fatal("Error occurred when deleting user identity: ", err)
			}

			loaded = map[string]auth.Credentials{}
			_ = iamK8sStore.loadUser(ctx, userName, userType, loaded)
			if _, ok := loaded[userName]; ok {
				t.Fatal(fmt.Sprintf("Expected user identity of type '%d' to have been deleted", userType))
			}
		}
	})
}

func timeInFuture() string {
	return strconv.FormatInt(time.Now().Unix()+10*60, 10)
}

func timeInPast() string {
	return strconv.FormatInt(time.Now().Unix()-10*60, 10)
}

func TestFilterExpiredItems(t *testing.T) {
	t.Run("no items", func(t *testing.T) {
		items := []iamConfigItem{}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 0 {
			t.Errorf("Expected empty. Was not")
		}
	})

	t.Run("no expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInFuture()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInFuture()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 4 {
			t.Errorf("Expected no items to be filtered")
		}
	})

	t.Run("all expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInPast()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInPast()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 0 {
			t.Errorf("Expected all items to be filtered")
		}
	})

	t.Run("some expired items", func(t *testing.T) {
		items := []iamConfigItem{
			{objPath: "some-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-path"), data: timeInFuture()},
			{objPath: "some-other-path", data: "jsonData"},
			{objPath: addTTLPrefix("some-other-path"), data: timeInPast()},
		}
		filteredItems := filterExpiredItems(items)
		if len(filteredItems) != 2 {
			t.Errorf("Expected 2 items to be filtered")
		}
	})
}
