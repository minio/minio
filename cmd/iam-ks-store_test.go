package cmd

import (
	"context"
	"reflect"
	"testing"

	"github.com/minio/minio/cmd/kv"
	"github.com/minio/minio/pkg/auth"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
)

func TestSaveLoadPolicy(t *testing.T) {
	var err error

	backend := kv.NewInmemoryBackend()
	store := newIAMKvStore(context.Background(), backend)

	apolicy := iampolicy.Policy{
		ID: "testid",
	}

	err = store.savePolicyDoc("mypolicy", apolicy)
	assertError(t, err, "savePolicyDoc failed")

	result := map[string]iampolicy.Policy{}
	err = store.loadPolicyDoc("mypolicy", result)
	assertError(t, err, "loadPolicyDoc failed")

	loadedPolicy := result["mypolicy"]
	if loadedPolicy.ID != apolicy.ID {
		t.Fatal("Unexpected policy")
	}

	assertEquals(t, loadedPolicy, apolicy, "User Credentials")

	err = store.deletePolicyDoc("mypolicy")
	assertError(t, err, "deletePolicyDoc failed")

	err = store.loadPolicyDoc("mypolicy", result)
	assertEquals(t, err, errConfigNotFound, "loadPolicyDoc for non existing policy must return a errConfigNotFound error")
}

func TestSaveLoadUser(t *testing.T) {
	var err error
	backend := kv.NewInmemoryBackend()
	store := newIAMKvStore(context.Background(), backend)
	user := UserIdentity{
		Credentials: auth.Credentials{
			AccessKey: "accessKey",
			SecretKey: "secretKey",
			Status:    "enabled",
		},
	}
	err = store.saveUserIdentity("mytestyser", regularUser, user)
	assertError(t, err, "saveUserIdentity failed")
	result := map[string]auth.Credentials{}
	err = store.loadUser("mytestyser", regularUser, result)
	assertError(t, err, "loadUser failed")
	loadedcreds := result["mytestyser"]
	assertEquals(t, loadedcreds, user.Credentials, "User Credentials")

	err = store.deleteUserIdentity("mytestyser", regularUser)
	assertError(t, err, "deleteUserIdentity failed")

	err = store.loadUser("mytestyser", regularUser, result)
	assertEquals(t, err, errNoSuchUser, "loadUser for non existing user must return a errNoSuchUser error")

}

func TestGroup(t *testing.T) {
	var err error
	backend := kv.NewInmemoryBackend()
	store := newIAMKvStore(context.Background(), backend)

	group := GroupInfo{
		Status: "enabled",
	}
	err = store.saveGroupInfo("mytestgroup", group)
	assertError(t, err, "saveGroupInfo failed")

	result := map[string]GroupInfo{}
	err = store.loadGroup("mytestgroup", result)
	assertError(t, err, "loadGroup failed")

	loadedGroup := result["mytestgroup"]
	assertEquals(t, loadedGroup, group, "Group")

	loadAll := map[string]GroupInfo{}
	store.loadGroups(context.Background(), loadAll)

	assertEquals(t, loadAll, map[string]GroupInfo{"mytestgroup": group}, "Load all Groups")

	err = store.deleteGroupInfo("mytestgroup")
	assertError(t, err, "deleteGroup failed")
	err = store.loadGroup("mytestgroup", result)
	assertEquals(t, err, errNoSuchGroup, "loadGroup for non existing group must return a errNoSuchGroup error")
}

func assertError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s\n Test %s failed with error: %s", msg, getSource(2), err)
	}
}

func assertEquals(t *testing.T, gotValue interface{}, expectedValue interface{}, msg string) {
	if !reflect.DeepEqual(gotValue, expectedValue) {
		t.Fatalf("%s\n Test %s expected '%v', got '%v'", msg, getSource(2), expectedValue, gotValue)
	}
}
