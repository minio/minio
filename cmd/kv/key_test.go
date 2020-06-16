package kv

import (
	"testing"
)

func TestKeyTransformTrimJSON(t *testing.T) {
	res := KeyTransformTrimJSON("/config/iam/policydb/users/", "/config/iam/policydb/users/myconfig.json")
	if res != "myconfig" {
		t.Errorf("Expected: %s Got: %s", "myconfig", res)
	}
}

func TestKeyTransformDefault(t *testing.T) {
	res := KeyTransformDefault("config/iam/policy/", "config/iam/policy/mypolicy/policy.json")
	if res != "mypolicy" {
		t.Errorf("Expected: %s Got: %s", "mypolicy", res)
	}
}
