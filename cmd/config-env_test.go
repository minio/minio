package cmd

import (
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
)

func TestEnvConfigBrowser(t *testing.T) {
	testCases := []struct {
		browser *string
		result  *bool
	}{
		{nil, nil},
		{proto.String("on"), proto.Bool(true)},
		{proto.String("off"), proto.Bool(false)},
	}

	for i, testCase := range testCases {
		if testCase.browser != nil {
			if err := os.Setenv("MINIO_BROWSER", *testCase.browser); err != nil {
				t.Errorf("Test %d, could not set environment variable", i)
			}
		}

		f := envConfigBrowser()

		if testCase.browser != nil {
			if err := os.Unsetenv("MINIO_BROWSER"); err != nil {
				t.Errorf("Test %d, could not unset environment variable", i)
			}
		}

		if testCase.result == nil && f != nil {
			t.Errorf("Test %d, expected to get `nil` but got `%v`", i, *f)
		}

		if testCase.result != nil && (f == nil || BrowserFlag(*testCase.result) != *f) {
			t.Errorf("Test %d, expected to get `%v` but got `%v`", i, *testCase.result, f)
		}
	}
}

func TestEnvConfigCredential(t *testing.T) {
	cred := envConfigCredential()
	if cred != nil {
		t.Errorf("Expected to get `nil` but got `%v`", cred)
	}

	if err := os.Setenv("MINIO_ACCESS_KEY", "access_key"); err != nil {
		t.Error("Could not set environment variable")
	}

	cred = envConfigCredential()

	if err := os.Unsetenv("MINIO_ACCESS_KEY"); err != nil {
		t.Error("Could not unset environment variable")
	}

	if cred != nil {
		t.Errorf("Expected to get `nil` but got `%v`", cred)
	}

	if err := os.Setenv("MINIO_SECRET_KEY", "secret_key"); err != nil {
		t.Error("Could not set environment variable")
	}

	cred = envConfigCredential()

	if err := os.Unsetenv("MINIO_SECRET_KEY"); err != nil {
		t.Error("Could not unset environment variable")
	}

	if cred != nil {
		t.Errorf("Expected to get `nil` but got `%v`", cred)
	}

	if err := os.Setenv("MINIO_ACCESS_KEY", "access_key"); err != nil {
		t.Error("Could not set environment variable")
	}

	if err := os.Setenv("MINIO_SECRET_KEY", "secret_key"); err != nil {
		t.Error("Could not set environment variable")
	}

	cred = envConfigCredential()

	if err := os.Unsetenv("MINIO_SECRET_KEY"); err != nil {
		t.Error("Could not unset environment variable")
	}

	if err := os.Unsetenv("MINIO_ACCESS_KEY"); err != nil {
		t.Error("Could not unset environment variable")
	}

	if cred == nil {
		t.Errorf("Expected to get `nil` but got `%v`", cred)
	}
}
