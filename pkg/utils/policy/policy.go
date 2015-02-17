package policy

import (
	"encoding/json"
	"io"
)

type UserCred struct {
	AWS string
}

type Stmt struct {
	Sid       string
	Effect    string
	Principal UserCred
	Action    []string
	Resource  []string
}

type BucketPolicy struct {
	Version   string // date in 0000-00-00 format
	Statement []Stmt
}

// TODO: Add more checks

// validate request body is proper JSON
func Parsepolicy(data io.Reader) (BucketPolicy, bool) {
	var policy BucketPolicy
	decoder := json.NewDecoder(data)
	err := decoder.Decode(&policy)
	if err != nil {
		goto error
	}
	if len(policy.Version) == 0 {
		goto error
	}
	_, err = ParseDate(policy.Version)
	if err != nil {
		goto error
	}
	if len(policy.Statement) == 0 {
		goto error
	}

	for _, statement := range policy.Statement {
		if len(statement.Sid) == 0 {
			goto error
		}
		if len(statement.Effect) == 0 {
			goto error
		}
		if len(statement.Principal.AWS) == 0 {
			goto error
		}
		if len(statement.Action) == 0 {
			goto error
		}
		if len(statement.Resource) == 0 {
			goto error
		}
	}
	return policy, true

error:
	return BucketPolicy{}, false
}
