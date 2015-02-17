package policy

import (
	"encoding/json"
	"io"
	"strings"
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
	// TODO fix it in future if necessary - Condition {}
}

type BucketPolicy struct {
	Version   string // date in 0000-00-00 format
	Statement []Stmt
}

const (
	awsResource   = "arn:aws:s3:::"
	minioResource = "minio:::"
)

// TODO support canonical user
const (
	awsPrincipal   = "arn:aws:iam::Account-ID:user/"
	minioPrincipal = "minio::Account-ID:user/"
)

var supportedActionMap = map[string]bool{
	"*":                     true,
	"s3:GetObject":          true,
	"s3:ListBucket":         true,
	"s3:PutObject":          true,
	"s3:CreateBucket":       true,
	"s3:GetBucketPolicy":    true,
	"s3:DeleteBucketPolicy": true,
	"s3:ListAllMyBuckets":   true,
	"s3:PutBucketPolicy":    true,
}

var supportedEffectMap = map[string]bool{
	"Allow": true,
	"Deny":  true,
}

func isValidAction(action []string) bool {
	var ok bool = false
	for _, a := range action {
		if supportedActionMap[a] {
			ok = true
		}
	}
	return ok
}

func isValidEffect(effect string) bool {
	if supportedEffectMap[effect] {
		return true
	}
	return false
}

func isValidResource(resources []string) bool {
	var ok bool = false
	for _, resource := range resources {
		switch true {
		case strings.HasPrefix(resource, awsResource):
			bucket := strings.SplitAfter(resource, awsResource)[1]
			ok = true
			if len(bucket) == 0 {
				ok = false
			}
		case strings.HasPrefix(resource, minioResource):
			bucket := strings.SplitAfter(resource, minioResource)[1]
			ok = true
			if len(bucket) == 0 {
				ok = false
			}
		default:
			ok = false
		}
	}
	return ok
}

func isValidPrincipal(principal string) bool {
	var ok bool = false
	if principal == "*" {
		return true
	}
	switch true {
	case strings.HasPrefix(principal, awsPrincipal):
		username := strings.SplitAfter(principal, awsPrincipal)[1]
		ok = true
		if len(username) == 0 {
			ok = false
		}
	case strings.HasPrefix(principal, minioPrincipal):
		username := strings.SplitAfter(principal, minioPrincipal)[1]
		ok = true
		if len(username) == 0 {
			ok = false
		}
	default:
		ok = false
	}
	return ok
}

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
	_, err = parseDate(policy.Version)
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
		if !isValidEffect(statement.Effect) {
			goto error
		}
		if len(statement.Principal.AWS) == 0 {
			goto error
		}
		if !isValidPrincipal(statement.Principal.AWS) {
			goto error
		}
		if len(statement.Action) == 0 {
			goto error
		}
		if !isValidAction(statement.Action) {
			goto error
		}
		if len(statement.Resource) == 0 {
			goto error
		}
		if !isValidResource(statement.Resource) {
			goto error
		}
	}
	return policy, true

error:
	return BucketPolicy{}, false
}
