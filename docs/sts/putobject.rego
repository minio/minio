package httpapi.authz

import input as http_api

default allow = false

allow = true {
 http_api.action = "s3:PutObject"
 http_api.owner = false
}
