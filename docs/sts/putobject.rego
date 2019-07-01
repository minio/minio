package httpapi.authz

import input as http_api

allow {
 input.action = "s3:PutObject"
 input.owner = false
}
