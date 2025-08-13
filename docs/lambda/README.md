# Object Lambda

MinIO's Object Lambda implementation allows for transforming your data to serve unique data format requirements for each application. For example, a dataset created by an ecommerce application might include personally identifiable information (PII). When the same data is processed for analytics, PII should be redacted. However, if the same dataset is used for a marketing campaign, you might need to enrich the data with additional details, such as information from the customer loyalty database.

MinIO's Object Lambda, enables application developers to process data retrieved from MinIO before returning it to an application. You can register a Lambda Function target on MinIO, once successfully registered it can be used to transform the data for application GET requests on demand.

This document focuses on showing a working example on how to use Object Lambda with MinIO, you must have [MinIO deployed in your environment](https://docs.min.io/community/minio-object-store/operations/installation.html) before you can start using external lambda functions. You also must install Python version 3.8 or later for the lambda handlers to work.

## Example Lambda handler

Install the necessary dependencies.
```sh
pip install flask requests
```

Following is an example lambda handler.
```py
from flask import Flask, request, abort, make_response
import requests

app = Flask(__name__)
@app.route('/', methods=['POST'])
def get_webhook():
	if request.method == 'POST':
		# obtain the request event from the 'POST' call
		event = request.json

		object_context = event["getObjectContext"]

		# Get the presigned URL to fetch the requested
		# original object from MinIO
		s3_url = object_context["inputS3Url"]

		# Extract the route and request token from the input context
		request_route = object_context["outputRoute"]
		request_token = object_context["outputToken"]

		# Get the original S3 object using the presigned URL
		r = requests.get(s3_url)
		original_object = r.content.decode('utf-8')

		# Transform all text in the original object to uppercase
		# You can replace it with your custom code based on your use case
		transformed_object = original_object.upper()

		# Write object back to S3 Object Lambda
		# response sends the transformed data
		# back to MinIO and then to the user
		resp = make_response(transformed_object, 200)
		resp.headers['x-amz-request-route'] = request_route
		resp.headers['x-amz-request-token'] = request_token
		return resp

	else:
		abort(400)

if __name__ == '__main__':
	app.run()
```

When you're writing a Lambda function for use with MinIO, the function is based on event context that MinIO provides to the Lambda function. The event context provides information about the request being made. It contains the parameters with relevant context. The fields used to create the Lambda function are as follows:

The field of `getObjectContext` means the input and output details for connections to MinIO. It has the following fields:

- `inputS3Url` – A presigned URL that the Lambda function can use to download the original object. By using a presigned URL, the Lambda function doesn't need to have MinIO credentials to retrieve the original object. This allows Lambda function to focus on transformation of the object instead of securing the credentials.

- `outputRoute` – A routing token that is added to the response headers when the Lambda function returns the transformed object. This is used by MinIO to further verify the incoming response validity.

- `outputToken` – A token added to the response headers when the Lambda function returns the transformed object. This is used by MinIO to verify the incoming response validity.

Lets start the lambda handler.

```
python lambda_handler.py
 * Serving Flask app 'webhook'
 * Debug mode: off
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on http://127.0.0.1:5000
Press CTRL+C to quit
```

## Start MinIO with Lambda target

Register MinIO with a Lambda function, we are calling our target name as `function`, but you may call it any other friendly name of your choice.
```
MINIO_LAMBDA_WEBHOOK_ENABLE_function=on MINIO_LAMBDA_WEBHOOK_ENDPOINT_function=http://localhost:5000 minio server /data &
...
...
MinIO Object Storage Server
Copyright: 2015-2023 MinIO, Inc.
License: GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
Version: DEVELOPMENT.2023-02-05T05-17-27Z (go1.19.4 linux/amd64)

...
...
Object Lambda ARNs: arn:minio:s3-object-lambda::function:webhook

```

### Lambda Target with Auth Token

If your lambda target expects an authorization token then you can enable it per function target as follows

```
MINIO_LAMBDA_WEBHOOK_ENABLE_function=on MINIO_LAMBDA_WEBHOOK_ENDPOINT_function=http://localhost:5000 MINIO_LAMBDA_WEBHOOK_AUTH_TOKEN="mytoken" minio server /data &
```

### Lambda Target with mTLS authentication

If your lambda target expects mTLS client you can enable it per function target as follows
```
MINIO_LAMBDA_WEBHOOK_ENABLE_function=on MINIO_LAMBDA_WEBHOOK_ENDPOINT_function=http://localhost:5000 MINIO_LAMBDA_WEBHOOK_CLIENT_CERT=client.crt MINIO_LAMBDA_WEBHOOK_CLIENT_KEY=client.key minio server /data &
```

## Create a bucket and upload some data

Create a bucket named `functionbucket`
```
mc alias set myminio/ http://localhost:9000 minioadmin minioadmin
mc mb myminio/functionbucket
```

Create a file `testobject` with some test data that will be transformed
```
cat > testobject << EOF
MinIO is a High Performance Object Storage released under GNU Affero General Public License v3.0. It is API compatible with Amazon S3 cloud storage service. Use MinIO to build high performance infrastructure for machine learning, analytics and application data workloads.
EOF
```

Upload this object to the bucket via `mc cp`
```
mc cp testobject myminio/functionbucket/
```

## Invoke Lambda transformation via PresignedGET

Following example shows how you can use [`minio-go` PresignedGetObject](https://docs.min.io/community/minio-object-store/developers/go/API.html#presignedgetobject-ctx-context-context-bucketname-objectname-string-expiry-time-duration-reqparams-url-values-url-url-error)
```go
package main

import (
	"context"
	"log"
	"net/url"
	"time"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	s3Client, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Set lambda function target via `lambdaArn`
	reqParams := make(url.Values)
	reqParams.Set("lambdaArn", "arn:minio:s3-object-lambda::function:webhook")

	// Generate presigned GET url with lambda function
	presignedURL, err := s3Client.PresignedGetObject(context.Background(), "functionbucket", "testobject", time.Duration(1000)*time.Second, reqParams)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(presignedURL)
}
```

Use the Presigned URL via `curl` to receive the transformed object.
```
curl -v $(go run presigned.go)
...
...
> GET /functionbucket/testobject?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=minioadmin%2F20230205%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230205T173023Z&X-Amz-Expires=1000&X-Amz-SignedHeaders=host&lambdaArn=arn%3Aminio%3As3-object-lambda%3A%3Atoupper%3Awebhook&X-Amz-Signature=d7e343f0da9d4fa2bc822c12ad2f54300ff16796a1edaa6d31f1313c8e94d5b2 HTTP/1.1
> Host: localhost:9000
> User-Agent: curl/7.81.0
> Accept: */*
>

MINIO IS A HIGH PERFORMANCE OBJECT STORAGE RELEASED UNDER GNU AFFERO GENERAL PUBLIC LICENSE V3.0. IT IS API COMPATIBLE WITH AMAZON S3 CLOUD STORAGE SERVICE. USE MINIO TO BUILD HIGH PERFORMANCE INFRASTRUCTURE FOR MACHINE LEARNING, ANALYTICS AND APPLICATION DATA WORKLOADS.
```
