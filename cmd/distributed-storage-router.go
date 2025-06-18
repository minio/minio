package cmd

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/mantle/gateway"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
)

func registerSDSRouter(router *mux.Router) {
	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
		CacheAPI:  newCachedObjectLayerFn,
	}

	router.Methods(http.MethodGet).Path("/health").HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := newContext(request, writer, "GetBucketHealth")

		defer logger.AuditLog(ctx, writer, request, mustGetClaimsFromToken(request))

		vars := mux.Vars(request)
		bucket := vars["bucket"]

		objectAPI := api.ObjectAPI()
		if objectAPI == nil {
			writeErrorResponse(ctx, writer, errorCodes.ToAPIErr(ErrServerNotInitialized), request.URL)
			return
		}

		//TODO: need to add action to the pkg repo. github.com/minio/pkg
		if s3Error := checkRequestAuthType(ctx, request, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, writer, errorCodes.ToAPIErr(s3Error), request.URL)
			return
		}

		resp, err := gateway.Health()
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		
		if err := json.NewEncoder(writer).Encode(resp); err != nil {
			http.Error(writer, "Could not encode response", http.StatusInternalServerError)
		}
	})
}
