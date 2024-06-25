// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strings"
	"sync"

	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/pkg/v3/policy"
)

// EventNotifier - notifies external systems about events in MinIO.
type EventNotifier struct {
	sync.RWMutex
	targetList     *event.TargetList
	bucketRulesMap map[string]event.RulesMap
}

// NewEventNotifier - creates new event notification object.
func NewEventNotifier(ctx context.Context) *EventNotifier {
	// targetList/bucketRulesMap/bucketRemoteTargetRulesMap are populated by NotificationSys.InitBucketTargets()
	return &EventNotifier{
		targetList:     event.NewTargetList(ctx),
		bucketRulesMap: make(map[string]event.RulesMap),
	}
}

// GetARNList - returns available ARNs.
func (evnot *EventNotifier) GetARNList() []string {
	arns := []string{}
	if evnot == nil {
		return arns
	}
	region := globalSite.Region()
	for targetID := range evnot.targetList.TargetMap() {
		// httpclient target is part of ListenNotification
		// which doesn't need to be listed as part of the ARN list
		// This list is only meant for external targets, filter
		// this out pro-actively.
		if !strings.HasPrefix(targetID.ID, "httpclient+") {
			arns = append(arns, targetID.ToARN(region).String())
		}
	}

	return arns
}

// Loads notification policies for all buckets into EventNotifier.
func (evnot *EventNotifier) set(bucket string, meta BucketMetadata) {
	config := meta.notificationConfig
	if config == nil {
		return
	}
	region := globalSite.Region()
	config.SetRegion(region)
	if err := config.Validate(region, globalEventNotifier.targetList); err != nil {
		if _, ok := err.(*event.ErrARNNotFound); !ok {
			internalLogIf(GlobalContext, err)
		}
	}
	evnot.AddRulesMap(bucket, config.ToRulesMap())
}

// Targets returns all the registered targets
func (evnot *EventNotifier) Targets() []event.Target {
	return evnot.targetList.Targets()
}

// InitBucketTargets - initializes event notification system from notification.xml of all buckets.
func (evnot *EventNotifier) InitBucketTargets(ctx context.Context, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	if err := evnot.targetList.Add(globalNotifyTargetList.Targets()...); err != nil {
		return err
	}
	evnot.targetList = evnot.targetList.Init(runtime.GOMAXPROCS(0)) // TODO: make this configurable (y4m4)
	return nil
}

// AddRulesMap - adds rules map for bucket name.
func (evnot *EventNotifier) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	evnot.Lock()
	defer evnot.Unlock()

	rulesMap = rulesMap.Clone()

	// Do not add for an empty rulesMap.
	if len(rulesMap) == 0 {
		delete(evnot.bucketRulesMap, bucketName)
	} else {
		evnot.bucketRulesMap[bucketName] = rulesMap
	}
}

// RemoveNotification - removes all notification configuration for bucket name.
func (evnot *EventNotifier) RemoveNotification(bucketName string) {
	evnot.Lock()
	defer evnot.Unlock()

	delete(evnot.bucketRulesMap, bucketName)
}

// RemoveAllBucketTargets - closes and removes all notification targets.
func (evnot *EventNotifier) RemoveAllBucketTargets() {
	evnot.Lock()
	defer evnot.Unlock()

	targetIDSet := event.NewTargetIDSet()
	for k := range evnot.targetList.TargetMap() {
		targetIDSet[k] = struct{}{}
	}
	evnot.targetList.Remove(targetIDSet)
}

// Send - sends the event to all registered notification targets
func (evnot *EventNotifier) Send(args eventArgs) {
	evnot.RLock()
	targetIDSet := evnot.bucketRulesMap[args.BucketName].Match(args.EventName, args.Object.Name)
	evnot.RUnlock()

	if len(targetIDSet) == 0 {
		return
	}

	// If MINIO_API_SYNC_EVENTS is set, send events synchronously.
	evnot.targetList.Send(args.ToEvent(true), targetIDSet, globalAPIConfig.isSyncEventsEnabled())
}

type eventArgs struct {
	EventName    event.Name
	BucketName   string
	Object       ObjectInfo
	ReqParams    map[string]string
	RespElements map[string]string
	Host         string
	UserAgent    string
}

// ToEvent - converts to notification event.
func (args eventArgs) ToEvent(escape bool) event.Event {
	eventTime := UTCNow()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())
	if !args.Object.ModTime.IsZero() {
		uniqueID = fmt.Sprintf("%X", args.Object.ModTime.UnixNano())
	}

	respElements := map[string]string{
		"x-amz-request-id": args.RespElements["requestId"],
		"x-amz-id-2":       args.RespElements["nodeId"],
		"x-minio-origin-endpoint": func() string {
			if globalMinioEndpoint != "" {
				return globalMinioEndpoint
			}
			return getAPIEndpoints()[0]
		}(), // MinIO specific custom elements.
	}

	// Add deployment as part of response elements.
	respElements["x-minio-deployment-id"] = globalDeploymentID()
	if args.RespElements["content-length"] != "" {
		respElements["content-length"] = args.RespElements["content-length"]
	}

	keyName := args.Object.Name
	if escape {
		keyName = url.QueryEscape(args.Object.Name)
	}

	newEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         args.ReqParams["region"],
		EventTime:         eventTime.Format(event.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      event.Identity{PrincipalID: args.ReqParams["principalId"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{PrincipalID: args.ReqParams["principalId"]},
				ARN:           policy.ResourceARNPrefix + args.BucketName,
			},
			Object: event.Object{
				Key:       keyName,
				VersionID: args.Object.VersionID,
				Sequencer: uniqueID,
			},
		},
		Source: event.Source{
			Host:      args.Host,
			UserAgent: args.UserAgent,
		},
	}

	isRemovedEvent := args.EventName == event.ObjectRemovedDelete ||
		args.EventName == event.ObjectRemovedDeleteMarkerCreated ||
		args.EventName == event.ObjectRemovedNoOP

	if !isRemovedEvent {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = make(map[string]string, len(args.Object.UserDefined))
		for k, v := range args.Object.UserDefined {
			if stringsHasPrefixFold(strings.ToLower(k), ReservedMetadataPrefixLower) {
				continue
			}
			newEvent.S3.Object.UserMetadata[k] = v
		}
	}

	return newEvent
}

func sendEvent(args eventArgs) {
	// avoid generating a notification for REPLICA creation event.
	if _, ok := args.ReqParams[xhttp.MinIOSourceReplicationRequest]; ok {
		return
	}

	args.Object.Size, _ = args.Object.GetActualSize()

	// remove sensitive encryption entries in metadata.
	crypto.RemoveSensitiveEntries(args.Object.UserDefined)
	crypto.RemoveInternalEntries(args.Object.UserDefined)

	if globalHTTPListen.NumSubscribers(pubsub.MaskFromMaskable(args.EventName)) > 0 {
		globalHTTPListen.Publish(args.ToEvent(false))
	}

	globalEventNotifier.Send(args)
}
