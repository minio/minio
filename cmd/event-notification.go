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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/pubsub"
	"github.com/minio/pkg/bucket/policy"
)

// EventNotifier - notifies external systems about events in MinIO.
type EventNotifier struct {
	sync.RWMutex
	targetList                 *event.TargetList
	targetResCh                chan event.TargetIDResult
	bucketRulesMap             map[string]event.RulesMap
	bucketRemoteTargetRulesMap map[string]map[event.TargetID]event.RulesMap
	eventsQueue                chan eventArgs
}

// NewEventNotifier - creates new event notification object.
func NewEventNotifier() *EventNotifier {
	// targetList/bucketRulesMap/bucketRemoteTargetRulesMap are populated by NotificationSys.InitBucketTargets()
	return &EventNotifier{
		targetList:                 event.NewTargetList(),
		targetResCh:                make(chan event.TargetIDResult),
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
		eventsQueue:                make(chan eventArgs, 10000),
	}
}

// GetARNList - returns available ARNs.
func (evnot *EventNotifier) GetARNList(onlyActive bool) []string {
	arns := []string{}
	if evnot == nil {
		return arns
	}
	region := globalSite.Region
	for targetID, target := range evnot.targetList.TargetMap() {
		// httpclient target is part of ListenNotification
		// which doesn't need to be listed as part of the ARN list
		// This list is only meant for external targets, filter
		// this out pro-actively.
		if !strings.HasPrefix(targetID.ID, "httpclient+") {
			if onlyActive {
				if _, err := target.IsActive(); err != nil {
					continue
				}
			}
			arns = append(arns, targetID.ToARN(region).String())
		}
	}

	return arns
}

// Loads notification policies for all buckets into EventNotifier.
func (evnot *EventNotifier) set(bucket BucketInfo, meta BucketMetadata) {
	config := meta.notificationConfig
	if config == nil {
		return
	}
	config.SetRegion(globalSite.Region)
	if err := config.Validate(globalSite.Region, globalEventNotifier.targetList); err != nil {
		if _, ok := err.(*event.ErrARNNotFound); !ok {
			logger.LogIf(GlobalContext, err)
		}
	}
	evnot.AddRulesMap(bucket.Name, config.ToRulesMap())
}

// InitBucketTargets - initializes event notification system from notification.xml of all buckets.
func (evnot *EventNotifier) InitBucketTargets(ctx context.Context, objAPI ObjectLayer) error {
	if objAPI == nil {
		return errServerNotInitialized
	}

	if err := evnot.targetList.Add(globalConfigTargetList.Targets()...); err != nil {
		return err
	}

	go func() {
		for e := range evnot.eventsQueue {
			evnot.send(e)
		}
	}()

	go func() {
		for res := range evnot.targetResCh {
			if res.Err != nil {
				reqInfo := &logger.ReqInfo{}
				reqInfo.AppendTags("targetID", res.ID.Name)
				logger.LogOnceIf(logger.SetReqInfo(GlobalContext, reqInfo), res.Err, res.ID.String())
			}
		}
	}()

	return nil
}

// AddRulesMap - adds rules map for bucket name.
func (evnot *EventNotifier) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	evnot.Lock()
	defer evnot.Unlock()

	rulesMap = rulesMap.Clone()

	for _, targetRulesMap := range evnot.bucketRemoteTargetRulesMap[bucketName] {
		rulesMap.Add(targetRulesMap)
	}

	// Do not add for an empty rulesMap.
	if len(rulesMap) == 0 {
		delete(evnot.bucketRulesMap, bucketName)
	} else {
		evnot.bucketRulesMap[bucketName] = rulesMap
	}
}

// RemoveRulesMap - removes rules map for bucket name.
func (evnot *EventNotifier) RemoveRulesMap(bucketName string, rulesMap event.RulesMap) {
	evnot.Lock()
	defer evnot.Unlock()

	evnot.bucketRulesMap[bucketName].Remove(rulesMap)
	if len(evnot.bucketRulesMap[bucketName]) == 0 {
		delete(evnot.bucketRulesMap, bucketName)
	}
}

// ConfiguredTargetIDs - returns list of configured target id's
func (evnot *EventNotifier) ConfiguredTargetIDs() []event.TargetID {
	if evnot == nil {
		return nil
	}

	evnot.RLock()
	defer evnot.RUnlock()

	var targetIDs []event.TargetID
	for _, rmap := range evnot.bucketRulesMap {
		for _, rules := range rmap {
			for _, targetSet := range rules {
				for id := range targetSet {
					targetIDs = append(targetIDs, id)
				}
			}
		}
	}

	return targetIDs
}

// RemoveNotification - removes all notification configuration for bucket name.
func (evnot *EventNotifier) RemoveNotification(bucketName string) {
	evnot.Lock()
	defer evnot.Unlock()

	delete(evnot.bucketRulesMap, bucketName)

	targetIDSet := event.NewTargetIDSet()
	for targetID := range evnot.bucketRemoteTargetRulesMap[bucketName] {
		targetIDSet[targetID] = struct{}{}
		delete(evnot.bucketRemoteTargetRulesMap[bucketName], targetID)
	}
	evnot.targetList.Remove(targetIDSet)

	delete(evnot.bucketRemoteTargetRulesMap, bucketName)
}

// RemoveAllRemoteTargets - closes and removes all notification targets.
func (evnot *EventNotifier) RemoveAllRemoteTargets() {
	evnot.Lock()
	defer evnot.Unlock()

	for _, targetMap := range evnot.bucketRemoteTargetRulesMap {
		targetIDSet := event.NewTargetIDSet()
		for k := range targetMap {
			targetIDSet[k] = struct{}{}
		}
		evnot.targetList.Remove(targetIDSet)
	}
}

// Send - sends the event to all registered notification targets
func (evnot *EventNotifier) Send(args eventArgs) {
	select {
	case evnot.eventsQueue <- args:
	default:
		// A new goroutine is created for each notification job, eventsQueue is
		// drained quickly and is not expected to be filled with any scenario.
		logger.LogIf(context.Background(), errors.New("internal events queue unexpectedly full"))
	}
}

func (evnot *EventNotifier) send(args eventArgs) {
	evnot.RLock()
	targetIDSet := evnot.bucketRulesMap[args.BucketName].Match(args.EventName, args.Object.Name)
	evnot.RUnlock()

	if len(targetIDSet) == 0 {
		return
	}

	evnot.targetList.Send(args.ToEvent(true), targetIDSet, evnot.targetResCh)
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

	respElements := map[string]string{
		"x-amz-request-id": args.RespElements["requestId"],
		"x-minio-origin-endpoint": func() string {
			if globalMinioEndpoint != "" {
				return globalMinioEndpoint
			}
			return getAPIEndpoints()[0]
		}(), // MinIO specific custom elements.
	}
	// Add deployment as part of
	if globalDeploymentID != "" {
		respElements["x-minio-deployment-id"] = globalDeploymentID
	}
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

	if args.EventName != event.ObjectRemovedDelete && args.EventName != event.ObjectRemovedDeleteMarkerCreated {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = make(map[string]string, len(args.Object.UserDefined))
		for k, v := range args.Object.UserDefined {
			if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
				continue
			}
			newEvent.S3.Object.UserMetadata[k] = v
		}
	}

	return newEvent
}

func sendEvent(args eventArgs) {
	args.Object.Size, _ = args.Object.GetActualSize()

	// avoid generating a notification for REPLICA creation event.
	if _, ok := args.ReqParams[xhttp.MinIOSourceReplicationRequest]; ok {
		return
	}
	// remove sensitive encryption entries in metadata.
	crypto.RemoveSensitiveEntries(args.Object.UserDefined)
	crypto.RemoveInternalEntries(args.Object.UserDefined)

	if globalHTTPListen.NumSubscribers(pubsub.MaskFromMaskable(args.EventName)) > 0 {
		globalHTTPListen.Publish(args.ToEvent(false))
	}

	globalEventNotifier.Send(args)
}
