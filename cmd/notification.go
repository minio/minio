/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/url"
	"path"
	"sync"

	xerrors "github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/hash"
	xnet "github.com/minio/minio/pkg/net"
)

// NotificationSys - notification system.
type NotificationSys struct {
	sync.RWMutex
	targetList                 *event.TargetList
	bucketRulesMap             map[string]event.RulesMap
	bucketRemoteTargetRulesMap map[string]map[event.TargetID]event.RulesMap
	peerRPCClientMap           map[xnet.Host]*PeerRPCClient
}

// GetARNList - returns available ARNs.
func (sys *NotificationSys) GetARNList() []string {
	arns := []string{}
	region := globalServerConfig.GetRegion()
	for _, targetID := range sys.targetList.List() {
		arns = append(arns, targetID.ToARN(region).String())
	}

	return arns
}

// GetPeerRPCClient - returns PeerRPCClient of addr.
func (sys *NotificationSys) GetPeerRPCClient(addr xnet.Host) *PeerRPCClient {
	return sys.peerRPCClientMap[addr]
}

// DeleteBucket - calls DeleteBucket RPC call on all peers.
func (sys *NotificationSys) DeleteBucket(bucketName string) map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			if err := client.DeleteBucket(bucketName); err != nil {
				errors[addr] = err
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// UpdateBucketPolicy - calls UpdateBucketPolicy RPC call on all peers.
func (sys *NotificationSys) UpdateBucketPolicy(bucketName string) map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			if err := client.UpdateBucketPolicy(bucketName); err != nil {
				errors[addr] = err
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// PutBucketNotification - calls PutBucketNotification RPC call on all peers.
func (sys *NotificationSys) PutBucketNotification(bucketName string, rulesMap event.RulesMap) map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient, rulesMap event.RulesMap) {
			defer wg.Done()
			if err := client.PutBucketNotification(bucketName, rulesMap); err != nil {
				errors[addr] = err
			}
		}(addr, client, rulesMap.Clone())
	}
	wg.Wait()

	return errors
}

// ListenBucketNotification - calls ListenBucketNotification RPC call on all peers.
func (sys *NotificationSys) ListenBucketNotification(bucketName string, eventNames []event.Name, pattern string, targetID event.TargetID, localPeer xnet.Host) map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			if err := client.ListenBucketNotification(bucketName, eventNames, pattern, targetID, localPeer); err != nil {
				errors[addr] = err
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// AddRemoteTarget - adds event rules map, HTTP/PeerRPC client target to bucket name.
func (sys *NotificationSys) AddRemoteTarget(bucketName string, target event.Target, rulesMap event.RulesMap) error {
	if err := sys.targetList.Add(target); err != nil {
		return err
	}

	sys.Lock()
	targetMap := sys.bucketRemoteTargetRulesMap[bucketName]
	if targetMap == nil {
		targetMap = make(map[event.TargetID]event.RulesMap)
	}
	targetMap[target.ID()] = rulesMap.Clone()
	sys.bucketRemoteTargetRulesMap[bucketName] = targetMap
	sys.Unlock()

	sys.AddRulesMap(bucketName, rulesMap)
	return nil
}

// RemoteTargetExist - checks whether given target ID is a HTTP/PeerRPC client target or not.
func (sys *NotificationSys) RemoteTargetExist(bucketName string, targetID event.TargetID) bool {
	sys.Lock()
	defer sys.Unlock()

	targetMap, ok := sys.bucketRemoteTargetRulesMap[bucketName]
	if ok {
		_, ok = targetMap[targetID]
	}

	return ok
}

// initListeners - initializes PeerRPC clients available in listener.json.
func (sys *NotificationSys) initListeners(objAPI ObjectLayer, bucketName string) error {
	// listener.json is available/applicable only in DistXL mode.
	if !globalIsDistXL {
		return nil
	}

	// Construct path to listener.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketListenerConfig)
	transactionConfigFile := configFile + ".transaction"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	reader, err := readConfig(objAPI, configFile)
	if err != nil && !xerrors.IsErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
		return err
	}

	listenerList := []ListenBucketNotificationArgs{}
	if reader != nil {
		if err = json.NewDecoder(reader).Decode(&listenerList); err != nil {
			errorIf(err, "Unable to parse listener.json.")
			return xerrors.Trace(err)
		}
	}

	if len(listenerList) == 0 {
		// Nothing to initialize for empty listener list.
		return nil
	}

	activeListenerList := []ListenBucketNotificationArgs{}
	for _, args := range listenerList {
		var found bool
		if found, err = isLocalHost(args.Addr.Name); err != nil {
			errorIf(err, "unable to check address %v is local host", args.Addr)
			return err
		}
		if found {
			// As this function is called at startup, skip HTTP listener to this host.
			continue
		}

		rpcClient := sys.GetPeerRPCClient(args.Addr)
		if rpcClient == nil {
			return fmt.Errorf("unable to find PeerRPCClient by address %v in listener.json for bucket %v", args.Addr, bucketName)
		}

		var exist bool
		if exist, err = rpcClient.RemoteTargetExist(bucketName, args.TargetID); err != nil {
			return err
		}
		if !exist {
			// Skip previously connected HTTP listener which is not found in remote peer.
			continue
		}

		target := NewPeerRPCClientTarget(bucketName, args.TargetID, rpcClient)
		rulesMap := event.NewRulesMap(args.EventNames, args.Pattern, target.ID())
		if err = sys.AddRemoteTarget(bucketName, target, rulesMap); err != nil {
			return err
		}
		activeListenerList = append(activeListenerList, args)
	}

	data, err := json.Marshal(activeListenerList)
	if err != nil {
		return err
	}

	return saveConfig(objAPI, configFile, data)
}

// Init - initializes notification system from notification.xml and listener.json of all buckets.
func (sys *NotificationSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		config, err := readNotificationConfig(objAPI, bucket.Name)
		if err != nil {
			if !xerrors.IsErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
				errorIf(err, "Unable to load notification configuration of bucket %v", bucket.Name)
				return err
			}
		} else {
			sys.AddRulesMap(bucket.Name, config.ToRulesMap())
		}

		if err = sys.initListeners(objAPI, bucket.Name); err != nil {
			errorIf(err, "Unable to initialize HTTP listener for bucket %v", bucket.Name)
			return err
		}
	}

	return nil
}

// AddRulesMap - adds rules map for bucket name.
func (sys *NotificationSys) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	rulesMap = rulesMap.Clone()

	for _, targetRulesMap := range sys.bucketRemoteTargetRulesMap[bucketName] {
		rulesMap.Add(targetRulesMap)
	}

	rulesMap.Add(sys.bucketRulesMap[bucketName])
	sys.bucketRulesMap[bucketName] = rulesMap
}

// RemoveRulesMap - removes rules map for bucket name.
func (sys *NotificationSys) RemoveRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	sys.bucketRulesMap[bucketName].Remove(rulesMap)
	if len(sys.bucketRulesMap[bucketName]) == 0 {
		delete(sys.bucketRulesMap, bucketName)
	}
}

// RemoveNotification - removes all notification configuration for bucket name.
func (sys *NotificationSys) RemoveNotification(bucketName string) {
	sys.Lock()
	defer sys.Unlock()

	delete(sys.bucketRulesMap, bucketName)

	for targetID := range sys.bucketRemoteTargetRulesMap[bucketName] {
		sys.targetList.Remove(targetID)
		delete(sys.bucketRemoteTargetRulesMap[bucketName], targetID)
	}

	delete(sys.bucketRemoteTargetRulesMap, bucketName)
}

// RemoveAllRemoteTargets - closes and removes all HTTP/PeerRPC client targets.
func (sys *NotificationSys) RemoveAllRemoteTargets() {
	for _, targetMap := range sys.bucketRemoteTargetRulesMap {
		for targetID := range targetMap {
			sys.targetList.Remove(targetID)
		}
	}
}

// RemoveRemoteTarget - closes and removes target by target ID.
func (sys *NotificationSys) RemoveRemoteTarget(bucketName string, targetID event.TargetID) {
	for id, err := range sys.targetList.Remove(targetID) {
		errorIf(err, "unable to close target ID %v", id)
	}

	sys.Lock()
	defer sys.Unlock()

	if _, ok := sys.bucketRemoteTargetRulesMap[bucketName]; ok {
		delete(sys.bucketRemoteTargetRulesMap[bucketName], targetID)
		if len(sys.bucketRemoteTargetRulesMap[bucketName]) == 0 {
			delete(sys.bucketRemoteTargetRulesMap, bucketName)
		}
	}
}

func (sys *NotificationSys) send(bucketName string, eventData event.Event, targetIDs ...event.TargetID) map[event.TargetID]error {
	errMap := sys.targetList.Send(eventData, targetIDs...)
	for targetID := range errMap {
		if sys.RemoteTargetExist(bucketName, targetID) {
			sys.RemoveRemoteTarget(bucketName, targetID)
		}
	}

	return errMap
}

// Send - sends event data to all matching targets.
func (sys *NotificationSys) Send(args eventArgs) map[event.TargetID]error {
	sys.RLock()
	targetIDSet := sys.bucketRulesMap[args.BucketName].Match(args.EventName, args.Object.Name)
	sys.RUnlock()
	if len(targetIDSet) == 0 {
		return nil
	}

	targetIDs := targetIDSet.ToSlice()
	return sys.send(args.BucketName, args.ToEvent(), targetIDs...)
}

// NewNotificationSys - creates new notification system object.
func NewNotificationSys(config *serverConfig, endpoints EndpointList) (*NotificationSys, error) {
	targetList, err := getNotificationTargets(config)
	if err != nil {
		return nil, err
	}

	peerRPCClientMap := makeRemoteRPCClients(endpoints)

	// bucketRulesMap/bucketRemoteTargetRulesMap are initialized by NotificationSys.Init()
	return &NotificationSys{
		targetList:                 targetList,
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
		peerRPCClientMap:           peerRPCClientMap,
	}, nil
}

type eventArgs struct {
	EventName  event.Name
	BucketName string
	Object     ObjectInfo
	ReqParams  map[string]string
	Host       string
	Port       string
	UserAgent  string
}

// ToEvent - converts to notification event.
func (args eventArgs) ToEvent() event.Event {
	getOriginEndpoint := func() string {
		host := globalMinioHost
		if host == "" {
			// FIXME: Send FQDN or hostname of this machine than sending IP address.
			host = localIP4.ToSlice()[0]
		}

		return fmt.Sprintf("%s://%s:%s", getURLScheme(globalIsSSL), host, globalMinioPort)
	}

	creds := globalServerConfig.GetCredential()
	eventTime := UTCNow()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())

	newEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         globalServerConfig.GetRegion(),
		EventTime:         eventTime.Format(event.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      event.Identity{creds.AccessKey},
		RequestParameters: args.ReqParams,
		ResponseElements: map[string]string{
			"x-amz-request-id":        uniqueID,
			"x-minio-origin-endpoint": getOriginEndpoint(), // Minio specific custom elements.
		},
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{creds.AccessKey},
				ARN:           bucketARNPrefix + args.BucketName,
			},
			Object: event.Object{
				Key:       url.QueryEscape(args.Object.Name),
				VersionID: "1",
				Sequencer: uniqueID,
			},
		},
		Source: event.Source{
			Host:      args.Host,
			Port:      args.Port,
			UserAgent: args.UserAgent,
		},
	}

	if args.EventName != event.ObjectRemovedDelete {
		newEvent.S3.Object.ETag = args.Object.ETag
		newEvent.S3.Object.Size = args.Object.Size
		newEvent.S3.Object.ContentType = args.Object.ContentType
		newEvent.S3.Object.UserMetadata = args.Object.UserDefined
	}

	return newEvent
}

func sendEvent(args eventArgs) {
	// globalNotificationSys is not initialized in gateway mode.
	if globalNotificationSys == nil {
		return
	}

	for targetID, err := range globalNotificationSys.Send(args) {
		errorIf(err, "unable to send event %v of bucket: %v, object: %v to target %v",
			args.EventName, args.BucketName, args.Object.Name, targetID)
	}
}

func saveConfig(objAPI ObjectLayer, configFile string, data []byte) error {
	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data))
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(context.Background(), minioMetaBucket, configFile, hashReader, nil)
	return err
}

func readConfig(objAPI ObjectLayer, configFile string) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	// Read entire content by setting size to -1
	err := objAPI.GetObject(context.Background(), minioMetaBucket, configFile, 0, -1, &buffer, "")
	if err != nil {
		// Ignore if err is ObjectNotFound or IncompleteBody when bucket is not configured with notification
		if isErrObjectNotFound(err) || isErrIncompleteBody(err) {
			return nil, xerrors.Trace(errNoSuchNotifications)
		}
		errorIf(err, "Unable to read file %v", configFile)
		return nil, err
	}

	// Return NoSuchNotifications on empty content.
	if buffer.Len() == 0 {
		return nil, xerrors.Trace(errNoSuchNotifications)
	}

	return &buffer, nil
}

func readNotificationConfig(objAPI ObjectLayer, bucketName string) (*event.Config, error) {
	// Construct path to notification.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketNotificationConfig)

	// Get read lock.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, configFile)
	if err := objLock.GetRLock(globalOperationTimeout); err != nil {
		return nil, err
	}
	defer objLock.RUnlock()

	reader, err := readConfig(objAPI, configFile)
	if err != nil {
		return nil, err
	}

	return event.ParseConfig(reader, globalServerConfig.GetRegion(), globalNotificationSys.targetList)
}

func saveNotificationConfig(objAPI ObjectLayer, bucketName string, config *event.Config) error {
	data, err := xml.Marshal(config)
	if err != nil {
		return err
	}

	configFile := path.Join(bucketConfigPrefix, bucketName, bucketNotificationConfig)

	// Get write lock.
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, configFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	return saveConfig(objAPI, configFile, data)
}

// SaveListener - saves HTTP client currently listening for events to listener.json.
func SaveListener(objAPI ObjectLayer, bucketName string, eventNames []event.Name, pattern string, targetID event.TargetID, addr xnet.Host) error {
	// listener.json is available/applicable only in DistXL mode.
	if !globalIsDistXL {
		return nil
	}

	// Construct path to listener.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketListenerConfig)
	transactionConfigFile := configFile + ".transaction"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	reader, err := readConfig(objAPI, configFile)
	if err != nil && !xerrors.IsErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
		return err
	}

	listenerList := []ListenBucketNotificationArgs{}
	if reader != nil {
		if err = json.NewDecoder(reader).Decode(&listenerList); err != nil {
			errorIf(err, "Unable to parse listener.json.")
			return xerrors.Trace(err)
		}
	}

	listenerList = append(listenerList, ListenBucketNotificationArgs{
		EventNames: eventNames,
		Pattern:    pattern,
		TargetID:   targetID,
		Addr:       addr,
	})

	data, err := json.Marshal(listenerList)
	if err != nil {
		return err
	}

	return saveConfig(objAPI, configFile, data)
}

// RemoveListener - removes HTTP client currently listening for events from listener.json.
func RemoveListener(objAPI ObjectLayer, bucketName string, targetID event.TargetID, addr xnet.Host) error {
	// listener.json is available/applicable only in DistXL mode.
	if !globalIsDistXL {
		return nil
	}

	// Construct path to listener.json for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketListenerConfig)
	transactionConfigFile := configFile + ".transaction"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(minioMetaBucket, transactionConfigFile)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	reader, err := readConfig(objAPI, configFile)
	if err != nil && !xerrors.IsErrIgnored(err, errDiskNotFound, errNoSuchNotifications) {
		return err
	}

	listenerList := []ListenBucketNotificationArgs{}
	if reader != nil {
		if err = json.NewDecoder(reader).Decode(&listenerList); err != nil {
			errorIf(err, "Unable to parse listener.json.")
			return xerrors.Trace(err)
		}
	}

	if len(listenerList) == 0 {
		// Nothing to remove.
		return nil
	}

	activeListenerList := []ListenBucketNotificationArgs{}
	for _, args := range listenerList {
		if args.TargetID == targetID && args.Addr.Equal(addr) {
			// Skip if matches
			continue
		}

		activeListenerList = append(activeListenerList, args)
	}

	data, err := json.Marshal(activeListenerList)
	if err != nil {
		return err
	}

	return saveConfig(objAPI, configFile, data)
}
