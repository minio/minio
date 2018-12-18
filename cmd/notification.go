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
	"net"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/minio/minio/pkg/policy"
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
		// httpclient target is part of ListenBucketNotification
		// which doesn't need to be listed as part of the ARN list
		// This list is only meant for external targets, filter
		// this out pro-actively.
		if !strings.HasPrefix(targetID.ID, "httpclient+") {
			arns = append(arns, targetID.ToARN(region).String())
		}
	}

	return arns
}

// GetPeerRPCClient - returns PeerRPCClient of addr.
func (sys *NotificationSys) GetPeerRPCClient(addr xnet.Host) *PeerRPCClient {
	return sys.peerRPCClientMap[addr]
}

// NotificationPeerErr returns error associated for a remote peer.
type NotificationPeerErr struct {
	Host xnet.Host // Remote host on which the rpc call was initiated
	Err  error     // Error returned by the remote peer for an rpc call
}

// DeleteBucket - calls DeleteBucket RPC call on all peers.
func (sys *NotificationSys) DeleteBucket(ctx context.Context, bucketName string) {
	go func() {
		var wg sync.WaitGroup
		for addr, client := range sys.peerRPCClientMap {
			wg.Add(1)
			go func(addr xnet.Host, client *PeerRPCClient) {
				defer wg.Done()
				if err := client.DeleteBucket(bucketName); err != nil {
					logger.GetReqInfo(ctx).AppendTags("remotePeer", addr.Name)
					logger.LogIf(ctx, err)
				}
			}(addr, client)
		}
		wg.Wait()
	}()
}

// ReloadFormat - calls ReloadFormat RPC call on all peers.
func (sys *NotificationSys) ReloadFormat(dryRun bool) map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			// Try to load format in three attempts, before giving up.
			for i := 0; i < 3; i++ {
				err := client.ReloadFormat(dryRun)
				if err == nil {
					break
				}
				errors[addr] = err
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// LoadUsers - calls LoadUsers RPC call on all peers.
func (sys *NotificationSys) LoadUsers() map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			// Try to load users in three attempts.
			for i := 0; i < 3; i++ {
				err := client.LoadUsers()
				if err == nil {
					break
				}
				errors[addr] = err
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// LoadCredentials - calls LoadCredentials RPC call on all peers.
func (sys *NotificationSys) LoadCredentials() map[xnet.Host]error {
	errors := make(map[xnet.Host]error)
	var wg sync.WaitGroup
	for addr, client := range sys.peerRPCClientMap {
		wg.Add(1)
		go func(addr xnet.Host, client *PeerRPCClient) {
			defer wg.Done()
			// Try to load credentials in three attempts.
			for i := 0; i < 3; i++ {
				err := client.LoadCredentials()
				if err == nil {
					break
				}
				errors[addr] = err
				// Wait for one second and no need wait after last attempt.
				if i < 2 {
					time.Sleep(1 * time.Second)
				}
			}
		}(addr, client)
	}
	wg.Wait()

	return errors
}

// SetBucketPolicy - calls SetBucketPolicy RPC call on all peers.
func (sys *NotificationSys) SetBucketPolicy(ctx context.Context, bucketName string, bucketPolicy *policy.Policy) {
	go func() {
		var wg sync.WaitGroup
		for addr, client := range sys.peerRPCClientMap {
			wg.Add(1)
			go func(addr xnet.Host, client *PeerRPCClient) {
				defer wg.Done()
				if err := client.SetBucketPolicy(bucketName, bucketPolicy); err != nil {
					logger.GetReqInfo(ctx).AppendTags("remotePeer", addr.Name)
					logger.LogIf(ctx, err)
				}
			}(addr, client)
		}
		wg.Wait()
	}()
}

// RemoveBucketPolicy - calls RemoveBucketPolicy RPC call on all peers.
func (sys *NotificationSys) RemoveBucketPolicy(ctx context.Context, bucketName string) {
	go func() {
		var wg sync.WaitGroup
		for addr, client := range sys.peerRPCClientMap {
			wg.Add(1)
			go func(addr xnet.Host, client *PeerRPCClient) {
				defer wg.Done()
				if err := client.RemoveBucketPolicy(bucketName); err != nil {
					logger.GetReqInfo(ctx).AppendTags("remotePeer", addr.Name)
					logger.LogIf(ctx, err)
				}
			}(addr, client)
		}
		wg.Wait()
	}()
}

// PutBucketNotification - calls PutBucketNotification RPC call on all peers.
func (sys *NotificationSys) PutBucketNotification(ctx context.Context, bucketName string, rulesMap event.RulesMap) {
	go func() {
		var wg sync.WaitGroup
		for addr, client := range sys.peerRPCClientMap {
			wg.Add(1)
			go func(addr xnet.Host, client *PeerRPCClient, rulesMap event.RulesMap) {
				defer wg.Done()
				if err := client.PutBucketNotification(bucketName, rulesMap); err != nil {
					logger.GetReqInfo(ctx).AppendTags("remotePeer", addr.Name)
					logger.LogIf(ctx, err)
				}
			}(addr, client, rulesMap.Clone())
		}
		wg.Wait()
	}()
}

// ListenBucketNotification - calls ListenBucketNotification RPC call on all peers.
func (sys *NotificationSys) ListenBucketNotification(ctx context.Context, bucketName string, eventNames []event.Name, pattern string,
	targetID event.TargetID, localPeer xnet.Host) {
	go func() {
		var wg sync.WaitGroup
		for addr, client := range sys.peerRPCClientMap {
			wg.Add(1)
			go func(addr xnet.Host, client *PeerRPCClient) {
				defer wg.Done()
				if err := client.ListenBucketNotification(bucketName, eventNames, pattern, targetID, localPeer); err != nil {
					logger.GetReqInfo(ctx).AppendTags("remotePeer", addr.Name)
					logger.LogIf(ctx, err)
				}
			}(addr, client)
		}
		wg.Wait()
	}()
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

	rulesMap = rulesMap.Clone()
	targetMap[target.ID()] = rulesMap
	sys.bucketRemoteTargetRulesMap[bucketName] = targetMap

	rulesMap = rulesMap.Clone()
	rulesMap.Add(sys.bucketRulesMap[bucketName])
	sys.bucketRulesMap[bucketName] = rulesMap

	sys.Unlock()

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
func (sys *NotificationSys) initListeners(ctx context.Context, objAPI ObjectLayer, bucketName string) error {
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
	if err := objLock.GetRLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.RUnlock()

	configData, e := readConfig(ctx, objAPI, configFile)
	if e != nil && !IsErrIgnored(e, errDiskNotFound, errConfigNotFound) {
		return e
	}

	listenerList := []ListenBucketNotificationArgs{}
	if configData != nil {
		if err := json.Unmarshal(configData, &listenerList); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
	}

	if len(listenerList) == 0 {
		// Nothing to initialize for empty listener list.
		return nil
	}

	for _, args := range listenerList {
		found, err := isLocalHost(args.Addr.Name)
		if err != nil {
			logger.GetReqInfo(ctx).AppendTags("host", args.Addr.Name)
			logger.LogIf(ctx, err)
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

		exist, err := rpcClient.RemoteTargetExist(bucketName, args.TargetID)
		if err != nil {
			logger.GetReqInfo(ctx).AppendTags("targetID", args.TargetID.Name)
			logger.LogIf(ctx, err)
			return err
		}
		if !exist {
			// Skip previously connected HTTP listener which is not found in remote peer.
			continue
		}

		target := NewPeerRPCClientTarget(bucketName, args.TargetID, rpcClient)
		rulesMap := event.NewRulesMap(args.EventNames, args.Pattern, target.ID())
		if err = sys.AddRemoteTarget(bucketName, target, rulesMap); err != nil {
			logger.GetReqInfo(ctx).AppendTags("targetName", target.id.Name)
			logger.LogIf(ctx, err)
			return err
		}
	}

	return nil
}

func (sys *NotificationSys) refresh(objAPI ObjectLayer) error {
	buckets, err := objAPI.ListBuckets(context.Background())
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{BucketName: bucket.Name})
		config, err := readNotificationConfig(ctx, objAPI, bucket.Name)
		if err != nil && err != errNoSuchNotifications {
			return err
		}
		if err == errNoSuchNotifications {
			continue
		}
		sys.AddRulesMap(bucket.Name, config.ToRulesMap())
		if err = sys.initListeners(ctx, objAPI, bucket.Name); err != nil {
			return err
		}
	}
	return nil
}

// Init - initializes notification system from notification.xml and listener.json of all buckets.
func (sys *NotificationSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing notification needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			if err := sys.refresh(objAPI); err != nil {
				if err == errDiskNotFound ||
					strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
					strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
					logger.Info("Waiting for notification subsystem to be initialized..")
					continue
				}
				return err
			}
			return nil
		}
	}
}

// AddRulesMap - adds rules map for bucket name.
func (sys *NotificationSys) AddRulesMap(bucketName string, rulesMap event.RulesMap) {
	sys.Lock()
	defer sys.Unlock()

	rulesMap = rulesMap.Clone()

	for _, targetRulesMap := range sys.bucketRemoteTargetRulesMap[bucketName] {
		rulesMap.Add(targetRulesMap)
	}

	// Do not add for an empty rulesMap.
	if len(rulesMap) == 0 {
		delete(sys.bucketRulesMap, bucketName)
	} else {
		sys.bucketRulesMap[bucketName] = rulesMap
	}
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
	for terr := range sys.targetList.Remove(targetID) {
		reqInfo := (&logger.ReqInfo{}).AppendTags("targetID", terr.ID.Name)
		ctx := logger.SetReqInfo(context.Background(), reqInfo)
		logger.LogIf(ctx, terr.Err)
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

func (sys *NotificationSys) send(bucketName string, eventData event.Event, targetIDs ...event.TargetID) (errs []event.TargetIDErr) {
	errCh := sys.targetList.Send(eventData, targetIDs...)
	for terr := range errCh {
		errs = append(errs, terr)
		if sys.RemoteTargetExist(bucketName, terr.ID) {
			sys.RemoveRemoteTarget(bucketName, terr.ID)
		}
	}

	return errs
}

// Send - sends event data to all matching targets.
func (sys *NotificationSys) Send(args eventArgs) []event.TargetIDErr {
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
func NewNotificationSys(config *serverConfig, endpoints EndpointList) *NotificationSys {
	targetList := getNotificationTargets(config)
	peerRPCClientMap := makeRemoteRPCClients(endpoints)

	// bucketRulesMap/bucketRemoteTargetRulesMap are initialized by NotificationSys.Init()
	return &NotificationSys{
		targetList:                 targetList,
		bucketRulesMap:             make(map[string]event.RulesMap),
		bucketRemoteTargetRulesMap: make(map[string]map[event.TargetID]event.RulesMap),
		peerRPCClientMap:           peerRPCClientMap,
	}
}

type eventArgs struct {
	EventName    event.Name
	BucketName   string
	Object       ObjectInfo
	ReqParams    map[string]string
	RespElements map[string]string
	Host         string
	Port         string
	UserAgent    string
}

// ToEvent - converts to notification event.
func (args eventArgs) ToEvent() event.Event {
	getOriginEndpoint := func() string {
		host := globalMinioHost
		if host == "" {
			// FIXME: Send FQDN or hostname of this machine than sending IP address.
			host = sortIPs(localIP4.ToSlice())[0]
		}

		return fmt.Sprintf("%s://%s", getURLScheme(globalIsSSL), net.JoinHostPort(host, globalMinioPort))
	}

	eventTime := UTCNow()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())

	respElements := map[string]string{
		"x-amz-request-id":        args.RespElements["requestId"],
		"x-minio-origin-endpoint": getOriginEndpoint(), // Minio specific custom elements.
	}
	// Add deployment as part of
	if globalDeploymentID != "" {
		respElements["x-minio-deployment-id"] = globalDeploymentID
	}
	if args.RespElements["content-length"] != "" {
		respElements["content-length"] = args.RespElements["content-length"]
	}
	newEvent := event.Event{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         args.ReqParams["region"],
		EventTime:         eventTime.Format(event.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      event.Identity{PrincipalID: args.ReqParams["accessKey"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: event.Metadata{
			SchemaVersion:   "1.0",
			ConfigurationID: "Config",
			Bucket: event.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: event.Identity{PrincipalID: args.ReqParams["accessKey"]},
				ARN:           policy.ResourceARNPrefix + args.BucketName,
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
		if args.Object.IsCompressed() {
			newEvent.S3.Object.Size = args.Object.GetActualSize()
		}
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

	notifyCh := globalNotificationSys.Send(args)
	go func() {
		for _, err := range notifyCh {
			reqInfo := &logger.ReqInfo{BucketName: args.BucketName, ObjectName: args.Object.Name}
			reqInfo.AppendTags("EventName", args.EventName.String())
			reqInfo.AppendTags("targetID", err.ID.Name)
			ctx := logger.SetReqInfo(context.Background(), reqInfo)
			logger.LogOnceIf(ctx, err.Err, err.ID)
		}
	}()
}

func readNotificationConfig(ctx context.Context, objAPI ObjectLayer, bucketName string) (*event.Config, error) {
	// Construct path to notification.xml for the given bucket.
	configFile := path.Join(bucketConfigPrefix, bucketName, bucketNotificationConfig)
	configData, err := readConfig(ctx, objAPI, configFile)
	if err != nil {
		if err == errConfigNotFound {
			err = errNoSuchNotifications
		}

		return nil, err
	}

	config, err := event.ParseConfig(bytes.NewReader(configData), globalServerConfig.GetRegion(), globalNotificationSys.targetList)
	logger.LogIf(ctx, err)
	return config, err
}

func saveNotificationConfig(ctx context.Context, objAPI ObjectLayer, bucketName string, config *event.Config) error {
	data, err := xml.Marshal(config)
	if err != nil {
		return err
	}

	configFile := path.Join(bucketConfigPrefix, bucketName, bucketNotificationConfig)
	return saveConfig(ctx, objAPI, configFile, data)
}

// SaveListener - saves HTTP client currently listening for events to listener.json.
func SaveListener(objAPI ObjectLayer, bucketName string, eventNames []event.Name, pattern string, targetID event.TargetID, addr xnet.Host) error {
	// listener.json is available/applicable only in DistXL mode.
	if !globalIsDistXL {
		return nil
	}

	ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{BucketName: bucketName})

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

	configData, err := readConfig(ctx, objAPI, configFile)
	if err != nil && !IsErrIgnored(err, errDiskNotFound, errConfigNotFound) {
		return err
	}

	listenerList := []ListenBucketNotificationArgs{}
	if configData != nil {
		if err = json.Unmarshal(configData, &listenerList); err != nil {
			logger.LogIf(ctx, err)
			return err
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
		logger.LogIf(ctx, err)
		return err
	}

	return saveConfig(ctx, objAPI, configFile, data)
}

// RemoveListener - removes HTTP client currently listening for events from listener.json.
func RemoveListener(objAPI ObjectLayer, bucketName string, targetID event.TargetID, addr xnet.Host) error {
	// listener.json is available/applicable only in DistXL mode.
	if !globalIsDistXL {
		return nil
	}

	ctx := logger.SetReqInfo(context.Background(), &logger.ReqInfo{BucketName: bucketName})

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

	configData, err := readConfig(ctx, objAPI, configFile)
	if err != nil && !IsErrIgnored(err, errDiskNotFound, errConfigNotFound) {
		return err
	}

	listenerList := []ListenBucketNotificationArgs{}
	if configData != nil {
		if err = json.Unmarshal(configData, &listenerList); err != nil {
			logger.LogIf(ctx, err)
			return err
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
		logger.LogIf(ctx, err)
		return err
	}

	return saveConfig(ctx, objAPI, configFile, data)
}
