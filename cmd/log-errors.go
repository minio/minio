package cmd

import (
	"runtime"
	"strconv"
	"strings"
)

func funcName() string {
	pc, _, _, _ := runtime.Caller(1)
	functionNamePath := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	if len(functionNamePath) > 0 {
		return functionNamePath[len(functionNamePath)-1]
	}
	return ""
}

// admin-handlers.go

// LogUptimeFailure logs Uptime update Failure from majority of servers.
func LogUptimeFailure(err error) {
	errorIf(err, funcName(), "Possibly failed to get uptime from majority of servers.", nil)
}

// LogFailedMarshalStorageInfo logs failure to marshal storage info into json.
func LogFailedMarshalStorageInfo(err error) {
	errorIf(err, funcName(), "Failed to marshal storage info into json.", nil)
}

// LogFailedUnMarshalCreds logs failure to unmarshal credentials request.
func LogFailedUnMarshalCreds(err error) {
	errorIf(err, funcName(), "Cannot unmarshal credentials request.", nil)
}

// LogFailedPeerCredsUpdate logs failure to update credentials on peer.
func LogFailedPeerCredsUpdate(err error, peer string) {
	m := make(map[string]string)
	m["Peer"] = peer
	errorIf(err, funcName(), "Unable to update credentials on peer.", m)
}

// LogFailedCredsUpdate logs failure to update config with new credentials.
func LogFailedCredsUpdate(err error) {
	errorIf(err, funcName(), "Unable to update the config with new credentials.", nil)
}

// LogFailedGetPeerInfo logs failure to get peer server info.
func LogFailedGetPeerInfo(err error, addr string) {
	m := make(map[string]string)
	m["PeerAddress"] = addr
	errorIf(err, funcName(), "Unable to get peer server info.", m)
}

// LogFailedDurationParse logs failure to parse duration passed as query value.
func LogFailedDurationParse(err error) {
	errorIf(err, funcName(), "Failed to parse duration passed as query value.", nil)
}

// LogFailedLockInfo logs failure to get lock info from remote nodes.
func LogFailedLockInfo(err error) {
	errorIf(err, funcName(), "Failed to fetch lock information from remote nodes.", nil)
}

// LogFailedMarshalLockInfo logs failure to marshal lock information into json.
func LogFailedMarshalLockInfo(err error) {
	errorIf(err, funcName(), "Failed to marshal lock information into json.", nil)
}

// LogFailedGetPeerConfig logs failure to get config from peers.
func LogFailedGetPeerConfig(err error) {
	errorIf(err, funcName(), "Failed to get config from peers.", nil)
}

// LogFailedConfigRead logs failure to read config from request body.
func LogFailedConfigRead(err error) {
	errorIf(err, funcName(), "Failed to read config from request body.", nil)
}

// LogDuplicateJSONEntry logs that config containing duplicate JSON entries.
func LogDuplicateJSONEntry(err error) {
	errorIf(err, funcName(), "config contains duplicate JSON entries.", nil)
}

// LogFailedUmarshalJSON logs failure to unmarshal JSON configuration.
func LogFailedUmarshalJSON(err error) {
	errorIf(err, funcName(), "Failed to unmarshal JSON configuration.", nil)
}

//admin-rpc-server.go

// LogFailedWriteServerTempConfig logs failure to write temporary config file on the server.
func LogFailedWriteServerTempConfig(err error, path string) {
	m := make(map[string]string)
	m["ConfigPath"] = path
	errorIf(err, funcName(), "Failed to write temporary config file.", m)
}

// admin-rpc-client.go

// LogFailedWriteTempConfig logs failure to write temporary config file.
func LogFailedWriteTempConfig(err error) {
	errorIf(err, funcName(), "Failed to write temporary config file.", nil)
}

// LogFailedConfigRename logs failure to rename config file.
func LogFailedConfigRename(err error, source, destination string) {
	m := make(map[string]string)
	m["sourceConfig"] = source
	m["destinationConfig"] = destination
	errorIf(err, funcName(), "Failed to rename config file.", m)
}

// LogFailedRemoteConfigRename logs failure to rename config file on a remote node.
func LogFailedRemoteConfigRename(err error) {
	errorIf(err, funcName(), "Failed to rename config file on a remote node.", nil)
}

// LogFailedFetchUptime logs failure to fetch uptime
func LogFailedFetchUptime(err error) {
	errorIf(err, funcName(), "Unable to fetch uptime.", nil)
}

// LogFailedUnMarshalPeerServerConfig logs failure to unmarshal serverConfig.
func LogFailedUnMarshalPeerServerConfig(err error, addr string) {
	m := make(map[string]string)
	m["PeerAddress"] = addr
	errorIf(err, funcName(), "Failed to unmarshal serverConfig.", m)
}

// LogFailedFindServerConfig logs failure to finad a valid server config."
func LogFailedFindServerConfig(err error) {
	errorIf(err, funcName(), "Unable to finad a valid server config.", nil)
}

// auth-handler.go

// LogFailedReadReqSignatureVerification logs failure to read request body for signature verification.
func LogFailedReadReqSignatureVerification(err error) {
	errorIf(err, funcName(), "Unable to read request body for signature verification.", nil)
}

// browser-peer-rpc.go

// LogFailedCredsUpdateViaRPC logs failure to update the config with new credentials sent from browser RPC.
func LogFailedCredsUpdateViaRPC(err error) {
	errorIf(err, funcName(), "Unable to update the config with new credentials sent from browser RPC.", nil)
}

// bucket-handlers.go

// LogFailedReadBucketPolicy logs failure to read bucket policy.
func LogFailedReadBucketPolicy(err error) {
	errorIf(err, funcName(), "Unable to read bucket policy.", nil)
}

// LogFailedReadHTTPBody logs failure to read HTTP body.
func LogFailedReadHTTPBody(err error) {
	errorIf(err, funcName(), "Unable to read HTTP body.", nil)
}

// LogFailedUnmarshalDeleteObjectsReqXML logs failure to unmarshal delete objects request XML.
func LogFailedUnmarshalDeleteObjectsReqXML(err error) {
	errorIf(err, funcName(), "Unable to unmarshal delete objects request XML.", nil)
}

// LogFailedInitMultipartReader logs failure to initialize multipart reader.
func LogFailedInitMultipartReader(err error) {
	errorIf(err, funcName(), "Unable to initialize multipart reader.", nil)
}

// LogFailedParseFormValues logs failure to parse form values.
func LogFailedParseFormValues(err error) {
	errorIf(err, funcName(), "Unable to parse form values.", nil)
}

// LogInvalidHTTPReqHeader logs invalid http request header was found.
func LogInvalidHTTPReqHeader(err error) {
	errorIf(err, funcName(), "found invalid http request header", nil)
}

// LogFailedInitHashReader logs failure to initialize hashReader.",
func LogFailedInitHashReader(err error) {
	errorIf(err, funcName(), "Unable to initialize hashReader.", nil)
}

// bucket-notifcation-handlers.go

// LogFailedReadNotificationConfig logs failure to read notification configuration.",
func LogFailedReadNotificationConfig(err error) {
	errorIf(err, funcName(), "Unable to read notification configuration.", nil)
}

// LogFailedMarshalNotificationconfig logs failure to marshal notification configuration into XML.
func LogFailedMarshalNotificationconfig(err error) {
	errorIf(err, funcName(), "Unable to marshal notification configuration into XML.", nil)
}

// LogFailedReadIncomingBody logs failure to read incoming body.",
func LogFailedReadIncomingBody(err error) {
	errorIf(err, funcName(), "Unable to read incoming body.", nil)
}

// LogFailedParseNofiticationConfigXML logs failure to parse notification configuration XML.
func LogFailedParseNofiticationConfigXML(err error) {
	errorIf(err, funcName(), "Unable to parse notification configuration XML.", nil)
}

// LogFailedNotificationWrite logs failure to write notification.
func LogFailedNotificationWrite(err error) {
	errorIf(err, funcName(), "Unable to write notification.", nil)
}

// LogFailedEmptyNotificationWrite logs failure to write empty notification.
func LogFailedEmptyNotificationWrite(err error) {
	errorIf(err, funcName(), "Unable to write empty notification.", nil)
}

// LogFailedAddListener logs error while adding a listener!
func LogFailedAddListener(err error) {
	errorIf(err, funcName(), "Error adding a listener!", nil)
}

// LogFailedPersistConfigInAddListener logs error while persisting listener config when adding a listener.",
func LogFailedPersistConfigInAddListener(err error) {
	errorIf(err, funcName(), "Error persisting listener config when adding a listener.", nil)
}

// LogFailedPersistConfigInRemoveListener logs error while persisting listener config when removing a listener.",
func LogFailedPersistConfigInRemoveListener(err error) {
	errorIf(err, funcName(), "Error persisting listener config when removing a listener.", nil)
}

// bucket-notification-utils.go

// LogInvalidSqsType logs an invalid SQS Type.
func LogInvalidSqsType(err error, sqsType string) {
	m := make(map[string]string)
	m["SQSType"] = sqsType
	errorIf(err, funcName(), "Invalid SQS Type.", m)
}

// bucket-policy-handlers.go

// LogFailedClientRead logs failure to read from client.
func LogFailedClientRead(err error) {
	errorIf(err, funcName(), "Unable to read from client.", nil)
}

// LogFailedBucketPolicyMarshal logs failure to marshal bucket policy.
func LogFailedBucketPolicyMarshal(err error) {
	errorIf(err, funcName(), "Unable to marshal bucket policy.", nil)
}

// bucket-policy.go

// LogFailedBucketPolicyLoad logs failure to load bucket policy
func LogFailedBucketPolicyLoad(err error, bucketName string) {
	m := make(map[string]string)
	m["Bucket"] = bucketName
	errorIf(err, funcName(), "Unable to load bucket policy.", m)
}

// LogFailedBucketPolicyMarshaltoJSON logs failure to marshal bucket policy to JSON.
func LogFailedBucketPolicyMarshaltoJSON(err error) {
	errorIf(err, funcName(), "Unable to marshal bucket policy to JSON.", nil)
}

// LogFailedSetBucketPolicy logs failure to set bucket policy.
func LogFailedSetBucketPolicy(err error, bucketName string) {
	m := make(map[string]string)
	m["Bucket"] = bucketName
	errorIf(err, funcName(), "Unable to set bucket policy.", m)
}

// LogFailedSaveBucketPolicy logs failure to save bucket policy.
func LogFailedSaveBucketPolicy(err error) {
	errorIf(err, funcName(), "Unable to save bucket policy.", nil)
}

// common-main.go

// LogConfigMigrationFailed logs failure to migrate config.
func LogConfigMigrationFailed(err error) {
	fatalIf(err, funcName(), "Config migration failed.", nil)
}

// LogFailedconfigLoad logs failure to load config.
func LogFailedconfigLoad(err error, version string) {
	m := make(map[string]string)
	m["ExpectedVersion"] = version
	fatalIf(err, funcName(), "Unable to load config.", m)
}

// LogConfigInitFailed logs failure to initialize minio config for the first time.
func LogConfigInitFailed(err error) {
	fatalIf(err, funcName(), "Unable to initialize minio config for the first time.", nil)
}

// LogConfigDirEmpty logs failure on configuration directory.
func LogConfigDirEmpty(err error) {
	fatalIf(err, funcName(), "Configuration directory cannot be empty.", nil)
}

// LogFailedConfigDirAbsPath logs failure to fetch absolute path for config directory.
func LogFailedConfigDirAbsPath(err error, configDir string) {
	m := make(map[string]string)
	m["ConfigDir"] = configDir
	fatalIf(err, funcName(), "Unable to fetch absolute path for config directory.", m)
}

// LogInvalidCredsInEnv logs invalid access/secret key was set in environment.
func LogInvalidCredsInEnv(err error) {
	fatalIf(err, funcName(), "Invalid access/secret key set in environment.", nil)
}

// LogMinioBrowserEnvUnknownValue logs unknown value in MINIO_BROWSER environment variable.
func LogMinioBrowserEnvUnknownValue(err error, browserEnv string) {
	m := make(map[string]string)
	m["Browser_env"] = browserEnv
	fatalIf(err, funcName(), "Unknown value in MINIO_BROWSER environment variable.", m)
}

// LogErrorOpeningTraceFile logs failure to open http trace file.
func LogErrorOpeningTraceFile(err error, path string) {
	m := make(map[string]string)
	m["HTTPTraceFile"] = path
	fatalIf(err, funcName(), "Error opening http trace file.", m)
}

// LogInvalidEnvValue logs that an invalid value was set in environment variable.
func LogInvalidEnvValue(err error, envVariable string) {
	m := make(map[string]string)
	m["EnvVariable"] = envVariable
	fatalIf(err, funcName(), "Invalid value set in environment variable.", m)
}

// config-current.go

// LogInvalidConfigJSONValue logs that an invalid value was set in config.json.
func LogInvalidConfigJSONValue(err error, scheme string, parity int) {
	m := make(map[string]string)
	m["Scheme"] = scheme
	m["Parity"] = strconv.Itoa(parity)
	fatalIf(err, funcName(), "Invalid value set in config.json.", m)
}

// config-dir.go

// LogGetHomeDirFailed logs a failure to get the home directory.
func LogGetHomeDirFailed(err error) {
	fatalIf(err, funcName(), "Unable to get home directory.", nil)
}

// net.go

// LogUnexpectedErrorResolvingHost logs failure to resolve host"
func LogUnexpectedErrorResolvingHost(err error, host string) {
	m := make(map[string]string)
	m["Host"] = host
	fatalIf(err, funcName(), "unexpected error when resolving host.", m)
}

// event-notifier.go

// LogFailedBucketNofificationLoad logs failure to load bucket-notification.
func LogFailedBucketNofificationLoad(err error, bucket string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	errorIf(err, funcName(), "Unable to load bucket-notification.", m)
}

// LogFailedBucketListenerLoad logs failure to load bucket-listeners.
func LogFailedBucketListenerLoad(err error, bucket string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	errorIf(err, funcName(), "Unable to load bucket-listeners.", m)
}

// LogFailedListenerConfigUnMarshal logs failure to unmarshal listener config from JSON.
func LogFailedListenerConfigUnMarshal(err error) {
	errorIf(err, funcName(), "Unable to unmarshal listener config from JSON.", nil)
}

// LogFailedNotificationConfigMarshalXML logs failure to marshal notification configuration into XML.
func LogFailedNotificationConfigMarshalXML(err error) {
	errorIf(err, funcName(), "Unable to marshal notification configuration into XML", nil)
}

// LogFailedBucketNotificationConfigWrite logs failure to write bucket notification configuration.
func LogFailedBucketNotificationConfigWrite(err error) {
	errorIf(err, funcName(), "Unable to write bucket notification configuration.", nil)
}

// LogFailedListenerConfigMarshal logs failure to marshal listener config to JSON.
func LogFailedListenerConfigMarshal(err error) {
	errorIf(err, funcName(), "Unable to marshal listener config to JSON.", nil)
}

// LogFailedWriteBucketListenerConfigToObjLayer logs failure to write bucket listener configuration to object layer.
func LogFailedWriteBucketListenerConfigToObjLayer(err error) {
	errorIf(err, funcName(), "Unable to write bucket listener configuration to object layer.", nil)
}

// LogFailedBucketNotificationsLoad logs failure to load bucket notifications
func LogFailedBucketNotificationsLoad(err error) {
	errorIf(err, funcName(), "Error loading bucket notifications", nil)
}

// LogFailedInitListenerTargetLogger logs failure to initialize listener target logger.
func LogFailedInitListenerTargetLogger(err error) {
	errorIf(err, funcName(), "Unable to initialize listener target logger.", nil)
}

// format-xl.go

// LogErrorDiskNotFound logs that a disk was not found in JBOD list.
func LogErrorDiskNotFound(err error, uuid string) {
	m := make(map[string]string)
	m["UUID"] = uuid
	errorIf(err, funcName(), "Disk not found in JBOD list.", m)
}

// LogDiskWrongOrder logs that disk is in wrong order.
func LogDiskWrongOrder(err error, uuid string, expectedIndex, index int) {
	m := make(map[string]string)
	m["UUID"] = uuid
	m["ExpectedOrder"] = strconv.Itoa(expectedIndex)
	m["ReceivedOrder"] = strconv.Itoa(index)
	errorIf(err, funcName(), "Disk is in wrong order.", m)
}

// fs-v1-multipart.go

// LogFailedAccessUploadsJSON logs failure to access uploads.json.
func LogFailedAccessUploadsJSON(err error, uploadIDPath string) {
	m := make(map[string]string)
	m["UploadIdsPath"] = uploadIDPath
	errorIf(err, funcName(), "Unable to access uploads.json.", m)
}

// LogFailedReadObject logs failure to read object.
func LogFailedReadObject(err error, bucket, object string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	m["ObjectName"] = object
	errorIf(err, funcName(), "Unable to read object.", m)
}

// LogFailedRemoveFile logs failure to remove file.
func LogFailedRemoveFile(err error, path, basePath string) {
	m := make(map[string]string)
	m["Path"] = path
	m["BasePath"] = basePath
	errorIf(err, funcName(), "Unable to remove file.", m)
}

// fs-v1-rwpool.go

// LogUnexpectedMapEntry logs that an unexpected entry was found in the map.
func LogUnexpectedMapEntry(err error, path string) {
	m := make(map[string]string)
	m["Path"] = path
	errorIf(err, funcName(), "Unexpected entry found in the map.", m)
}

// fs-v1.go

// LogGetDiskInfoFailed logs failure to get disk info.
func LogGetDiskInfoFailed(err error, path string) {
	m := make(map[string]string)
	m["Path"] = path
	errorIf(err, funcName(), "Unable to get disk info.", m)
}

// LogGetObjectInfoFailed logs failure to fetch object info.
func LogGetObjectInfoFailed(err error, bucket, object string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	m["ObjectName"] = object
	errorIf(err, funcName(), "Unable to fetch object info.", m)
}

// gateway-common.go

// LogExpectedTypeError logs Expected type *Error.
func LogExpectedTypeError(err error) {
	errorIf(err, funcName(), "Expected type *Error.", nil)
}

// gateway-handlers.go

// LogInvalidRequestRange logs an invalid request range.
func LogInvalidRequestRange(err error) {
	errorIf(err, funcName(), "Invalid request range.", nil)
}

// LogClientWriteFailed logs failure to save an object.
func LogClientWriteFailed(err error) {
	errorIf(err, funcName(), "Unable to write to client.", nil)
}

// LogFailedSaveObject logs failure to write to client.
func LogFailedSaveObject(err error, urlPath string) {
	m := make(map[string]string)
	m["URLPath"] = urlPath
	errorIf(err, funcName(), "Unable to save an object.", m)
}

// LogClientReadFailed logs failure to read from client.
func LogClientReadFailed(err error) {
	errorIf(err, funcName(), "Unable to read from client.", nil)
}

// gateway-main.go

// LogGatewayImplUninit logs missing gateway implementation.
func LogGatewayImplUninit(err error) {
	fatalIf(err, funcName(), "Gateway implementation not initialized, exiting.", nil)
}

// LogFailedInitGatewayLayer logs failure to initialize gateway layer.
func LogFailedInitGatewayLayer(err error) {
	fatalIf(err, funcName(), "Unable to initialize gateway layer", nil)
}

// LogFailedConfigureBrowser logs failure to configure web browser.
func LogFailedConfigureBrowser(err error) {
	fatalIf(err, funcName(), "Unable to configure web browser", nil)
}

// LogGatewayCredsNotSet logs that access and secret keys should be set through ENVs for backend.
func LogGatewayCredsNotSet(err error, gateway string) {
	m := make(map[string]string)
	m["Gateway"] = gateway
	errorIf(err, funcName(), "Access and Secret keys should be set through ENVs for backend.", m)
}

// LogResourceLimitChangeFailed logs failure to change resource limit.
func LogResourceLimitChangeFailed(err error) {
	errorIf(err, funcName(), "Unable to change resource limit.", nil)
}

// LogInvalidAddressInCli logs invalid address in CLI.
func LogInvalidAddressInCli(err error, addr string) {
	m := make(map[string]string)
	m["Addr"] = addr
	fatalIf(err, funcName(), "Invalid address in command line argument", m)
}

// LogInvalidArgsInCli logs invalid args in CLI.
func LogInvalidArgsInCli(err error, addr, args string) {
	m := make(map[string]string)
	m["Addr"] = addr
	m["Args"] = args
	fatalIf(err, funcName(), "Invalid command line arguments", m)
}

// LogPortAlreadyInUse logs port is already in use.
func LogPortAlreadyInUse(err error, port string) {
	m := make(map[string]string)
	m["Port"] = port
	fatalIf(err, funcName(), "Port already in use", m)
}

// LogMissingHTTPSCerts logs https cert missing.
func LogMissingHTTPSCerts(err error) {
	fatalIf(err, funcName(), "No certificates found for HTTPS endpoints.", nil)
}

// LogFailedCreateConfigDirs logs failure to create configuration directories.".
func LogFailedCreateConfigDirs(err error) {
	fatalIf(err, funcName(), "Unable to create configuration directories.", nil)
}

// LogInvalidSSLCert logs invalid SSL certificate file.
func LogInvalidSSLCert(err error) {
	fatalIf(err, funcName(), "Invalid SSL certificate file.", nil)
}

// LogFailedInitDistributedLockingClients logs failure to initialize distributed locking clients.
func LogFailedInitDistributedLockingClients(err error) {
	fatalIf(err, funcName(), "Unable to initialize distributed locking clients.", nil)
}

// LogFailedConfigRPCService logs failure to configure one of server's RPC services.
func LogFailedConfigRPCService(err error) {
	fatalIf(err, funcName(), "Unable to configure one of server's RPC services.", nil)
}

// gateway-azure.go

// LogFailedRemoveMetaDataObject logs failure to remove meta data object for upload ID.
func LogFailedRemoveMetaDataObject(err error, uploadID string) {
	m := make(map[string]string)
	m["UploadID"] = uploadID
	errorIf(err, funcName(), "unable to remove meta data object for upload ID.", m)
}

// gateway-gcs.go

// LogGCSMissingProjectID logs and erro message that project-id should be provided as argument or GOOGLE_APPLICATION_CREDENTIALS should be set with path to credentials.json.
func LogGCSMissingProjectID(err error) {
	errorIf(err, funcName(), "project-id should be provided as argument or GOOGLE_APPLICATION_CREDENTIALS should be set with path to credentials.json.", nil)
}

// LogGCSFailedToStart logs failure to start GCS gateway.
func LogGCSFailedToStart(err error, projectID string) {
	m := make(map[string]string)
	m["ProjectID"] = projectID
	errorIf(err, funcName(), "Unable to start GCS gateway.", m)
}

// LogGCSObjectListPurgeError logs failure to list objects during purging of old files in minio.sys.tmp.
func LogGCSObjectListPurgeError(err error, bucket string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	errorIf(err, funcName(), "Object listing error during purging of old files in minio.sys.tmp.", m)
}

// LogGCSDeleteObjectPurgeError logs failure to delete object during purging of old files in minio.sys.tmp
func LogGCSDeleteObjectPurgeError(err error, bucket, attribute string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	m["Attribute"] = attribute
	errorIf(err, funcName(), "Unable to delete object during purging of old files in minio.sys.tmp.", m)
}

// LogGCSListBucketPurgeError logs failure in bucket listing during purging of old files in minio.sys.tmp
func LogGCSListBucketPurgeError(err error) {
	errorIf(err, funcName(), "Bucket listing error during purging of old files in minio.sys.tmp.", nil)
}

// gateway-sia.go

// LogMissingSiaFilePath logs failure in finding file uploaded to Sia path.
func LogMissingSiaFilePath(err error, bucket, object string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	m["ObjectName"] = object
	errorIf(err, funcName(), "Unable to find file uploaded to Sia path.", m)
}

// LogInvalidArguments logs invalid arguments.
func LogInvalidArguments(err error) {
	fatalIf(err, funcName(), "Invalid Arguments.", nil)
}

// handler-utils.go

// LogFailedXMLDecodeConstraint logs failure to xml decode location constraint.
func LogFailedXMLDecodeConstraint(err error) {
	errorIf(err, funcName(), "Unable to xml decode location constraint.", nil)
}

// LogSplitHostFailed logs failure to split host.
func LogSplitHostFailed(err error, host string) {
	m := make(map[string]string)
	m["Host"] = host
	errorIf(err, funcName(), "Unable to split host.", m)
}

// jwt.go

// LogFailedJWTTokenParse logs failure to parse JWT token string.
func LogFailedJWTTokenParse(err error) {
	errorIf(err, funcName(), "Unable to parse JWT token string.", nil)
}

// LogInvaidJWTTokenClaims logs nnvalid claims in JWT token string.
func LogInvaidJWTTokenClaims(err error) {
	errorIf(err, funcName(), "Invalid claims in JWT token string.", nil)
}

// lock-rpc-server-common.go

// LogFailedRemoveWriteLock logs failure to maintain lock to remove entry for write lock",
func LogFailedRemoveWriteLock(err error, name, uid string) {
	m := make(map[string]string)
	m["Name"] = name
	m["Uid"] = uid
	errorIf(err, funcName(), "Lock maintenance failed to remove entry for write lock.", m)
}

// namespace-lock.go

// LogFailedSetLockStateBlocked logs failure to set lock state to blocked.
func LogFailedSetLockStateBlocked(err error, volume, path, opsID string) {
	m := make(map[string]string)
	m["Volume"] = volume
	m["Path"] = path
	m["OpsID"] = opsID
	errorIf(err, funcName(), "Failed to set lock state to blocked.", m)
}

// LogFailedClearLockState logs failure to clear the lock state.
func LogFailedClearLockState(err error, volume, path, opsID string) {
	m := make(map[string]string)
	m["Volume"] = volume
	m["Path"] = path
	m["OpsID"] = opsID
	errorIf(err, funcName(), "Failed to clear the lock state.", m)
}

// LogFailedDeleteLockInfoEntryForOpsID logs failure to delete lock info entry.
func LogFailedDeleteLockInfoEntryForOpsID(err error, volume, path, opsID string) {
	m := make(map[string]string)
	m["Volume"] = volume
	m["Path"] = path
	m["OpsID"] = opsID
	errorIf(err, funcName(), "Failed to delete lock info entry.", m)
}

// LogFailedDeleteLockInfoEntryForVolumePath logs failure to delete lock info entry.
func LogFailedDeleteLockInfoEntryForVolumePath(err error, volume, path string) {
	m := make(map[string]string)
	m["Volume"] = volume
	m["Path"] = path
	errorIf(err, funcName(), "Failed to delete lock info entry for volume path.", m)
}

// LogFailedSetLockStateRunning logs failure to set the lock state to running.
func LogFailedSetLockStateRunning(err error) {
	errorIf(err, funcName(), "Failed to set the lock state to running.", nil)
}

// LogInvalidReferenceCount logs invalid reference count detected
func LogInvalidReferenceCount(err error) {
	errorIf(err, funcName(), "Invalid reference count detected.", nil)
}

// LogFailedDeleteLockInfoEntry logs failure to delete lock info entry.
func LogFailedDeleteLockInfoEntry(err error) {
	errorIf(err, funcName(), "Failed to delete lock info entry for volume path.", nil)
}

// net-rpc-client.go

// LogFailedSecureConnection logs failure to establish secure connection"
func LogFailedSecureConnection(err error, addr string) {
	m := make(map[string]string)
	m["Address"] = addr
	errorIf(err, funcName(), "Failed to delete lock info entry for volume path.", m)
}

// net.go

// LogFailedHostResolve logs failure to resolve host"
func LogFailedHostResolve(err error, host, elapsedTime string) {
	m := make(map[string]string)
	m["Host"] = host
	m["ElapsedTime"] = elapsedTime
	errorIf(err, funcName(), "Unable to resolve host.", m)
}

// LogFailedGetIPAddress logs failure to delete lock info entry.
func LogFailedGetIPAddress(err error) {
	fatalIf(err, funcName(), "Unable to get IP addresses of this host.", nil)
}

// LogFailedSplitHostPort logs failure to split host port.
func LogFailedSplitHostPort(err error, hostPort string) {
	m := make(map[string]string)
	m["HostPort"] = hostPort
	fatalIf(err, funcName(), "Unable to split host port.", m)
}

// notifiers.go

// LogAMQPConnectFailed logs failure to connect to amqp service.
func LogAMQPConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to amqp service.", nil)
}

// LogMQTTConnectFailed logs failure to connect to mqtt service.
func LogMQTTConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to mqtt service.", nil)
}

// LogNATSConnectFailed logs failure to connect to nats service.
func LogNATSConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to nats service.", nil)
}

// LogRedisConnectFailed logs failure to connect to redis service.
func LogRedisConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to redis service.", nil)
}

// LogESConnectFailed logs failure to connect to elastic search service.
func LogESConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to elastic search service.", nil)
}

// LogPostGreSQLConnectFailed logs failure to connect to PostGreSQL Server.
func LogPostGreSQLConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to PostGreSQL service.", nil)
}

// LogMySQLConnectFailed logs failure to connect to MySQL server.
func LogMySQLConnectFailed(err error) {
	errorIf(err, funcName(), "Unable to connect to MySQL server.", nil)
}

// LogKafkaDialFailed logs failure to dial kafka server.
func LogKafkaDialFailed(err error) {
	errorIf(err, funcName(), "Unable to dial kafka server.", nil)
}

// notify-webhook.go

// LogLookupWebhookFailed logs failure to lookup webhook endpoint.
func LogLookupWebhookFailed(err error, urlStr string) {
	m := make(map[string]string)
	m["URL"] = urlStr
	errorIf(err, funcName(), "Unable to lookup webhook endpoint.", m)
}

// object-api-multipart-common.go

// LogListBucketsFailed logs failure to list buckets.
func LogListBucketsFailed(err error) {
	errorIf(err, funcName(), "Unable to list buckets.", nil)
}

// LogListUploadsFailed logs failure to list uploads.
func LogListUploadsFailed(err error) {
	errorIf(err, funcName(), "Unable to list uploads.", nil)
}

// object-handlers.go

// LogDeleteObjectFailed logs failure to delete object.
func LogDeleteObjectFailed(err error, bucket, object string) {
	m := make(map[string]string)
	m["Bucket"] = bucket
	m["Object"] = object
	errorIf(err, funcName(), "Unable to delete object.", m)
}

// LogFailedExtractRange logs failure to extract range header.
func LogFailedExtractRange(err error, rangeHeader string) {
	m := make(map[string]string)
	m["RangeHeader"] = rangeHeader
	errorIf(err, funcName(), "Unable to extract range.", m)
}

// posix-list-dir-others.go

// LogFailedStatPath logs failure to stat path.
func LogFailedStatPath(err error, path string) {
	m := make(map[string]string)
	m["Path"] = path
	errorIf(err, funcName(), "Unable to stat path.", m)
}

// posix.go

// LogDirAccessFailed logs failure to access directory
func LogDirAccessFailed(err error) {
	errorIf(err, funcName(), "Unable to access directory.", nil)
}

// LogListDirFailed logs failure to list Directory.
func LogListDirFailed(err error) {
	errorIf(err, funcName(), "Unable to list directory.", nil)
}

// prepare-storage.go

// LogDiskUnreachable logs disk is still unreachable.
func LogDiskUnreachable(err error, disk string) {
	m := make(map[string]string)
	m["Disk"] = disk
	errorIf(err, funcName(), "Disk is still unreachable.", m)
}

// LogDiskWrongFormat logs disk detected in unexpected format.
func LogDiskWrongFormat(err error, disk string) {
	m := make(map[string]string)
	m["Disk"] = disk
	errorIf(err, funcName(), "Disk detected in unexpected format.", m)
}

// rpc-server.go

// LogRPCHijackingFailed logs failure of rpc hijacking.
func LogRPCHijackingFailed(err error, remoteAddr string) {
	m := make(map[string]string)
	m["RemoteAddr"] = remoteAddr
	errorIf(err, funcName(), "rpc hijacking failed.", m)
}

//s3-peer-client.go

// LogPeerIndexOutofBounds logs peerIndex out of bounds"
func LogPeerIndexOutofBounds(err error, index int) {
	m := make(map[string]string)
	m["Index"] = strconv.Itoa(index)
	errorIf(err, funcName(), "peerIndex out of bounds.", m)
}

// LogUpdateBucketNotificationError logs failure to send update of bucket notification.
func LogUpdateBucketNotificationError(err error, addr string) {
	m := make(map[string]string)
	m["Addr"] = addr
	errorIf(err, funcName(), "Error sending update bucket notification.", m)
}

// LogUpdateBucketListenerError logs failure to sending update bucket listener.
func LogUpdateBucketListenerError(err error, addr string) {
	m := make(map[string]string)
	m["Addr"] = addr
	errorIf(err, funcName(), "Error sending update bucket listener.", m)
}

// LogFailedPolicyChangeMarshal logs failure to marshal policyChange",
func LogFailedPolicyChangeMarshal(err error) {
	errorIf(err, funcName(), "Failed to marshal policyChange.", nil)
}

// LogUpdateBucketPolicyError logs failure sending update bucket policy"
func LogUpdateBucketPolicyError(err error, addr string) {
	m := make(map[string]string)
	m["Addr"] = addr
	errorIf(err, funcName(), "Error sending update bucket policy.", m)
}

// server-main.go

// LogInitObjectLayerFailed logs failure to initialize object layer.
func LogInitObjectLayerFailed(err error) {
	errorIf(err, funcName(), "Initializing object layer failed.", nil)
}

// LogShutdownServerFailed logs failure to shutdown http server"
func LogShutdownServerFailed(err error) {
	errorIf(err, funcName(), "Unable to shutdown http server.", nil)
}

// signals.go

// LogStopObjectLayerFailed logs failure to shutdown object layer.
func LogStopObjectLayerFailed(err error) {
	errorIf(err, funcName(), "Unable to shutdown object layer.", nil)
}

// LogServerExitAbnormal logs abnormal exit of http server.
func LogServerExitAbnormal(err error) {
	errorIf(err, funcName(), "http server exited abnormally.", nil)
}

// LogServerRestartFailed logs failure to restart the server.
func LogServerRestartFailed(err error) {
	errorIf(err, funcName(), "Unable to restart the server.", nil)
}

// update-main.go

// LogDockerCheckError logs error in docker check.
func LogDockerCheckError(err error) {
	errorIf(err, funcName(), "Error in docker check.", nil)
}

// LogBOSHCheckError logs error in BOSH check.
func LogBOSHCheckError(err error) {
	errorIf(err, funcName(), "Error in BOSH check.", nil)
}

// LogReadHelmFileFailed logs failure to read helm info file.
func LogReadHelmFileFailed(err error, helmPath string) {
	m := make(map[string]string)
	m["HelmPath"] = helmPath
	errorIf(err, funcName(), "Unable to read helm info file.", m)
}

// utils.go

// LogHTTPTraceCloseFailed logs failure to close httpTraceFile.
func LogHTTPTraceCloseFailed(err error, httpTrace string) {
	m := make(map[string]string)
	m["HttpTraceFile"] = httpTrace
	errorIf(err, funcName(), "Unable to close httpTraceFile.", m)
}

// web-handlers.go

// LogRemoteLoginReqFailed logs failure to login request from remote.
func LogRemoteLoginReqFailed(err error, addr string) {
	m := make(map[string]string)
	m["Addr"] = addr
	errorIf(err, funcName(), "Unable to login request from remote.", m)
}

// LogCredsPropagationFailed logs failure to change credentials.
func LogCredsPropagationFailed(err error) {
	errorIf(err, funcName(), "Credentials change could not be propagated successfully!", nil)
}

// LogUnexpectedError logs Unexpected Error".
func LogUnexpectedError(err error) {
	errorIf(err, funcName(), "Unexpected Error", nil)
}

// xl-v1-common.go

// LogStatXLObjectFileFailed logs failure to stat file.
func LogStatXLObjectFileFailed(err error, bucket, prefix, xlMetaJSON string) {
	m := make(map[string]string)
	m["BucketName"] = bucket
	m["Prefix"] = prefix
	m["XLMetaJSON"] = xlMetaJSON
	errorIf(err, funcName(), "Unable to stat file.", m)
}

// xl-v1-utils.go

// LogInternalDiskEvaluateFailed logs failure to evaluate internal disks.
func LogInternalDiskEvaluateFailed(err error) {
	errorIf(err, funcName(), "unable to evaluate internal disks.", nil)
}

// xl-v1.go

// LogFailedInitXLObjectLayer logs failure to initialize XL object layer.
func LogFailedInitXLObjectLayer(err error) {
	errorIf(err, funcName(), "Unable to initialize XL object layer.", nil)
}

// LogFailedLoadBucketPolicies logs failure to load all bucket policies.
func LogFailedLoadBucketPolicies(err error) {
	errorIf(err, funcName(), "Unable to load all bucket policies.", nil)
}

// LogFailedInitEventNotification logs failure to initialize event notification.
func LogFailedInitEventNotification(err error) {
	errorIf(err, funcName(), "Unable to initialize event notification.", nil)
}

// LogGetMaxCacheSizeFailed logs failure to get maximum cache size.
func LogGetMaxCacheSizeFailed(err error) {
	errorIf(err, funcName(), "Unable to get maximum cache size.", nil)
}

// LogFetchDiskInfoFailed logs failure to fetch disk info.
func LogFetchDiskInfoFailed(err error, disk string) {
	m := make(map[string]string)
	m["Disk"] = disk
	errorIf(err, funcName(), "Unable to fetch disk info.", m)
}

// test-utils_test.go

// LogFailedCreateEndPointList logs failure to create new endpoint.
func LogFailedCreateEndPointList(err error) {
	errorIf(err, funcName(), "unable to create new endpoint list.", nil)
}

// LogFailedCreateEndPoint logs failure to create new endpoint.
func LogFailedCreateEndPoint(err error) {
	errorIf(err, funcName(), "unable to create new endpoint.", nil)
}
