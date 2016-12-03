/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"errors"
	"fmt"
	"io"
)

// Converts underlying storage error. Convenience function written to
// handle all cases where we have known types of errors returned by
// underlying storage layer.
func toObjectErr(err error, params ...string) error {
	e, ok := err.(*Error)
	if ok {
		err = e.e
	}

	switch err {
	case errVolumeNotFound:
		if len(params) >= 1 {
			err = eBucketNotFound(params[0])
		}
	case errVolumeNotEmpty:
		if len(params) >= 1 {
			err = eBucketNotEmpty(params[0])
		}
	case errVolumeExists:
		if len(params) >= 1 {
			err = eBucketExists(params[0])
		}
	case errDiskFull:
		err = eStorageFull()
	case errFileAccessDenied:
		if len(params) >= 2 {
			err = ePrefixAccessDenied(
				params[0],
				params[1],
			)
		}
	case errIsNotRegular:
		if len(params) >= 2 {
			err = eObjectExistsAsDirectory(
				params[0],
				params[1],
			)
		}
	case errFileNotFound:
		if len(params) >= 2 {
			err = eObjectNotFound(
				params[0],
				params[1],
			)
		}
	case errFileNameTooLong:
		if len(params) >= 2 {
			err = eObjectNameInvalid(
				params[0],
				params[1],
			)
		}
	case errXLReadQuorum:
		err = eInsufficientReadQuorum()
	case errXLWriteQuorum:
		err = eInsufficientWriteQuorum()
	case io.ErrUnexpectedEOF, io.ErrShortWrite:
		err = eIncompleteBody()
	case errFaultyDisk:
		err = eFaultyDisk()
	}
	if ok {
		e.e = err
		return e
	}
	return err
}

// IncompleteBody --
type IncompleteBody struct {
	APIErrorResponse
}

func eIncompleteBody() error {
	e := IncompleteBody{}
	e.ErrCode = ErrIncompleteBody
	e.Message = "You did not provide the number of bytes specified by the Content-Length HTTP header."
	return e
}

// SHA256Mismatch - when content sha256 does not match with what was sent from client.
type SHA256Mismatch struct {
	APIErrorResponse
	ClientSHA256 string `xml:"ClientComputedContentSHA256"`
	ServerSHA256 string `xml:"S3ComputedContentSHA256"`
}

func eSHA256Mismatch(clientSha256, serverSha256 string) error {
	e := SHA256Mismatch{}
	e.ClientSHA256 = clientSha256
	e.ServerSHA256 = serverSha256
	e.ErrCode = ErrContentSHA256Mismatch
	e.Message = "The provided 'x-amz-content-sha256' header does not match what was computed."
	return e
}

type InvalidDigest struct {
	APIErrorResponse
	ContentMD5 string `xml:"ContentMd5"`
}

func eInvalidDigest(contentMD5 string) error {
	e := InvalidDigest{}
	e.ContentMD5 = contentMD5
	e.ErrCode = ErrInvalidDigest
	e.Message = "The Content-MD5 you specified was invalid"
	return e
}

// BadDigest - Content-MD5 you specified did not match what we received.
type BadDigest struct {
	APIErrorResponse
	CalculatedDigest string
	ExpectedDigest   string
}

func eBadDigest(calcDigest, expectedDigest string) error {
	e := BadDigest{}
	e.CalculatedDigest = calcDigest
	e.ExpectedDigest = expectedDigest
	e.ErrCode = ErrBadDigest
	e.Message = "The Content-Md5 you specified did not match what we received."
	return e
}

// FaultyDisk - disk is found to be faulty.
type FaultyDisk struct {
	APIErrorResponse
}

func eFaultyDisk() error {
	e := FaultyDisk{}
	e.ErrCode = ErrFaultyDisk
	e.Message = "Storage backend is faulty or corrupted. Please check your server logs."
	return e
}

// StorageFull storage ran out of space.
type StorageFull struct {
	APIErrorResponse
}

func eStorageFull() error {
	e := StorageFull{}
	e.ErrCode = ErrStorageFull
	e.Message = "Storage backend has reached its minimum free disk threshold. Please delete few objects to proceed."
	return e
}

// InsufficientReadQuorum storage cannot satisfy quorum for read operation.
type InsufficientReadQuorum struct {
	APIErrorResponse
}

func eInsufficientReadQuorum() error {
	e := InsufficientReadQuorum{}
	e.ErrCode = ErrReadQuorum
	e.Message = "Multiple disk failures, unable to reconstruct data."
	return e
}

// InsufficientWriteQuorum storage cannot satisfy quorum for write operation.
type InsufficientWriteQuorum struct {
	APIErrorResponse
}

func eInsufficientWriteQuorum() error {
	e := InsufficientWriteQuorum{}
	e.ErrCode = ErrWriteQuorum
	e.Message = "Multiple disks failures, unable to write data."
	return e
}

// BucketNotFound bucket does not exist.
type BucketNotFound struct {
	APIErrorResponse
	Bucket string
}

func eBucketNotFound(bucket string) error {
	e := BucketNotFound{}
	e.Bucket = bucket
	e.ErrCode = ErrNoSuchBucket
	e.Message = "The specified bucket does not exist"
	return e
}

// BucketNotEmpty bucket is not empty.
type BucketNotEmpty struct {
	APIErrorResponse
	Bucket string
}

func eBucketNotEmpty(bucket string) error {
	e := BucketNotEmpty{}
	e.Bucket = bucket
	e.ErrCode = ErrBucketNotEmpty
	e.Message = "The bucket you tried to delete is not empty"
	return e
}

// ObjectNotFound object does not exist.
type ObjectNotFound struct {
	APIErrorResponse
	Bucket string
	Object string `xml:"Key"`
}

func eObjectNotFound(bucket, object string) error {
	e := ObjectNotFound{}
	e.Bucket = bucket
	e.Object = object
	e.ErrCode = ErrNoSuchKey
	e.Message = "The specified key does not exist."
	return e
}

// ObjectExistsAsDirectory object already exists as a directory.
type ObjectExistsAsDirectory struct {
	APIErrorResponse
	Bucket string
	Object string `xml:"Key"`
}

func eObjectExistsAsDirectory(bucket, object string) error {
	e := ObjectExistsAsDirectory{}
	e.Bucket = bucket
	e.Object = object
	e.ErrCode = ErrObjectExistsAsDirectory
	e.Message = "Object name already exists as a directory."
	return e
}

// PrefixAccessDenied object access is denied.
type PrefixAccessDenied struct {
	APIErrorResponse
	Bucket string
	Object string `xml:"Key"`
}

func ePrefixAccessDenied(bucket, object string) error {
	e := PrefixAccessDenied{}
	e.Bucket = bucket
	e.Object = object
	e.ErrCode = ErrAccessDenied
	e.Message = "Access Denied."
	return e
}

// BucketExists bucket exists.
type BucketExists struct {
	APIErrorResponse
	Bucket string
}

func eBucketExists(bucket string) error {
	e := BucketExists{}
	e.Bucket = bucket
	e.ErrCode = ErrBucketAlreadyOwnedByYou
	e.Message = "Your previous request to create the named bucket succeeded and you already own it."
	return e
}

// UnsupportedDelimiter - unsupported delimiter.
type UnsupportedDelimiter struct {
	APIErrorResponse
	Delimiter string
}

func eUnsupportedDelimiter(delimiter string) error {
	e := UnsupportedDelimiter{}
	e.Delimiter = delimiter
	e.ErrCode = ErrUnsupportedDelimiter
	e.Message = "Delimiter is not supported. '/' is the only supported delimiter."
	return e
}

// InvalidUploadIDKeyCombination - invalid upload id and key marker combination.
type InvalidUploadIDKeyCombination struct {
	APIErrorResponse
	UploadIDMarker, KeyMarker string
}

func eInvalidUploadIDKeyCombination(uploadIDMarker, keyMarker string) error {
	e := InvalidUploadIDKeyCombination{}
	e.UploadIDMarker = uploadIDMarker
	e.KeyMarker = keyMarker
	e.ErrCode = ErrInvalidUploadIDKeyCombination
	e.Message = "Invalid combination of uploadID marker and marker."
	return e
}

// InvalidMarkerPrefixCombination - invalid marker and prefix combination.
type InvalidMarkerPrefixCombination struct {
	APIErrorResponse
	Marker, Prefix string
}

func eInvalidMarkerPrefixCombination(marker, prefix string) error {
	e := InvalidMarkerPrefixCombination{}
	e.Marker = marker
	e.Prefix = prefix
	e.ErrCode = ErrInvalidMarkerKeyCombination
	e.Message = "Invalid combination of marker and prefix."
	return e
}

// BucketPolicyNotFound - no bucket policy found.
type BucketPolicyNotFound struct {
	APIErrorResponse
	Bucket string
}

func eBucketPolicyNotFound(bucket string) error {
	e := BucketPolicyNotFound{}
	e.Bucket = bucket
	e.ErrCode = ErrNoSuchBucketPolicy
	e.Message = "The bucket policy does not exist"
	return e
}

/// Bucket related errors.

// BucketNameInvalid - bucketname provided is invalid.
type BucketNameInvalid struct {
	APIErrorResponse
	Bucket string
}

func eBucketNameInvalid(bucket string) error {
	e := BucketNameInvalid{}
	e.Bucket = bucket
	e.ErrCode = ErrInvalidBucketName
	e.Message = "The specified bucket is not valid."
	return e
}

/// Object related errors.

// ObjectNameInvalid - object name provided is invalid.
type ObjectNameInvalid struct {
	APIErrorResponse
	Bucket string
	Object string `xml:"Key"`
}

func eObjectNameInvalid(bucket, object string) error {
	e := ObjectNameInvalid{}
	e.Bucket = bucket
	e.Object = object
	e.ErrCode = ErrInvalidObjectName
	e.Message = `Object name contains unsupported characters. Unsupported characters are '\'`
	return e
}

// ObjectTooLarge error returned when the size of the object > max object size allowed (5G) per request.
type ObjectTooLarge struct {
	APIErrorResponse
	MaxSizeAllowed int64
	ProposedSize   int64
}

func eObjectTooLarge(maxSizeAllowed, proposedSize int64) error {
	e := ObjectTooLarge{}
	e.MaxSizeAllowed = maxSizeAllowed
	e.ProposedSize = proposedSize
	e.ErrCode = ErrEntityTooLarge
	e.Message = "Your proposed upload exceeds the maximum allowed object size."
	return e
}

// ObjectTooSmall error returned when the size of the object < what is expected.
type ObjectTooSmall struct {
	APIErrorResponse
	MinSizeAllowed int64
	ProposedSize   int64
}

func eObjectTooSmall(minSizeAllowed, proposedSize int64) error {
	e := ObjectTooSmall{}
	e.MinSizeAllowed = minSizeAllowed
	e.ProposedSize = proposedSize
	e.ErrCode = ErrEntityTooSmall
	e.Message = "Your proposed upload is smaller than the minimum allowed object size."
	return e
}

// errInvalidRange - returned when given range value is not valid.
var errInvalidRange = errors.New("Invalid range")

// InvalidRange - invalid range typed error.
type InvalidRange struct {
	APIErrorResponse
	offsetBegin  int64
	offsetEnd    int64
	resourceSize int64
}

func eInvalidRange(offsetBegin, offsetEnd, resourceSize int64) error {
	e := InvalidRange{}
	e.offsetBegin = offsetBegin
	e.offsetEnd = offsetEnd
	e.resourceSize = resourceSize
	e.ErrCode = ErrInvalidRange
	e.Message = "The requested range is not satisfiable"
	return e
}

/// Multipart related errors.

// MalformedUploadID malformed upload id.
type MalformedUploadID InvalidUploadID

func eMalformedUploadID(bucket, object, uploadID string) error {
	e := MalformedUploadID{}
	e.Bucket = bucket
	e.Object = object
	e.UploadID = uploadID
	e.ErrCode = ErrNoSuchUpload
	e.Message = "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
	return e
}

// InvalidUploadID invalid upload id.
type InvalidUploadID struct {
	APIErrorResponse
	Bucket   string
	Object   string `xml:"Key"`
	UploadID string
}

func eInvalidUploadID(bucket, object, uploadID string) error {
	e := InvalidUploadID{}
	e.Bucket = bucket
	e.Object = object
	e.UploadID = uploadID
	e.ErrCode = ErrNoSuchUpload
	e.Message = "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
	return e
}

// InvalidPart One or more of the specified parts could not be found
type InvalidPart struct {
	APIErrorResponse
	Bucket     string
	Object     string `xml:"Key"`
	UploadID   string `xml:"UploadId"`
	PartNumber int
}

func eInvalidPart(bucket, object, uploadID string, partNumber int) error {
	e := InvalidPart{}
	e.Bucket = bucket
	e.Object = object
	e.UploadID = uploadID
	e.PartNumber = partNumber
	e.ErrCode = ErrInvalidPart
	e.Message = "One or more of the specified parts could not be found."
	return e
}

// PartTooSmall - error if part size is less than 5MB.
type PartTooSmall struct {
	APIErrorResponse
	// Proposed size represents uploaded size of the part.
	ProposedSize int64
	// Minimum size allowed epresents the minimum size allowed per
	// part. Defaults to 5MB.
	MinSizeAllowed int64
	// Part number of the part which is incorrect.
	PartNumber int
	// ETag of the part which is incorrect.
	PartETag string
	// Other default XML error responses.
}

func ePartTooSmall(proposedSize, minSizeAllowed int64, partNumber int, partETag string) error {
	e := PartTooSmall{}
	e.ProposedSize = proposedSize
	e.MinSizeAllowed = minPartSize
	e.PartNumber = partNumber
	e.PartETag = partETag
	e.ErrCode = ErrEntityTooSmall
	e.Message = "Your proposed upload is smaller than the minimum allowed object size."
	return e
}

// NotImplemented If a feature is not implemented
type NotImplemented struct {
	APIErrorResponse
}

func eNotImplemented() error {
	e := NotImplemented{}
	e.ErrCode = ErrNotImplemented
	e.Message = "A header you provided implies functionality that is not implemented"
	return e
}

type InvalidCopyDest struct {
	APIErrorResponse
	CopySource string
	CopyDest   string
}

func eInvalidCopyDest(copySource, copyDest string) error {
	e := InvalidCopyDest{}
	e.CopySource = copySource
	e.CopyDest = copyDest
	e.ErrCode = ErrInvalidRequest
	e.Message = "This copy request is illegal because it is trying to copy an object to itself."
	return e
}

type InvalidCopySource struct {
	APIErrorResponse
	CopySource string
}

func eInvalidCopySource(copySource string) error {
	e := InvalidCopySource{}
	e.CopySource = copySource
	e.ErrCode = ErrInvalidArgument
	e.Message = "Copy Source must mention the source bucket and key: sourcebucket/sourcekey."
	return e
}

type ServerNotInitialized struct {
	APIErrorResponse
}

func eServerNotInitialized() error {
	e := ServerNotInitialized{}
	e.ErrCode = ErrServerNotInitialized
	e.Message = "Server not initialized, please try again."
	return e
}

type InvalidRequestBody struct {
	APIErrorResponse
}

func eInvalidRequestBody() error {
	e := InvalidRequestBody{}
	e.ErrCode = ErrInvalidRequestBody
	e.Message = "Body shouldn't be set for this request."
	return e
}

type InvalidMaxUploads struct {
	APIErrorResponse
	MaxUploads int
}

func eInvalidMaxUploads(maxUploads int) error {
	e := InvalidMaxUploads{}
	e.MaxUploads = maxUploads
	e.ErrCode = ErrInvalidMaxUploads
	e.Message = "Argument max-uploads must be an integer between 0 and 2147483647"
	return e
}

type InvalidMaxKeys struct {
	APIErrorResponse
	MaxKeys int
}

func eInvalidMaxKeys(maxKeys int) error {
	e := InvalidMaxKeys{}
	e.MaxKeys = maxKeys
	e.ErrCode = ErrInvalidMaxKeys
	e.Message = "Argument maxKeys must be an integer between 0 and 2147483647"
	return e
}

type InvalidMaxParts struct {
	APIErrorResponse
	MaxParts int
}

func eInvalidMaxParts(maxParts int) error {
	e := InvalidMaxParts{}
	e.MaxParts = maxParts
	e.ErrCode = ErrInvalidMaxParts
	e.Message = "Argument max-parts must be an integer between 0 and 2147483647"
	return e
}

type InvalidPartNumberMarker struct {
	APIErrorResponse
	PartNumberMarker int
}

func eInvalidPartNumberMarker(partNumberMarker int) error {
	e := InvalidPartNumberMarker{}
	e.PartNumberMarker = partNumberMarker
	e.ErrCode = ErrInvalidPartNumberMarker
	e.Message = "Argument partNumberMarker must be an integer."
	return e
}

type InvalidPolicyDocument struct {
	APIErrorResponse
	Bucket string
}

func eInvalidPolicyDocument(bucket string) error {
	e := InvalidPolicyDocument{}
	e.Bucket = bucket
	e.ErrCode = ErrInvalidPolicyDocument
	e.Message = "The content of the form does not meet the conditions specified in the policy document."
	return e
}

type InvalidAccessKeyID struct {
	APIErrorResponse
}

func eInvalidAccessKeyID() error {
	e := InvalidAccessKeyID{}
	e.ErrCode = ErrInvalidAccessKeyID
	e.Message = "The access key ID you provided does not exist in our records."
	return e
}

type MalformedXML struct {
	APIErrorResponse
}

func eMalformedXML() error {
	e := MalformedXML{}
	e.ErrCode = ErrMalformedXML
	e.Message = "The XML you provided was not well-formed or did not validate against our published schema."
	return e
}

type MissingContentLength struct {
	APIErrorResponse
}

func eMissingContentLength() error {
	e := MissingContentLength{}
	e.ErrCode = ErrMissingContentLength
	e.Message = "You must provide the Content-Length HTTP header."
	return e
}

type MissingContentMD5 struct {
	APIErrorResponse
}

func eMissingContentMD5() error {
	e := MissingContentMD5{}
	e.ErrCode = ErrMissingContentMD5
	e.Message = "Missing required header for this request: Content-Md5."
	return e
}

type PreconditionFailed struct {
	APIErrorResponse
}

func ePreconditionFailed() error {
	e := PreconditionFailed{}
	e.ErrCode = ErrPreconditionFailed
	e.Message = "At least one of the pre-conditions you specified did not hold"
	return e
}

type RequestTimeTooSkewed struct {
	APIErrorResponse
}

func eRequestTimeTooSkewed() error {
	e := RequestTimeTooSkewed{}
	e.ErrCode = ErrRequestTimeTooSkewed
	e.Message = "The difference between the request time and the server's time is too large."
	return e
}

type SignatureDoesNotMatch struct {
	APIErrorResponse
	AWSAccessKeyID        string `xml:"AWSAccessKeyId"`
	SignatureProvided     string
	StringToSign          string `xml:"StringToSign,omitempty"`
	StringToSignBytes     string `xml:"StringToSignBytes,omitempty"`
	CanonicalRequest      string `xml:"CanonicalRequest,omitempty"`
	CanonicalRequestBytes string `xml:"CanonicalRequestBytes,omitempty"`
}

func eSignatureDoesNotMatch(accessKeyID, signProvided, strToSign, strToSignBytes, canReq, canReqBytes string) error {
	e := SignatureDoesNotMatch{}
	e.AWSAccessKeyID = accessKeyID
	e.SignatureProvided = signProvided
	e.StringToSign = strToSign
	e.StringToSignBytes = strToSignBytes
	e.CanonicalRequest = canReq
	e.CanonicalRequestBytes = canReqBytes
	e.ErrCode = ErrSignatureDoesNotMatch
	e.Message = "The request signature we calculated does not match the signature you provided. Check your key and signing method."
	return e
}

type MethodNotAllowed struct {
	APIErrorResponse
}

func eMethodNotAllowed() error {
	e := MethodNotAllowed{}
	e.ErrCode = ErrMethodNotAllowed
	e.Message = "The specified method is not allowed against this resource."
	return e
}

type InvalidPartOrder struct {
	APIErrorResponse
}

func eInvalidPartOrder() error {
	e := InvalidPartOrder{}
	e.ErrCode = ErrInvalidPartOrder
	e.Message = "The list of parts was not in ascending order. The parts list must be specified in order by part number."
	return e
}

type InvalidObjectState struct {
	APIErrorResponse
	Bucket string
	Object string `xml:"Key"`
}

func eInvalidObjectState(bucket, object string) error {
	e := InvalidObjectState{}
	e.Bucket = bucket
	e.Object = object
	e.ErrCode = ErrInvalidObjectState
	e.Message = "The operation is not valid for the current state of the object."
	return e
}

type AccessDenied struct {
	APIErrorResponse
}

func eAccessDenied() error {
	e := AccessDenied{}
	e.ErrCode = ErrAccessDenied
	e.Message = "Access Denied."
	return e
}

type AuthHeaderMalformed struct {
	APIErrorResponse
	clientRegion string
	ServerRegion string `xml:"Region"`
}

func eAuthHeaderMalfomed(clntRegion, srvRegion string) error {
	e := AuthHeaderMalformed{}
	e.clientRegion = clntRegion
	e.ServerRegion = srvRegion
	e.ErrCode = ErrAuthorizationHeaderMalformed
	e.Message = fmt.Sprintf("The authorization header is malformed; the region '%s' is wrong; expecting '%s'",
		e.clientRegion, e.ServerRegion)
	return e
}

type MalformedPOSTRequest struct {
	APIErrorResponse
}

func eMalformedPOSTReq() error {
	e := MalformedPOSTRequest{}
	e.ErrCode = ErrMalformedPOSTRequest
	e.Message = "The body of your POST request is not well-formed multipart/form-data."
	return e
}

type SignatureNotSupported struct {
	APIErrorResponse
}

func eSignatureNotSupported() error {
	e := SignatureNotSupported{}
	e.ErrCode = ErrSignatureVersionNotSupported
	e.Message = "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256."
	return e
}

type AllAccessDisabled struct {
	APIErrorResponse
	Bucket string
}

func eAllAccessDisabled(bucket string) error {
	e := AllAccessDisabled{}
	e.Bucket = bucket
	e.ErrCode = ErrAllAccessDisabled
	e.Message = "All access to this bucket has been disabled."
	return e
}

type PolicyNesting struct {
	APIErrorResponse
	ServerResource    string
	RequestedResource string
}

func ePolicyNesting(srvRes, reqRes string) error {
	e := PolicyNesting{}
	e.ErrCode = ErrPolicyNesting
	e.Message = fmt.Sprintf("Requested policy resource %s conflicts with an existing policy resource %s.",
		e.RequestedResource,
		e.ServerResource)
	e.ServerResource = srvRes
	e.RequestedResource = reqRes
	return e
}

type MalformedPolicy struct {
	APIErrorResponse
}

func eMalformedPolicy() error {
	e := MalformedPolicy{}
	e.ErrCode = ErrMalformedPolicy
	e.Message = "Policy has invalid resource."
	return e
}

type MissingFields struct {
	APIErrorResponse
}

func eMissingFields() error {
	e := MissingFields{}
	e.ErrCode = ErrMissingFields
	e.Message = "Missing fields in request."
	return e
}

type MissingCredTag struct {
	APIErrorResponse
}

func eMissingCredTag() error {
	e := MissingCredTag{}
	e.ErrCode = ErrMissingCredTag
	e.Message = "Missing Credential field for this request."
	return e
}

type CredMalformed struct {
	APIErrorResponse
	AccessKeyID string `xml:"AccessKeyId"`
}

func eCredMalformed(accessKey string) error {
	e := CredMalformed{}
	e.AccessKeyID = accessKey
	e.ErrCode = ErrCredMalformed
	e.Message = fmt.Sprintf("Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"%s/YYYYMMDD/REGION/SERVICE/aws4_request\".", e.AccessKeyID)
	return e
}

type MalformedDate struct {
	APIErrorResponse
	Date string `xml:"XAmzDate"`
}

func eMalformedDate(dateStr string) error {
	e := MalformedDate{}
	e.Date = dateStr
	e.ErrCode = ErrMalformedDate
	e.Message = "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format."
	return e
}

type MalformedPresignedDate struct {
	APIErrorResponse
	Date string
}

func eMalformedPresignedDate(dateStr string) error {
	e := MalformedPresignedDate{}
	e.Date = dateStr
	e.ErrCode = ErrMalformedPresignedDate
	e.Message = "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\""
	return e
}

type MalformedCredentialDate struct {
	APIErrorResponse
}

func eMalformedCredentialDate(reqDateStr string) error {
	e := MalformedCredentialDate{}
	e.ErrCode = ErrMalformedCredentialDate
	e.Message = fmt.Sprintf("Error parsing the X-Amz-Credential parameter; incorrect date format \"%s\". This date in the credential must be in the format \"yyyyMMdd\".", reqDateStr)
	return e
}

type MalformedCredentialRegion struct {
	APIErrorResponse
	ServerRegion string `xml:"Region"`
	clientRegion string
}

func eMalformedCredentialRegion(srvRegion, clntRegion string) error {
	e := MalformedCredentialRegion{}
	e.ServerRegion = srvRegion
	e.clientRegion = clntRegion
	e.ErrCode = ErrMalformedCredentialRegion
	e.Message = fmt.Sprintf("Error parsing the X-Amz-Credential parameter; the region '%s' is wrong; expecting '%s'",
		e.clientRegion, e.ServerRegion)
	return e
}

type InvalidService struct {
	APIErrorResponse
	clientService string
}

func eInvalidService(clntService string) error {
	e := InvalidService{}
	e.clientService = clntService
	e.ErrCode = ErrInvalidService
	e.Message = fmt.Sprintf("Error parsing the X-Amz-Credential parameter; incorrect service \"%s\". This endpoint belongs to \"s3\".",
		e.clientService)
	return e
}

type InvalidReqVersion struct {
	APIErrorResponse
	clientReqVersion string
}

func eInvalidReqVersion(clntReqVersion string) error {
	e := InvalidReqVersion{}
	e.clientReqVersion = clntReqVersion
	e.ErrCode = ErrInvalidRequestVersion
	e.Message = fmt.Sprintf("Error parsing the X-Amz-Credential parameter; incorrect terminal \"%s\". This endpoint uses \"aws4_request\"",
		e.clientReqVersion)
	return e
}

type MissingSignTag struct {
	APIErrorResponse
}

func eMissingSignTag() error {
	e := MissingSignTag{}
	e.ErrCode = ErrMissingSignTag
	e.Message = "Signature header missing Signature field."
	return e
}

type MissingSignHeadersTag struct {
	APIErrorResponse
}

func eMissingSignHeadersTag() error {
	e := MissingSignHeadersTag{}
	e.ErrCode = ErrMissingSignHeadersTag
	e.Message = "Signature header missing SignedHeaders field."
	return e
}

type PolicyAlreadyExpired struct {
	APIErrorResponse
}

func ePolicyAlreadyExpired() error {
	e := PolicyAlreadyExpired{}
	e.ErrCode = ErrPolicyAlreadyExpired
	e.Message = "Invalid according to Policy: Policy expired."
	return e
}

type MalformedExpires struct {
	APIErrorResponse
}

func eMalformedExpires() error {
	e := MalformedExpires{}
	e.ErrCode = ErrMalformedExpires
	e.Message = "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds"
	return e
}

type NegativeExpires struct {
	APIErrorResponse
}

func eNegativeExpires() error {
	e := NegativeExpires{}
	e.ErrCode = ErrNegativeExpires
	e.Message = "X-Amz-Expires must be non-negative"
	return e
}

type AuthHeaderEmpty struct {
	APIErrorResponse
}

func eAuthHeaderEmpty() error {
	e := AuthHeaderEmpty{}
	e.ErrCode = ErrAuthHeaderEmpty
	e.Message = "Authorization header is invalid -- one and only one ' ' (space) required."
	return e
}

type MissingDateHeader struct {
	APIErrorResponse
}

func eMissingDateHeader() error {
	e := MissingDateHeader{}
	e.ErrCode = ErrMissingDateHeader
	e.Message = "AWS authentication requires a valid Date or x-amz-date header"
	return e
}

type InvalidQuerySignAlgo struct {
	APIErrorResponse
}

func eInvalidQuerySignAlgo() error {
	e := InvalidQuerySignAlgo{}
	e.ErrCode = ErrInvalidQuerySignAlgo
	e.Message = "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\"."
	return e
}

type ExpiredPresignRequest struct {
	APIErrorResponse
	// TODO - check for more fields for this error xml.
}

func eExpiredPresignRequest() error {
	e := ExpiredPresignRequest{}
	e.ErrCode = ErrExpiredPresignRequest
	e.Message = "Request has expired"
	return e
}

type RequestNotReadyYet struct {
	APIErrorResponse
}

func eRequestNotReadyYet() error {
	e := RequestNotReadyYet{}
	e.ErrCode = ErrRequestNotReadyYet
	e.Message = "Request is not valid yet"
	return e
}

type UnsignedHeaders struct {
	APIErrorResponse
	HeadersNotSigned string
}

func eUnsignedHeaders(hdrsNotSigned string) error {
	e := UnsignedHeaders{}
	e.HeadersNotSigned = hdrsNotSigned
	e.ErrCode = ErrUnsignedHeaders
	e.Message = "There were headers present in the request which were not signed"
	return e
}

type InvalidQueryParams struct {
	APIErrorResponse
}

func eInvalidQueryParams() error {
	e := InvalidQueryParams{}
	e.ErrCode = ErrInvalidQueryParams
	e.Message = "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters."
	return e
}

type InvalidRegion struct {
	APIErrorResponse
	clientRegion string
	ServerRegion string `xml:"Region"`
}

func eInvalidRegion(clntRegion, srvRegion string) error {
	e := InvalidRegion{}
	e.clientRegion = clntRegion
	e.ServerRegion = srvRegion
	e.ErrCode = ErrInvalidRegion
	e.Message = fmt.Sprintf("Region does not match; the region '%s' is wrong; expecting '%s'",
		e.clientRegion, e.ServerRegion)
	return e
}

type InternalError struct {
	APIErrorResponse
}

func eInternalError() error {
	e := InternalError{}
	e.ErrCode = ErrInternalError
	e.Message = "We encountered an internal error, please try again."
	return e
}

// Check if error type is IncompleteBody.
func isErrIncompleteBody(err error) bool {
	err = errorCause(err)
	switch err.(type) {
	case IncompleteBody:
		return true
	}
	return false
}

// Check if error type is BucketPolicyNotFound.
func isErrBucketPolicyNotFound(err error) bool {
	err = errorCause(err)
	switch err.(type) {
	case BucketPolicyNotFound:
		return true
	}
	return false
}

// Check if error type is ObjectNameInvalid.
func isErrObjectNameInvalid(err error) bool {
	err = errorCause(err)
	switch err.(type) {
	case ObjectNameInvalid:
		return true
	}
	return false
}

// Check if error type is ObjectNotFound.
func isErrObjectNotFound(err error) bool {
	err = errorCause(err)
	switch err.(type) {
	case ObjectNotFound:
		return true
	}
	return false
}
