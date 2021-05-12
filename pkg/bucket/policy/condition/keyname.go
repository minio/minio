// Copyright (c) 2015-2021 MinIO, Inc.
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

package condition

import (
	"fmt"
	"strings"
)

// KeyName - conditional key which is used to fetch values for any condition.
// Refer https://docs.aws.amazon.com/IAM/latest/UserGuide/list_s3.html
// for more information about available condition keys.
type KeyName string

// Name - returns key name which is stripped value of prefixes "aws:" and "s3:"
func (key KeyName) Name() string {
	name := string(key)
	switch {
	case strings.HasPrefix(name, "aws:"):
		return strings.TrimPrefix(name, "aws:")
	case strings.HasPrefix(name, "jwt:"):
		return strings.TrimPrefix(name, "jwt:")
	case strings.HasPrefix(name, "ldap:"):
		return strings.TrimPrefix(name, "ldap:")
	default:
		return strings.TrimPrefix(name, "s3:")
	}
}

// VarName - returns variable key name, such as "${aws:username}"
func (key KeyName) VarName() string {
	return fmt.Sprintf("${%s}", key)
}

// ToKey - creates key from name.
func (key KeyName) ToKey() Key {
	return NewKey(key, "")
}

// Condition key names.
const (
	// S3XAmzCopySource - key representing x-amz-copy-source HTTP header applicable to PutObject API only.
	S3XAmzCopySource KeyName = "s3:x-amz-copy-source"

	// S3XAmzServerSideEncryption - key representing x-amz-server-side-encryption HTTP header applicable
	// to PutObject API only.
	S3XAmzServerSideEncryption KeyName = "s3:x-amz-server-side-encryption"

	// S3XAmzServerSideEncryptionCustomerAlgorithm - key representing
	// x-amz-server-side-encryption-customer-algorithm HTTP header applicable to PutObject API only.
	S3XAmzServerSideEncryptionCustomerAlgorithm KeyName = "s3:x-amz-server-side-encryption-customer-algorithm"

	// S3XAmzMetadataDirective - key representing x-amz-metadata-directive HTTP header applicable to
	// PutObject API only.
	S3XAmzMetadataDirective KeyName = "s3:x-amz-metadata-directive"

	// S3XAmzContentSha256 - set a static content-sha256 for all calls for a given action.
	S3XAmzContentSha256 KeyName = "s3:x-amz-content-sha256"

	// S3XAmzStorageClass - key representing x-amz-storage-class HTTP header applicable to PutObject API
	// only.
	S3XAmzStorageClass KeyName = "s3:x-amz-storage-class"

	// S3LocationConstraint - key representing LocationConstraint XML tag of CreateBucket API only.
	S3LocationConstraint KeyName = "s3:LocationConstraint"

	// S3Prefix - key representing prefix query parameter of ListBucket API only.
	S3Prefix KeyName = "s3:prefix"

	// S3Delimiter - key representing delimiter query parameter of ListBucket API only.
	S3Delimiter KeyName = "s3:delimiter"

	// S3VersionID - Enables you to limit the permission for the
	// s3:PutObjectVersionTagging action to a specific object version.
	S3VersionID KeyName = "s3:versionid"

	// S3MaxKeys - key representing max-keys query parameter of ListBucket API only.
	S3MaxKeys KeyName = "s3:max-keys"

	// S3ObjectLockRemainingRetentionDays - key representing object-lock-remaining-retention-days
	// Enables enforcement of an object relative to the remaining retention days, you can set
	// minimum and maximum allowable retention periods for a bucket using a bucket policy.
	// This key are specific for s3:PutObjectRetention API.
	S3ObjectLockRemainingRetentionDays KeyName = "s3:object-lock-remaining-retention-days"

	// S3ObjectLockMode - key representing object-lock-mode
	// Enables enforcement of the specified object retention mode
	S3ObjectLockMode KeyName = "s3:object-lock-mode"

	// S3ObjectLockRetainUntilDate - key representing object-lock-retain-util-date
	// Enables enforcement of a specific retain-until-date
	S3ObjectLockRetainUntilDate KeyName = "s3:object-lock-retain-until-date"

	// S3ObjectLockLegalHold - key representing object-local-legal-hold
	// Enables enforcement of the specified object legal hold status
	S3ObjectLockLegalHold KeyName = "s3:object-lock-legal-hold"

	// AWSReferer - key representing Referer header of any API.
	AWSReferer KeyName = "aws:Referer"

	// AWSSourceIP - key representing client's IP address (not intermittent proxies) of any API.
	AWSSourceIP KeyName = "aws:SourceIp"

	// AWSUserAgent - key representing UserAgent header for any API.
	AWSUserAgent KeyName = "aws:UserAgent"

	// AWSSecureTransport - key representing if the clients request is authenticated or not.
	AWSSecureTransport KeyName = "aws:SecureTransport"

	// AWSCurrentTime - key representing the current time.
	AWSCurrentTime KeyName = "aws:CurrentTime"

	// AWSEpochTime - key representing the current epoch time.
	AWSEpochTime KeyName = "aws:EpochTime"

	// AWSPrincipalType - user principal type currently supported values are "User" and "Anonymous".
	AWSPrincipalType KeyName = "aws:principaltype"

	// AWSUserID - user unique ID, in MinIO this value is same as your user Access Key.
	AWSUserID KeyName = "aws:userid"

	// AWSUsername - user friendly name, in MinIO this value is same as your user Access Key.
	AWSUsername KeyName = "aws:username"

	// S3SignatureVersion - identifies the version of AWS Signature that you want to support for authenticated requests.
	S3SignatureVersion KeyName = "s3:signatureversion"

	// S3AuthType - optionally use this condition key to restrict incoming requests to use a specific authentication method.
	S3AuthType KeyName = "s3:authType"

	// Refer https://docs.aws.amazon.com/AmazonS3/latest/userguide/tagging-and-policies.html
	ExistingObjectTag    KeyName = "s3:ExistingObjectTag"
	RequestObjectTagKeys KeyName = "s3:RequestObjectTagKeys"
	RequestObjectTag     KeyName = "s3:RequestObjectTag"
)

// JWT claims supported substitutions.
// https://www.iana.org/assignments/jwt/jwt.xhtml#claims
const (
	// JWTSub - JWT subject claim substitution.
	JWTSub KeyName = "jwt:sub"

	// JWTIss issuer claim substitution.
	JWTIss KeyName = "jwt:iss"

	// JWTAud audience claim substitution.
	JWTAud KeyName = "jwt:aud"

	// JWTJti JWT unique identifier claim substitution.
	JWTJti KeyName = "jwt:jti"

	JWTUpn          KeyName = "jwt:upn"
	JWTName         KeyName = "jwt:name"
	JWTGroups       KeyName = "jwt:groups"
	JWTGivenName    KeyName = "jwt:given_name"
	JWTFamilyName   KeyName = "jwt:family_name"
	JWTMiddleName   KeyName = "jwt:middle_name"
	JWTNickName     KeyName = "jwt:nickname"
	JWTPrefUsername KeyName = "jwt:preferred_username"
	JWTProfile      KeyName = "jwt:profile"
	JWTPicture      KeyName = "jwt:picture"
	JWTWebsite      KeyName = "jwt:website"
	JWTEmail        KeyName = "jwt:email"
	JWTGender       KeyName = "jwt:gender"
	JWTBirthdate    KeyName = "jwt:birthdate"
	JWTPhoneNumber  KeyName = "jwt:phone_number"
	JWTAddress      KeyName = "jwt:address"
	JWTScope        KeyName = "jwt:scope"
	JWTClientID     KeyName = "jwt:client_id"
)

const (
	// LDAPUser - LDAP username, in MinIO this value is equal to your authenticating LDAP user.
	LDAPUser KeyName = "ldap:user"
)

// JWTKeys - Supported JWT keys, non-exhaustive list please
// expand as new claims are standardized.
var JWTKeys = []KeyName{
	JWTSub,
	JWTIss,
	JWTAud,
	JWTJti,
	JWTName,
	JWTUpn,
	JWTGroups,
	JWTGivenName,
	JWTFamilyName,
	JWTMiddleName,
	JWTNickName,
	JWTPrefUsername,
	JWTProfile,
	JWTPicture,
	JWTWebsite,
	JWTEmail,
	JWTGender,
	JWTBirthdate,
	JWTPhoneNumber,
	JWTAddress,
	JWTScope,
	JWTClientID,
}

// AllSupportedKeys - is list of all all supported keys.
var AllSupportedKeys = append([]KeyName{
	S3SignatureVersion,
	S3AuthType,
	S3XAmzCopySource,
	S3XAmzServerSideEncryption,
	S3XAmzServerSideEncryptionCustomerAlgorithm,
	S3XAmzMetadataDirective,
	S3XAmzStorageClass,
	S3XAmzContentSha256,
	S3LocationConstraint,
	S3Prefix,
	S3Delimiter,
	S3MaxKeys,
	S3VersionID,
	S3ObjectLockRemainingRetentionDays,
	S3ObjectLockMode,
	S3ObjectLockLegalHold,
	S3ObjectLockRetainUntilDate,
	AWSReferer,
	AWSSourceIP,
	AWSUserAgent,
	AWSSecureTransport,
	AWSCurrentTime,
	AWSEpochTime,
	AWSPrincipalType,
	AWSUserID,
	AWSUsername,
	LDAPUser,
	// Add new supported condition keys.
}, JWTKeys...)

// CommonKeys - is list of all common condition keys.
var CommonKeys = append([]KeyName{
	S3SignatureVersion,
	S3AuthType,
	S3XAmzContentSha256,
	S3LocationConstraint,
	AWSReferer,
	AWSSourceIP,
	AWSUserAgent,
	AWSSecureTransport,
	AWSCurrentTime,
	AWSEpochTime,
	AWSPrincipalType,
	AWSUserID,
	AWSUsername,
	LDAPUser,
}, JWTKeys...)

// AllSupportedAdminKeys - is list of all admin supported keys.
var AllSupportedAdminKeys = []KeyName{
	AWSReferer,
	AWSSourceIP,
	AWSUserAgent,
	AWSSecureTransport,
	AWSCurrentTime,
	AWSEpochTime,
	// Add new supported condition keys.
}
