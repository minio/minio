// Copyright (c) 2015-2023 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/auth"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/mimedb"
	ftp "goftp.io/server/v2"
)

var _ ftp.Driver = &ftpDriver{}

// ftpDriver implements ftpDriver to store files in minio
type ftpDriver struct {
	endpoint string
}

// NewFTPDriver implements ftp.Driver interface
func NewFTPDriver() ftp.Driver {
	return &ftpDriver{endpoint: fmt.Sprintf("127.0.0.1:%s", globalMinioPort)}
}

func buildMinioPath(p string) string {
	return strings.TrimPrefix(p, SlashSeparator)
}

func buildMinioDir(p string) string {
	v := buildMinioPath(p)
	if !strings.HasSuffix(v, SlashSeparator) {
		return v + SlashSeparator
	}
	return v
}

type minioFileInfo struct {
	p     string
	info  minio.ObjectInfo
	isDir bool
}

func (m *minioFileInfo) Name() string {
	return m.p
}

func (m *minioFileInfo) Size() int64 {
	return m.info.Size
}

func (m *minioFileInfo) Mode() os.FileMode {
	if m.isDir {
		return os.ModeDir
	}
	return os.ModePerm
}

var minFileDate = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC) // Workaround for Filezilla

func (m *minioFileInfo) ModTime() time.Time {
	if !m.info.LastModified.IsZero() {
		return m.info.LastModified
	}
	return minFileDate
}

func (m *minioFileInfo) IsDir() bool {
	return m.isDir
}

func (m *minioFileInfo) Sys() any {
	return nil
}

//msgp:ignore ftpMetrics
type ftpMetrics struct{}

var globalFtpMetrics ftpMetrics

func ftpTrace(s *ftp.Context, startTime time.Time, source, objPath string, err error, sz int64) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return madmin.TraceInfo{
		TraceType: madmin.TraceFTP,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  s.Cmd,
		Duration:  time.Since(startTime),
		Path:      objPath,
		Error:     errStr,
		Bytes:     sz,
		Custom: map[string]string{
			"user":   s.Sess.LoginUser(),
			"cmd":    s.Cmd,
			"param":  s.Param,
			"login":  fmt.Sprintf("%t", s.Sess.IsLogin()),
			"source": source,
		},
	}
}

func (m *ftpMetrics) log(s *ftp.Context, paths ...string) func(sz int64, err error) {
	startTime := time.Now()
	source := getSource(2)
	return func(sz int64, err error) {
		globalTrace.Publish(ftpTrace(s, startTime, source, strings.Join(paths, " "), err, sz))
	}
}

// Stat implements ftpDriver
func (driver *ftpDriver) Stat(ctx *ftp.Context, objPath string) (fi os.FileInfo, err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(0, err)

	if objPath == SlashSeparator {
		return &minioFileInfo{
			p:     SlashSeparator,
			isDir: true,
		}, nil
	}

	bucket, object := path2BucketObject(objPath)
	if bucket == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return nil, err
	}

	if object == "" {
		ok, err := clnt.BucketExists(context.Background(), bucket)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, os.ErrNotExist
		}
		return &minioFileInfo{
			p:     pathClean(bucket),
			info:  minio.ObjectInfo{Key: bucket},
			isDir: true,
		}, nil
	}

	objInfo, err := clnt.StatObject(context.Background(), bucket, object, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			// dummy return to satisfy LIST (stat -> list) behavior.
			return &minioFileInfo{
				p:     pathClean(object),
				info:  minio.ObjectInfo{Key: object},
				isDir: true,
			}, nil
		}
		return nil, err
	}

	isDir := strings.HasSuffix(objInfo.Key, SlashSeparator)
	return &minioFileInfo{
		p:     pathClean(object),
		info:  objInfo,
		isDir: isDir,
	}, nil
}

// ListDir implements ftpDriver
func (driver *ftpDriver) ListDir(ctx *ftp.Context, objPath string, callback func(os.FileInfo) error) (err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(0, err)

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucket, prefix := path2BucketObject(objPath)
	if bucket == "" {
		buckets, err := clnt.ListBuckets(cctx)
		if err != nil {
			return err
		}

		for _, bucket := range buckets {
			info := minioFileInfo{
				p:     pathClean(bucket.Name),
				info:  minio.ObjectInfo{Key: retainSlash(bucket.Name), LastModified: bucket.CreationDate},
				isDir: true,
			}
			if err := callback(&info); err != nil {
				return err
			}
		}

		return nil
	}

	prefix = retainSlash(prefix)

	for object := range clnt.ListObjects(cctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}) {
		if object.Err != nil {
			return object.Err
		}

		if object.Key == prefix {
			continue
		}

		isDir := strings.HasSuffix(object.Key, SlashSeparator)
		info := minioFileInfo{
			p:     pathClean(strings.TrimPrefix(object.Key, prefix)),
			info:  object,
			isDir: isDir,
		}

		if err := callback(&info); err != nil {
			return err
		}
	}

	return nil
}

func (driver *ftpDriver) CheckPasswd(c *ftp.Context, username, password string) (ok bool, err error) {
	stopFn := globalFtpMetrics.log(c, username)
	defer stopFn(0, err)

	if globalIAMSys.LDAPConfig.Enabled() {
		sa, _, err := globalIAMSys.getServiceAccount(context.Background(), username)
		if err != nil && !errors.Is(err, errNoSuchServiceAccount) {
			return false, err
		}
		if errors.Is(err, errNoSuchServiceAccount) {
			lookupRes, groupDistNames, err := globalIAMSys.LDAPConfig.Bind(username, password)
			if err != nil {
				return false, err
			}
			ldapPolicies, _ := globalIAMSys.PolicyDBGet(lookupRes.NormDN, groupDistNames...)
			return len(ldapPolicies) > 0, nil
		}
		return subtle.ConstantTimeCompare([]byte(sa.Credentials.SecretKey), []byte(password)) == 1, nil
	}

	ui, ok := globalIAMSys.GetUser(context.Background(), username)
	if !ok {
		return false, nil
	}
	return subtle.ConstantTimeCompare([]byte(ui.Credentials.SecretKey), []byte(password)) == 1, nil
}

func (driver *ftpDriver) getMinIOClient(ctx *ftp.Context) (*minio.Client, error) {
	tr := http.RoundTripper(globalRemoteFTPClientTransport)
	if host, _, err := net.SplitHostPort(ctx.Sess.RemoteAddr().String()); err == nil {
		tr = forwardForTransport{tr: tr, fwd: host}
	}
	ui, ok := globalIAMSys.GetUser(context.Background(), ctx.Sess.LoginUser())
	if !ok && !globalIAMSys.LDAPConfig.Enabled() {
		return nil, errNoSuchUser
	}
	if !ok && globalIAMSys.LDAPConfig.Enabled() {
		sa, _, err := globalIAMSys.getServiceAccount(context.Background(), ctx.Sess.LoginUser())
		if err != nil && !errors.Is(err, errNoSuchServiceAccount) {
			return nil, err
		}

		var mcreds *credentials.Credentials
		if errors.Is(err, errNoSuchServiceAccount) {
			lookupResult, targetGroups, err := globalIAMSys.LDAPConfig.LookupUserDN(ctx.Sess.LoginUser())
			if err != nil {
				return nil, err
			}
			ldapPolicies, _ := globalIAMSys.PolicyDBGet(lookupResult.NormDN, targetGroups...)
			if len(ldapPolicies) == 0 {
				return nil, errAuthentication
			}
			expiryDur, err := globalIAMSys.LDAPConfig.GetExpiryDuration("")
			if err != nil {
				return nil, err
			}
			claims := make(map[string]any)
			claims[expClaim] = UTCNow().Add(expiryDur).Unix()

			claims[ldapUser] = lookupResult.NormDN
			claims[ldapActualUser] = lookupResult.ActualDN
			claims[ldapUserN] = ctx.Sess.LoginUser()

			// Add LDAP attributes that were looked up into the claims.
			for attribKey, attribValue := range lookupResult.Attributes {
				claims[ldapAttribPrefix+attribKey] = attribValue
			}

			cred, err := auth.GetNewCredentialsWithMetadata(claims, globalActiveCred.SecretKey)
			if err != nil {
				return nil, err
			}

			// Set the parent of the temporary access key, this is useful
			// in obtaining service accounts by this cred.
			cred.ParentUser = lookupResult.NormDN

			// Set this value to LDAP groups, LDAP user can be part
			// of large number of groups
			cred.Groups = targetGroups

			// Set the newly generated credentials, policyName is empty on purpose
			// LDAP policies are applied automatically using their ldapUser, ldapGroups
			// mapping.
			updatedAt, err := globalIAMSys.SetTempUser(context.Background(), cred.AccessKey, cred, "")
			if err != nil {
				return nil, err
			}

			// Call hook for site replication.
			replLogIf(context.Background(), globalSiteReplicationSys.IAMChangeHook(context.Background(), madmin.SRIAMItem{
				Type: madmin.SRIAMItemSTSAcc,
				STSCredential: &madmin.SRSTSCredential{
					AccessKey:    cred.AccessKey,
					SecretKey:    cred.SecretKey,
					SessionToken: cred.SessionToken,
					ParentUser:   cred.ParentUser,
				},
				UpdatedAt: updatedAt,
			}))

			mcreds = credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken)
		} else {
			mcreds = credentials.NewStaticV4(sa.Credentials.AccessKey, sa.Credentials.SecretKey, "")
		}

		return minio.New(driver.endpoint, &minio.Options{
			Creds:           mcreds,
			Secure:          globalIsTLS,
			Transport:       tr,
			TrailingHeaders: true,
		})
	}

	// ok == true - at this point

	if ui.Credentials.IsTemp() {
		// Temporary credentials are not allowed.
		return nil, errAuthentication
	}

	return minio.New(driver.endpoint, &minio.Options{
		Creds:           credentials.NewStaticV4(ui.Credentials.AccessKey, ui.Credentials.SecretKey, ""),
		Secure:          globalIsTLS,
		Transport:       tr,
		TrailingHeaders: true,
	})
}

// DeleteDir implements ftpDriver
func (driver *ftpDriver) DeleteDir(ctx *ftp.Context, objPath string) (err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(0, err)

	bucket, prefix := path2BucketObject(objPath)
	if bucket == "" {
		return errors.New("deleting all buckets not allowed")
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if prefix == "" {
		// if all objects are not deleted yet this call may fail.
		return clnt.RemoveBucket(cctx, bucket)
	}

	objectsCh := make(chan minio.ObjectInfo)

	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer xioutil.SafeClose(objectsCh)
		opts := minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}
		for object := range clnt.ListObjects(cctx, bucket, opts) {
			if object.Err != nil {
				return
			}
			objectsCh <- object
		}
	}()

	// Call RemoveObjects API
	for err := range clnt.RemoveObjects(context.Background(), bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			return err.Err
		}
	}

	return nil
}

// DeleteFile implements ftpDriver
func (driver *ftpDriver) DeleteFile(ctx *ftp.Context, objPath string) (err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(0, err)

	bucket, object := path2BucketObject(objPath)
	if bucket == "" {
		return errors.New("bucket name cannot be empty")
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return err
	}

	return clnt.RemoveObject(context.Background(), bucket, object, minio.RemoveObjectOptions{})
}

// Rename implements ftpDriver
func (driver *ftpDriver) Rename(ctx *ftp.Context, fromObjPath string, toObjPath string) (err error) {
	stopFn := globalFtpMetrics.log(ctx, fromObjPath, toObjPath)
	defer stopFn(0, err)

	return NotImplemented{}
}

// MakeDir implements ftpDriver
func (driver *ftpDriver) MakeDir(ctx *ftp.Context, objPath string) (err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(0, err)

	bucket, prefix := path2BucketObject(objPath)
	if bucket == "" {
		return errors.New("bucket name cannot be empty")
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return err
	}

	if prefix == "" {
		return clnt.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: globalSite.Region()})
	}

	dirPath := buildMinioDir(prefix)

	_, err = clnt.PutObject(context.Background(), bucket, dirPath, bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{
		DisableContentSha256: true,
	})
	return err
}

// GetFile implements ftpDriver
func (driver *ftpDriver) GetFile(ctx *ftp.Context, objPath string, offset int64) (n int64, rc io.ReadCloser, err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(n, err)

	bucket, object := path2BucketObject(objPath)
	if bucket == "" {
		return 0, nil, errors.New("bucket name cannot be empty")
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return 0, nil, err
	}

	opts := minio.GetObjectOptions{}
	obj, err := clnt.GetObject(context.Background(), bucket, object, opts)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil && obj != nil {
			obj.Close()
		}
	}()

	_, err = obj.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, nil, err
	}

	info, err := obj.Stat()
	if err != nil {
		return 0, nil, err
	}
	n = info.Size - offset
	return n, obj, nil
}

// PutFile implements ftpDriver
func (driver *ftpDriver) PutFile(ctx *ftp.Context, objPath string, data io.Reader, offset int64) (n int64, err error) {
	stopFn := globalFtpMetrics.log(ctx, objPath)
	defer stopFn(n, err)

	bucket, object := path2BucketObject(objPath)
	if bucket == "" {
		return 0, errors.New("bucket name cannot be empty")
	}

	if offset != -1 {
		// FTP - APPEND not implemented
		return 0, NotImplemented{}
	}

	clnt, err := driver.getMinIOClient(ctx)
	if err != nil {
		return 0, err
	}

	info, err := clnt.PutObject(context.Background(), bucket, object, data, -1, minio.PutObjectOptions{
		ContentType:          mimedb.TypeByExtension(path.Ext(object)),
		DisableContentSha256: true,
		Checksum:             minio.ChecksumFullObjectCRC32C,
	})
	n = info.Size
	return n, err
}
