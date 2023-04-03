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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/madmin-go/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpDriver struct {
	permissions *ssh.Permissions
	endpoint    string
}

//msgp:ignore sftpMetrics
type sftpMetrics struct{}

var globalSftpMetrics sftpMetrics

func sftpTrace(s *sftp.Request, startTime time.Time, source string, user string, err error) madmin.TraceInfo {
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	return madmin.TraceInfo{
		TraceType: madmin.TraceFTP,
		Time:      startTime,
		NodeName:  globalLocalNodeName,
		FuncName:  fmt.Sprintf("sftp USER=%s COMMAND=%s PARAM=%s, Source=%s", user, s.Method, s.Filepath, source),
		Duration:  time.Since(startTime),
		Path:      s.Filepath,
		Error:     errStr,
	}
}

func (m *sftpMetrics) log(s *sftp.Request, user string) func(err error) {
	startTime := time.Now()
	source := getSource(2)
	return func(err error) {
		globalTrace.Publish(sftpTrace(s, startTime, source, user, err))
	}
}

// NewSFTPDriver initializes sftp.Handlers implementation of following interfaces
//
// - sftp.Fileread
// - sftp.Filewrite
// - sftp.Filelist
// - sftp.Filecmd
func NewSFTPDriver(perms *ssh.Permissions) sftp.Handlers {
	handler := &sftpDriver{endpoint: fmt.Sprintf("127.0.0.1:%s", globalMinioPort), permissions: perms}
	return sftp.Handlers{
		FileGet:  handler,
		FilePut:  handler,
		FileCmd:  handler,
		FileList: handler,
	}
}

func (f *sftpDriver) getMinIOClient() (*minio.Client, error) {
	ui, ok := globalIAMSys.GetUser(context.Background(), f.AccessKey())
	if !ok && !globalIAMSys.LDAPConfig.Enabled() {
		return nil, errNoSuchUser
	}
	if !ok && globalIAMSys.LDAPConfig.Enabled() {
		targetUser, targetGroups, err := globalIAMSys.LDAPConfig.LookupUserDN(f.AccessKey())
		if err != nil {
			return nil, err
		}
		ldapPolicies, _ := globalIAMSys.PolicyDBGet(targetUser, false, targetGroups...)
		if len(ldapPolicies) == 0 {
			return nil, errAuthentication
		}
		expiryDur, err := globalIAMSys.LDAPConfig.GetExpiryDuration("")
		if err != nil {
			return nil, err
		}
		claims := make(map[string]interface{})
		claims[expClaim] = UTCNow().Add(expiryDur).Unix()
		claims[ldapUser] = targetUser
		claims[ldapUserN] = f.AccessKey()

		cred, err := auth.GetNewCredentialsWithMetadata(claims, globalActiveCred.SecretKey)
		if err != nil {
			return nil, err
		}

		// Set the parent of the temporary access key, this is useful
		// in obtaining service accounts by this cred.
		cred.ParentUser = targetUser

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
		logger.LogIf(context.Background(), globalSiteReplicationSys.IAMChangeHook(context.Background(), madmin.SRIAMItem{
			Type: madmin.SRIAMItemSTSAcc,
			STSCredential: &madmin.SRSTSCredential{
				AccessKey:    cred.AccessKey,
				SecretKey:    cred.SecretKey,
				SessionToken: cred.SessionToken,
				ParentUser:   cred.ParentUser,
			},
			UpdatedAt: updatedAt,
		}))

		return minio.New(f.endpoint, &minio.Options{
			Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, cred.SessionToken),
			Secure:    globalIsTLS,
			Transport: globalRemoteTargetTransport,
		})
	}

	// ok == true - at this point

	if ui.Credentials.IsTemp() {
		// Temporary credentials are not allowed.
		return nil, errAuthentication
	}

	return minio.New(f.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(ui.Credentials.AccessKey, ui.Credentials.SecretKey, ""),
		Secure:    globalIsTLS,
		Transport: globalRemoteTargetTransport,
	})
}

func (f *sftpDriver) AccessKey() string {
	return f.permissions.CriticalOptions["accessKey"]
}

func (f *sftpDriver) Fileread(r *sftp.Request) (ra io.ReaderAt, err error) {
	stopFn := globalSftpMetrics.log(r, f.AccessKey())
	defer stopFn(err)

	flags := r.Pflags()
	if !flags.Read {
		// sanity check
		return nil, os.ErrInvalid
	}

	bucket, object := path2BucketObject(r.Filepath)
	if bucket == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	clnt, err := f.getMinIOClient()
	if err != nil {
		return nil, err
	}

	obj, err := clnt.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	_, err = obj.Stat()
	if err != nil {
		return nil, err
	}

	return obj, nil
}

type writerAt struct {
	w  *io.PipeWriter
	wg *sync.WaitGroup
}

func (w *writerAt) Close() error {
	err := w.w.Close()
	w.wg.Wait()
	return err
}

func (w *writerAt) WriteAt(b []byte, offset int64) (n int, err error) {
	return w.w.Write(b)
}

func (f *sftpDriver) Filewrite(r *sftp.Request) (w io.WriterAt, err error) {
	stopFn := globalSftpMetrics.log(r, f.AccessKey())
	defer stopFn(err)

	flags := r.Pflags()
	if !flags.Write {
		// sanity check
		return nil, os.ErrInvalid
	}

	bucket, object := path2BucketObject(r.Filepath)
	if bucket == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	clnt, err := f.getMinIOClient()
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	wa := &writerAt{w: pw, wg: &sync.WaitGroup{}}
	wa.wg.Add(1)
	go func() {
		_, err := clnt.PutObject(r.Context(), bucket, object, pr, -1, minio.PutObjectOptions{SendContentMd5: true})
		pr.CloseWithError(err)
		wa.wg.Done()
	}()
	return wa, nil
}

func (f *sftpDriver) Filecmd(r *sftp.Request) (err error) {
	stopFn := globalSftpMetrics.log(r, f.AccessKey())
	defer stopFn(err)

	clnt, err := f.getMinIOClient()
	if err != nil {
		return err
	}

	switch r.Method {
	case "Setstat", "Rename", "Link", "Symlink":
		return NotImplemented{}

	case "Rmdir":
		bucket, prefix := path2BucketObject(r.Filepath)
		if bucket == "" {
			return errors.New("deleting all buckets not allowed")
		}

		cctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		objectsCh := make(chan minio.ObjectInfo)

		// Send object names that are needed to be removed to objectsCh
		go func() {
			defer close(objectsCh)
			opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
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

	case "Remove":
		bucket, object := path2BucketObject(r.Filepath)
		if bucket == "" {
			return errors.New("bucket name cannot be empty")
		}

		return clnt.RemoveObject(context.Background(), bucket, object, minio.RemoveObjectOptions{})

	case "Mkdir":
		bucket, prefix := path2BucketObject(r.Filepath)
		if bucket == "" {
			return errors.New("bucket name cannot be empty")
		}

		dirPath := buildMinioDir(prefix)

		_, err = clnt.PutObject(context.Background(), bucket, dirPath, bytes.NewReader([]byte("")), 0,
			// Always send Content-MD5 to succeed with bucket with
			// locking enabled. There is no performance hit since
			// this is always an empty object
			minio.PutObjectOptions{SendContentMd5: true},
		)
		return err
	}

	return NotImplemented{}
}

type listerAt []os.FileInfo

// Modeled after strings.Reader's ReadAt() implementation
func (f listerAt) ListAt(ls []os.FileInfo, offset int64) (int, error) {
	var n int
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}
	n = copy(ls, f[offset:])
	if n < len(ls) {
		return n, io.EOF
	}
	return n, nil
}

func (f *sftpDriver) Filelist(r *sftp.Request) (la sftp.ListerAt, err error) {
	stopFn := globalSftpMetrics.log(r, f.AccessKey())
	defer stopFn(err)

	clnt, err := f.getMinIOClient()
	if err != nil {
		return nil, err
	}

	switch r.Method {
	case "List":
		var files []os.FileInfo

		bucket, prefix := path2BucketObject(r.Filepath)
		if bucket == "" {
			buckets, err := clnt.ListBuckets(r.Context())
			if err != nil {
				return nil, err
			}

			for _, bucket := range buckets {
				files = append(files, &minioFileInfo{
					p:     bucket.Name,
					info:  minio.ObjectInfo{Key: bucket.Name, LastModified: bucket.CreationDate},
					isDir: true,
				})
			}

			return listerAt(files), nil
		}

		prefix = retainSlash(prefix)

		for object := range clnt.ListObjects(r.Context(), bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false,
		}) {
			if object.Err != nil {
				return nil, object.Err
			}

			if object.Key == prefix {
				continue
			}

			isDir := strings.HasSuffix(object.Key, SlashSeparator)
			files = append(files, &minioFileInfo{
				p:     pathClean(strings.TrimPrefix(object.Key, prefix)),
				info:  object,
				isDir: isDir,
			})
		}

		return listerAt(files), nil

	case "Stat":
		if r.Filepath == SlashSeparator {
			return listerAt{&minioFileInfo{
				p:     r.Filepath,
				isDir: true,
			}}, nil
		}

		bucket, object := path2BucketObject(r.Filepath)
		if bucket == "" {
			return nil, errors.New("bucket name cannot be empty")
		}

		if object == "" {
			ok, err := clnt.BucketExists(context.Background(), bucket)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, os.ErrNotExist
			}
			return listerAt{&minioFileInfo{
				p:     pathClean(bucket),
				info:  minio.ObjectInfo{Key: bucket},
				isDir: true,
			}}, nil
		}

		objInfo, err := clnt.StatObject(context.Background(), bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if minio.ToErrorResponse(err).Code == "NoSuchKey" {
				// dummy return to satisfy LIST (stat -> list) behavior.
				return listerAt{&minioFileInfo{
					p:     pathClean(object),
					info:  minio.ObjectInfo{Key: object},
					isDir: true,
				}}, nil
			}
			return nil, err
		}

		isDir := strings.HasSuffix(objInfo.Key, SlashSeparator)
		return listerAt{&minioFileInfo{
			p:     pathClean(object),
			info:  objInfo,
			isDir: isDir,
		}}, nil
	}

	return nil, NotImplemented{}
}
