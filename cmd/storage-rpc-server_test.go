/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"net/url"
	"testing"
	"time"

	"github.com/minio/minio/pkg/disk"
)

const invalidToken = "invalidToken"

type testStorageRPCServer struct {
	configDir string
	token     string
	diskDirs  []string
	stServer  *storageServer
	endpoints []*url.URL
}

func createTestStorageServer(t *testing.T) *testStorageRPCServer {
	testPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}

	jwt, err := newJWT(defaultInterNodeJWTExpiry, serverConfig.GetCredential())
	if err != nil {
		t.Fatalf("unable to get new JWT, %s", err)
	}

	err = jwt.Authenticate(serverConfig.GetCredential().AccessKeyID, serverConfig.GetCredential().SecretAccessKey)
	if err != nil {
		t.Fatalf("unable for JWT to authenticate, %s", err)
	}

	token, err := jwt.GenerateToken(serverConfig.GetCredential().AccessKeyID)
	if err != nil {
		t.Fatalf("unable for JWT to generate token, %s", err)
	}

	fsDirs, err := getRandomDisks(1)
	if err != nil {
		t.Fatalf("unable to create FS backend, %s", err)
	}

	endpoints, err := parseStorageEndpoints(fsDirs)
	if err != nil {
		t.Fatalf("unable to parse storage endpoints, %s", err)
	}

	storageDisks, err := initStorageDisks(endpoints)
	if err != nil {
		t.Fatalf("unable to initialize storage disks, %s", err)
	}
	stServer := &storageServer{
		storage:   storageDisks[0],
		path:      "/disk1",
		timestamp: time.Now().UTC(),
	}
	return &testStorageRPCServer{
		token:     token,
		configDir: testPath,
		diskDirs:  fsDirs,
		endpoints: endpoints,
		stServer:  stServer,
	}
}

func errorIfInvalidToken(t *testing.T, err error) {
	realErr := errorCause(err)
	if realErr != errInvalidToken {
		t.Errorf("Expected to fail with %s but failed with %s", errInvalidToken, realErr)
	}
}

func TestStorageRPCInvalidToken(t *testing.T) {
	st := createTestStorageServer(t)
	defer removeRoots(st.diskDirs)
	defer removeAll(st.configDir)

	storageRPC := st.stServer
	timestamp := time.Now().UTC()
	ga := GenericArgs{
		Token:     st.token,
		Timestamp: timestamp,
	}
	// Construct an invalid token.
	badga := ga
	badga.Token = "invalidToken"

	// Following test cases are meant to exercise the invalid
	// token code path of the storage RPC methods.

	var err error
	gva := GenericVolArgs{
		GenericArgs: badga,
		Vol:         "myvol",
	}
	// 1. DiskInfoHandler
	diskInfoReply := &disk.Info{}
	err = storageRPC.DiskInfoHandler(&badga, diskInfoReply)
	errorIfInvalidToken(t, err)

	// 2. MakeVolHandler
	makeVolArgs := &gva
	makeVolReply := &GenericReply{}
	err = storageRPC.MakeVolHandler(makeVolArgs, makeVolReply)
	errorIfInvalidToken(t, err)

	// 3. ListVolsHandler
	listVolReply := &ListVolsReply{}
	err = storageRPC.ListVolsHandler(&badga, listVolReply)
	errorIfInvalidToken(t, err)

	// 4. StatVolHandler
	statVolReply := &VolInfo{}
	statVolArgs := &gva
	err = storageRPC.StatVolHandler(statVolArgs, statVolReply)
	errorIfInvalidToken(t, err)

	// 5. DeleteVolHandler
	deleteVolArgs := &gva
	deleteVolReply := &GenericReply{}
	err = storageRPC.DeleteVolHandler(deleteVolArgs, deleteVolReply)
	errorIfInvalidToken(t, err)

	// 6. StatFileHandler
	statFileArgs := &StatFileArgs{
		GenericArgs: badga,
	}
	statReply := &FileInfo{}
	err = storageRPC.StatFileHandler(statFileArgs, statReply)
	errorIfInvalidToken(t, err)

	// 7. ListDirHandler
	listDirArgs := &ListDirArgs{
		GenericArgs: badga,
	}
	listDirReply := &[]string{}
	err = storageRPC.ListDirHandler(listDirArgs, listDirReply)
	errorIfInvalidToken(t, err)

	// 8. ReadAllHandler
	readFileArgs := &ReadFileArgs{
		GenericArgs: badga,
	}
	readFileReply := &[]byte{}
	err = storageRPC.ReadAllHandler(readFileArgs, readFileReply)
	errorIfInvalidToken(t, err)

	// 9. ReadFileHandler
	err = storageRPC.ReadFileHandler(readFileArgs, readFileReply)
	errorIfInvalidToken(t, err)

	// 10. PrepareFileHandler
	prepFileArgs := &PrepareFileArgs{
		GenericArgs: badga,
	}
	prepFileReply := &GenericReply{}
	err = storageRPC.PrepareFileHandler(prepFileArgs, prepFileReply)
	errorIfInvalidToken(t, err)

	// 11. AppendFileHandler
	appendArgs := &AppendFileArgs{
		GenericArgs: badga,
	}
	appendReply := &GenericReply{}
	err = storageRPC.AppendFileHandler(appendArgs, appendReply)
	errorIfInvalidToken(t, err)

	// 12. DeleteFileHandler
	delFileArgs := &DeleteFileArgs{
		GenericArgs: badga,
	}
	delFileRely := &GenericReply{}
	err = storageRPC.DeleteFileHandler(delFileArgs, delFileRely)
	errorIfInvalidToken(t, err)

	// 13. RenameFileHandler
	renameArgs := &RenameFileArgs{
		GenericArgs: badga,
	}
	renameReply := &GenericReply{}
	err = storageRPC.RenameFileHandler(renameArgs, renameReply)
	errorIfInvalidToken(t, err)
}
