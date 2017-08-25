// +build windows

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package os

import (
	"io/ioutil"
	os1 "os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

var (
	modnetapi32 = syscall.NewLazyDLL("netapi32.dll")

	procNetShareAdd = modnetapi32.NewProc("NetShareAdd")
	procNetShareDel = modnetapi32.NewProc("NetShareDel")
)

func netShareAdd(serverName *uint16, level uint32, buf *byte, parmErr *uint16) (neterr error) {
	r0, _, _ := syscall.Syscall6(procNetShareAdd.Addr(), 4, uintptr(unsafe.Pointer(serverName)), uintptr(level), uintptr(unsafe.Pointer(buf)), uintptr(unsafe.Pointer(parmErr)), 0, 0)
	if r0 != 0 {
		neterr = syscall.Errno(r0)
	}
	return
}

func netShareDel(serverName *uint16, netName *uint16, reserved uint32) (neterr error) {
	r0, _, _ := syscall.Syscall(procNetShareDel.Addr(), 3, uintptr(unsafe.Pointer(serverName)), uintptr(unsafe.Pointer(netName)), uintptr(reserved))
	if r0 != 0 {
		neterr = syscall.Errno(r0)
	}
	return
}

type shareInfo2 struct {
	Netname     *uint16
	Type        uint32
	Remark      *uint16
	Permissions uint32
	MaxUses     uint32
	CurrentUses uint32
	Path        *uint16
	Passwd      *uint16
}

func sameFile(fi1, fi2 os1.FileInfo) bool {
	fii1, ok1 := fi1.(*fileStat)
	fii2, ok2 := fi2.(*fileStat)
	if !ok1 || !ok2 {
		return false
	}
	return fii1.vol == fii2.vol && fii1.idxhi == fii2.idxhi && fii1.idxlo == fii2.idxlo
}

func TestNetworkSymbolicLink(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestNetworkSymbolicLink")
	if err != nil {
		t.Fatal(err)
	}
	defer os1.RemoveAll(dir)

	oldwd, err := os1.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	err = os1.Chdir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer os1.Chdir(oldwd)

	shareName := "GoSymbolicLinkTestShare" // hope no conflicts.
	sharePath := filepath.Join(dir, shareName)
	testDir := "TestDir"

	err = os1.MkdirAll(filepath.Join(sharePath, testDir), 0777)
	if err != nil {
		t.Fatal(err)
	}

	wShareName, err := syscall.UTF16PtrFromString(shareName)
	if err != nil {
		t.Fatal(err)
	}
	wSharePath, err := syscall.UTF16PtrFromString(sharePath)
	if err != nil {
		t.Fatal(err)
	}

	p := shareInfo2{
		Netname:     wShareName,
		Type:        0x00, // STYPE_DISKTREE
		Remark:      nil,
		Permissions: 0,
		MaxUses:     1,
		CurrentUses: 0,
		Path:        wSharePath,
		Passwd:      nil,
	}

	err = netShareAdd(nil, 2, (*byte)(unsafe.Pointer(&p)), nil)
	if err != nil {
		if err == syscall.ERROR_ACCESS_DENIED {
			t.Skip("you don't have enough privileges to add network share")
		}
		t.Fatal(err)
	}
	defer func() {
		err := netShareDel(nil, wShareName, 0)
		if err != nil {
			t.Fatal(err)
		}
	}()

	UNCPath := `\\localhost\` + shareName + `\`

	fi1, err := Stat(sharePath)
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := Stat(UNCPath)
	if err != nil {
		t.Fatal(err)
	}
	if !sameFile(fi1, fi2) {
		t.Fatalf("%q and %q should be the same directory, but not", sharePath, UNCPath)
	}

	target := filepath.Join(UNCPath, testDir)
	link := "link"

	err = os1.Symlink(target, link)
	if err != nil {
		t.Fatal(err)
	}
	defer os1.Remove(link)

	got, err := os1.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}

	if got != target {
		t.Errorf(`os1.Readlink("%s"): got %v, want %v`, link, got, target)
	}
}

func TestShareNotExistError(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test that uses network; skipping")
	}
	_, err := Stat(`\\no_such_server\no_such_share\no_such_file`)
	if err == nil {
		t.Fatal("Stat succeeded, but expected to fail")
	}
	if !os1.IsNotExist(err) {
		t.Fatalf("os1.Stat failed with %q, but os1.IsNotExist(err) is false", err)
	}
}

func TestStatPagefile(t *testing.T) {
	_, err := Stat(`c:\pagefile.sys`)
	if err == nil {
		return
	}
	if os1.IsNotExist(err) {
		t.Skip(`skipping because c:\pagefile.sys is not found`)
	}
	t.Fatal(err)
}

func TestStatSymlinkLoop(t *testing.T) {
	err := os1.Symlink("x", "y")
	if err != nil {
		t.Fatal(err)
	}
	defer os1.Remove("y")

	err = os1.Symlink("y", "x")
	if err != nil {
		t.Fatal(err)
	}
	defer os1.Remove("x")

	_, err = Stat("x")
	if err != nil {
		if _, ok := err.(*os1.PathError); !ok {
			t.Errorf("expected *PathError, got %T: %v\n", err, err)
		}
	}
}

func TestFixLongPath(t *testing.T) {
	// 248 is long enough to trigger the longer-than-248 checks in
	// fixLongPath, but short enough not to make a path component
	// longer than 255, which is illegal on Windows. (which
	// doesn't really matter anyway, since this is purely a string
	// function we're testing, and it's not actually being used to
	// do a system call)
	veryLong := "l" + strings.Repeat("o", 248) + "ng"
	for _, test := range []struct{ in, want string }{
		// Short; unchanged:
		{`C:\short.txt`, `C:\short.txt`},
		{`C:\`, `C:\`},
		{`C:`, `C:`},
		// The "long" substring is replaced by a looooooong
		// string which triggers the rewriting. Except in the
		// cases below where it doesn't.
		{`C:\long\foo.txt`, `\\?\C:\long\foo.txt`},
		{`C:/long/foo.txt`, `\\?\C:\long\foo.txt`},
		{`C:\long\foo\\bar\.\baz\\`, `\\?\C:\long\foo\bar\baz`},
		{`\\unc\path`, `\\unc\path`},
		{`long.txt`, `long.txt`},
		{`C:long.txt`, `C:long.txt`},
		{`c:\long\..\bar\baz`, `c:\long\..\bar\baz`},
		{`\\?\c:\long\foo.txt`, `\\?\c:\long\foo.txt`},
		{`\\?\c:\long/foo.txt`, `\\?\c:\long/foo.txt`},
	} {
		in := strings.Replace(test.in, "long", veryLong, -1)
		want := strings.Replace(test.want, "long", veryLong, -1)
		if got := fixLongPath(in); got != want {
			got = strings.Replace(got, veryLong, "long", -1)
			t.Errorf("fixLongPath(%q) = %q; want %q", test.in, got, test.want)
		}
	}
}
