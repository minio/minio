// mgo - MongoDB driver for Go
//
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package mgo_test

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var fast = flag.Bool("fast", false, "Skip slow tests")

type M bson.M

type cLogger C

func (c *cLogger) Output(calldepth int, s string) error {
	ns := time.Now().UnixNano()
	t := float64(ns%100e9) / 1e9
	((*C)(c)).Logf("[LOG] %.05f %s", t, s)
	return nil
}

func TestAll(t *testing.T) {
	TestingT(t)
}

type S struct {
	session *mgo.Session
	stopped bool
	build   mgo.BuildInfo
	frozen  []string
}

func (s *S) versionAtLeast(v ...int) (result bool) {
	for i := range v {
		if i == len(s.build.VersionArray) {
			return false
		}
		if s.build.VersionArray[i] != v[i] {
			return s.build.VersionArray[i] >= v[i]
		}
	}
	return true
}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
	mgo.SetDebug(true)
	mgo.SetStats(true)
	s.StartAll()

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	s.build, err = session.BuildInfo()
	c.Check(err, IsNil)
	session.Close()
}

func (s *S) SetUpTest(c *C) {
	err := run("mongo --nodb testdb/dropall.js")
	if err != nil {
		panic(err.Error())
	}
	mgo.SetLogger((*cLogger)(c))
	mgo.ResetStats()
}

func (s *S) TearDownTest(c *C) {
	if s.stopped {
		s.Stop(":40201")
		s.Stop(":40202")
		s.Stop(":40203")
		s.StartAll()
	}
	for _, host := range s.frozen {
		if host != "" {
			s.Thaw(host)
		}
	}
	var stats mgo.Stats
	for i := 0; ; i++ {
		stats = mgo.GetStats()
		if stats.SocketsInUse == 0 && stats.SocketsAlive == 0 {
			break
		}
		if i == 20 {
			c.Fatal("Test left sockets in a dirty state")
		}
		c.Logf("Waiting for sockets to die: %d in use, %d alive", stats.SocketsInUse, stats.SocketsAlive)
		time.Sleep(500 * time.Millisecond)
	}
	for i := 0; ; i++ {
		stats = mgo.GetStats()
		if stats.Clusters == 0 {
			break
		}
		if i == 60 {
			c.Fatal("Test left clusters alive")
		}
		c.Logf("Waiting for clusters to die: %d alive", stats.Clusters)
		time.Sleep(1 * time.Second)
	}
}

func (s *S) Stop(host string) {
	// Give a moment for slaves to sync and avoid getting rollback issues.
	panicOnWindows()
	time.Sleep(2 * time.Second)
	err := run("cd _testdb && supervisorctl stop " + supvName(host))
	if err != nil {
		panic(err)
	}
	s.stopped = true
}

func (s *S) pid(host string) int {
	output, err := exec.Command("lsof", "-iTCP:"+hostPort(host), "-sTCP:LISTEN", "-Fp").CombinedOutput()
	if err != nil {
		panic(err)
	}
	pidstr := string(output[1 : len(output)-1])
	pid, err := strconv.Atoi(pidstr)
	if err != nil {
		panic("cannot convert pid to int: " + pidstr)
	}
	return pid
}

func (s *S) Freeze(host string) {
	err := stop(s.pid(host))
	if err != nil {
		panic(err)
	}
	s.frozen = append(s.frozen, host)
}

func (s *S) Thaw(host string) {
	err := cont(s.pid(host))
	if err != nil {
		panic(err)
	}
	for i, frozen := range s.frozen {
		if frozen == host {
			s.frozen[i] = ""
		}
	}
}

func (s *S) StartAll() {
	if s.stopped {
		// Restart any stopped nodes.
		run("cd _testdb && supervisorctl start all")
		err := run("cd testdb && mongo --nodb wait.js")
		if err != nil {
			panic(err)
		}
		s.stopped = false
	}
}

func run(command string) error {
	var output []byte
	var err error
	if runtime.GOOS == "windows" {
		output, err = exec.Command("cmd", "/C", command).CombinedOutput()
	} else {
		output, err = exec.Command("/bin/sh", "-c", command).CombinedOutput()
	}

	if err != nil {
		msg := fmt.Sprintf("Failed to execute: %s: %s\n%s", command, err.Error(), string(output))
		return errors.New(msg)
	}
	return nil
}

var supvNames = map[string]string{
	"40001": "db1",
	"40002": "db2",
	"40011": "rs1a",
	"40012": "rs1b",
	"40013": "rs1c",
	"40021": "rs2a",
	"40022": "rs2b",
	"40023": "rs2c",
	"40031": "rs3a",
	"40032": "rs3b",
	"40033": "rs3c",
	"40041": "rs4a",
	"40101": "cfg1",
	"40102": "cfg2",
	"40103": "cfg3",
	"40201": "s1",
	"40202": "s2",
	"40203": "s3",
}

// supvName returns the supervisord name for the given host address.
func supvName(host string) string {
	host, port, err := net.SplitHostPort(host)
	if err != nil {
		panic(err)
	}
	name, ok := supvNames[port]
	if !ok {
		panic("Unknown host: " + host)
	}
	return name
}

func hostPort(host string) string {
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		panic(err)
	}
	return port
}

func panicOnWindows() {
	if runtime.GOOS == "windows" {
		panic("the test suite is not yet fully supported on Windows")
	}
}
