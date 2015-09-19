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
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
)

func (s *S) TestAuthLoginDatabase(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		admindb := session.DB("admin")

		err = admindb.Login("root", "wrong")
		c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")

		err = admindb.Login("root", "rapadura")
		c.Assert(err, IsNil)

		err = coll.Insert(M{"n": 1})
		c.Assert(err, IsNil)
	}
}

func (s *S) TestAuthLoginSession(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		cred := mgo.Credential{
			Username: "root",
			Password: "wrong",
		}
		err = session.Login(&cred)
		c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")

		cred.Password = "rapadura"

		err = session.Login(&cred)
		c.Assert(err, IsNil)

		err = coll.Insert(M{"n": 1})
		c.Assert(err, IsNil)
	}
}

func (s *S) TestAuthLoginLogout(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		admindb := session.DB("admin")
		err = admindb.Login("root", "rapadura")
		c.Assert(err, IsNil)

		admindb.Logout()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		// Must have dropped auth from the session too.
		session = session.Copy()
		defer session.Close()

		coll = session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")
	}
}

func (s *S) TestAuthLoginLogoutAll(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.LogoutAll()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

	// Must have dropped auth from the session too.
	session = session.Copy()
	defer session.Close()

	coll = session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")
}

func (s *S) TestAuthUpsertUserErrors(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	err = mydb.UpsertUser(&mgo.User{})
	c.Assert(err, ErrorMatches, "user has no Username")

	err = mydb.UpsertUser(&mgo.User{Username: "user", Password: "pass", UserSource: "source"})
	c.Assert(err, ErrorMatches, "user has both Password/PasswordHash and UserSource set")

	err = mydb.UpsertUser(&mgo.User{Username: "user", Password: "pass", OtherDBRoles: map[string][]mgo.Role{"db": nil}})
	c.Assert(err, ErrorMatches, "user with OtherDBRoles is only supported in the admin or \\$external databases")
}

func (s *S) TestAuthUpsertUser(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	ruser := &mgo.User{
		Username: "myruser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleRead},
	}
	rwuser := &mgo.User{
		Username: "myrwuser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleReadWrite},
	}

	err = mydb.UpsertUser(ruser)
	c.Assert(err, IsNil)
	err = mydb.UpsertUser(rwuser)
	c.Assert(err, IsNil)

	err = mydb.Login("myruser", "mypass")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = mydb.Login("myrwuser", "mypass")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	myotherdb := session.DB("myotherdb")

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Test UserSource.
	rwuserother := &mgo.User{
		Username:   "myrwuser",
		UserSource: "mydb",
		Roles:      []mgo.Role{mgo.RoleRead},
	}

	err = myotherdb.UpsertUser(rwuserother)
	if s.versionAtLeast(2, 6) {
		c.Assert(err, ErrorMatches, `MongoDB 2.6\+ does not support the UserSource setting`)
		return
	}
	c.Assert(err, IsNil)

	admindb.Logout()

	// Test indirection via UserSource: we can't write to it, because
	// the roles for myrwuser are different there.
	othercoll := myotherdb.C("myothercoll")
	err = othercoll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// Reading works, though.
	err = othercoll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// Can't login directly into the database using UserSource, though.
	err = myotherdb.Login("myrwuser", "mypass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")
}

func (s *S) TestAuthUpsertUserOtherDBRoles(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	ruser := &mgo.User{
		Username:     "myruser",
		Password:     "mypass",
		OtherDBRoles: map[string][]mgo.Role{"mydb": []mgo.Role{mgo.RoleRead}},
	}

	err = admindb.UpsertUser(ruser)
	c.Assert(err, IsNil)
	defer admindb.RemoveUser("myruser")

	admindb.Logout()
	err = admindb.Login("myruser", "mypass")

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = coll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthUpsertUserUpdates(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	// Insert a user that can read.
	user := &mgo.User{
		Username: "myruser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleRead},
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Now update the user password.
	user = &mgo.User{
		Username: "myruser",
		Password: "mynewpass",
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Login with the new user.
	usession, err := mgo.Dial("myruser:mynewpass@localhost:40002/mydb")
	c.Assert(err, IsNil)
	defer usession.Close()

	// Can read, but not write.
	err = usession.DB("mydb").C("mycoll").Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
	err = usession.DB("mydb").C("mycoll").Insert(M{"ok": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// Update the user role.
	user = &mgo.User{
		Username: "myruser",
		Roles:    []mgo.Role{mgo.RoleReadWrite},
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Dial again to ensure the password hasn't changed.
	usession, err = mgo.Dial("myruser:mynewpass@localhost:40002/mydb")
	c.Assert(err, IsNil)
	defer usession.Close()

	// Now it can write.
	err = usession.DB("mydb").C("mycoll").Insert(M{"ok": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthAddUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myruser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.AddUser("mywuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myruser", "mypass")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = mydb.Login("mywuser", "mypass")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthAddUserReplaces(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)
	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	admindb.Logout()

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")
	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	// ReadOnly flag was changed too.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
}

func (s *S) TestAuthRemoveUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.RemoveUser("myuser")
	c.Assert(err, IsNil)
	err = mydb.RemoveUser("myuser")
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")
}

func (s *S) TestAuthLoginTwiceDoesNothing(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginLogoutLoginDoesNothing(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	admindb.Logout()
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginSwitchUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	err = admindb.Login("reader", "rapadura")
	c.Assert(err, IsNil)

	// Can't write.
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// But can read.
	result := struct{ N int }{}
	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthLoginChangePassword(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, IsNil)

	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	admindb.Logout()

	// The second login must be in effect, which means read-only.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
}

func (s *S) TestAuthLoginCachingWithSessionRefresh(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.Refresh()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionCopy(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Copy()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionClone(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Clone()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithNewSession(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.New()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")
}

func (s *S) TestAuthLoginCachingAcrossPool(c *C) {
	// Logins are cached even when the conenction goes back
	// into the pool.

	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Logout root explicitly, to test both cases.
	admindb.Logout()

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("admin").Login("root", "rapadura")
	c.Assert(err, IsNil)
	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Both logins were cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// And they actually worked.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	other.DB("admin").Logout()

	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingAcrossPoolWithLogout(c *C) {
	// Now verify that logouts are properly flushed if they
	// are not revalidated after leaving the pool.

	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Just some data to query later.
	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Login was cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// Can't write, since root has been implicitly logged out
	// when the collection went into the pool, and not revalidated.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// But can read due to the revalidated myuser login.
	result := struct{ N int }{}
	err = other.DB("mydb").C("mycoll").Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthEventual(c *C) {
	// Eventual sessions don't keep sockets around, so they are
	// an interesting test case.
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			var result struct{ N int }
			err := session.DB("mydb").C("mycoll").Find(nil).One(&result)
			c.Assert(err, IsNil)
			c.Assert(result.N, Equals, 1)
		}()
	}

	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			err := session.DB("mydb").C("mycoll").Insert(M{"n": 1})
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
}

func (s *S) TestAuthURL(c *C) {
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthURLWrongCredentials(c *C) {
	session, err := mgo.Dial("mongodb://root:wrong@localhost:40002/")
	if session != nil {
		session.Close()
	}
	c.Assert(err, ErrorMatches, "auth fail(s|ed)|.*Authentication failed.")
	c.Assert(session, IsNil)
}

func (s *S) TestAuthURLWithNewSession(c *C) {
	// When authentication is in the URL, the new session will
	// actually carry it on as well, even if logged out explicitly.
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	session.DB("admin").Logout()

	// Do it twice to ensure it passes the needed data on.
	session = session.New()
	defer session.Close()
	session = session.New()
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthURLWithDatabase(c *C) {
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	mydb := session.DB("mydb")
	err = mydb.AddUser("myruser", "mypass", true)
	c.Assert(err, IsNil)

	// Test once with database, and once with source.
	for i := 0; i < 2; i++ {
		var url string
		if i == 0 {
			url = "mongodb://myruser:mypass@localhost:40002/mydb"
		} else {
			url = "mongodb://myruser:mypass@localhost:40002/admin?authSource=mydb"
		}
		usession, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer usession.Close()

		ucoll := usession.DB("mydb").C("mycoll")
		err = ucoll.FindId(0).One(nil)
		c.Assert(err, Equals, mgo.ErrNotFound)
		err = ucoll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
	}
}

func (s *S) TestDefaultDatabase(c *C) {
	tests := []struct{ url, db string }{
		{"mongodb://root:rapadura@localhost:40002", "test"},
		{"mongodb://root:rapadura@localhost:40002/admin", "admin"},
		{"mongodb://localhost:40001", "test"},
		{"mongodb://localhost:40001/", "test"},
		{"mongodb://localhost:40001/mydb", "mydb"},
	}

	for _, test := range tests {
		session, err := mgo.Dial(test.url)
		c.Assert(err, IsNil)
		defer session.Close()

		c.Logf("test: %#v", test)
		c.Assert(session.DB("").Name, Equals, test.db)

		scopy := session.Copy()
		c.Check(scopy.DB("").Name, Equals, test.db)
		scopy.Close()
	}
}

func (s *S) TestAuthDirect(c *C) {
	// Direct connections must work to the master and slaves.
	for _, port := range []string{"40031", "40032", "40033"} {
		url := fmt.Sprintf("mongodb://root:rapadura@localhost:%s/?connect=direct", port)
		session, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer session.Close()

		session.SetMode(mgo.Monotonic, true)

		var result struct{}
		err = session.DB("mydb").C("mycoll").Find(nil).One(&result)
		c.Assert(err, Equals, mgo.ErrNotFound)
	}
}

func (s *S) TestAuthDirectWithLogin(c *C) {
	// Direct connections must work to the master and slaves.
	for _, port := range []string{"40031", "40032", "40033"} {
		url := fmt.Sprintf("mongodb://localhost:%s/?connect=direct", port)
		session, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer session.Close()

		session.SetMode(mgo.Monotonic, true)
		session.SetSyncTimeout(3 * time.Second)

		err = session.DB("admin").Login("root", "rapadura")
		c.Assert(err, IsNil)

		var result struct{}
		err = session.DB("mydb").C("mycoll").Find(nil).One(&result)
		c.Assert(err, Equals, mgo.ErrNotFound)
	}
}

func (s *S) TestAuthScramSha1Cred(c *C) {
	if !s.versionAtLeast(2, 7, 7) {
		c.Skip("SCRAM-SHA-1 tests depend on 2.7.7")
	}
	cred := &mgo.Credential{
		Username:  "root",
		Password:  "rapadura",
		Mechanism: "SCRAM-SHA-1",
		Source:    "admin",
	}
	host := "localhost:40002"
	c.Logf("Connecting to %s...", host)
	session, err := mgo.Dial(host)
	c.Assert(err, IsNil)
	defer session.Close()

	mycoll := session.DB("admin").C("mycoll")

	c.Logf("Connected! Testing the need for authentication...")
	err = mycoll.Find(nil).One(nil)
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	c.Logf("Connected! Testing the need for authentication...")
	err = mycoll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthScramSha1URL(c *C) {
	if !s.versionAtLeast(2, 7, 7) {
		c.Skip("SCRAM-SHA-1 tests depend on 2.7.7")
	}
	host := "localhost:40002"
	c.Logf("Connecting to %s...", host)
	session, err := mgo.Dial(fmt.Sprintf("root:rapadura@%s?authMechanism=SCRAM-SHA-1", host))
	c.Assert(err, IsNil)
	defer session.Close()

	mycoll := session.DB("admin").C("mycoll")

	c.Logf("Connected! Testing the need for authentication...")
	err = mycoll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthX509Cred(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	binfo, err := session.BuildInfo()
	c.Assert(err, IsNil)
	if binfo.OpenSSLVersion == "" {
		c.Skip("server does not support SSL")
	}

	clientCertPEM, err := ioutil.ReadFile("testdb/client.pem")
	c.Assert(err, IsNil)

	clientCert, err := tls.X509KeyPair(clientCertPEM, clientCertPEM)
	c.Assert(err, IsNil)

	tlsConfig := &tls.Config{
		// Isolating tests to client certs, don't care about server validation.
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
	}

	var host = "localhost:40003"
	c.Logf("Connecting to %s...", host)
	session, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: []string{host},
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		},
	})
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.Login(&mgo.Credential{Username: "root", Password: "rapadura"})
	c.Assert(err, IsNil)

	// This needs to be kept in sync with client.pem
	x509Subject := "CN=localhost,OU=Client,O=MGO,L=MGO,ST=MGO,C=GO"

	externalDB := session.DB("$external")
	var x509User mgo.User = mgo.User{
		Username:     x509Subject,
		OtherDBRoles: map[string][]mgo.Role{"admin": []mgo.Role{mgo.RoleRoot}},
	}
	err = externalDB.UpsertUser(&x509User)
	c.Assert(err, IsNil)

	session.LogoutAll()

	c.Logf("Connected! Ensuring authentication is required...")
	names, err := session.DatabaseNames()
	c.Assert(err, ErrorMatches, "not authorized .*")

	cred := &mgo.Credential{
		Username:  x509Subject,
		Mechanism: "MONGODB-X509",
		Source:    "$external",
	}

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	names, err = session.DatabaseNames()
	c.Assert(err, IsNil)
	c.Assert(len(names) > 0, Equals, true)
}

var (
	plainFlag = flag.String("plain", "", "Host to test PLAIN authentication against (depends on custom environment)")
	plainUser = "einstein"
	plainPass = "password"
)

func (s *S) TestAuthPlainCred(c *C) {
	if *plainFlag == "" {
		c.Skip("no -plain")
	}
	cred := &mgo.Credential{
		Username:  plainUser,
		Password:  plainPass,
		Source:    "$external",
		Mechanism: "PLAIN",
	}
	c.Logf("Connecting to %s...", *plainFlag)
	session, err := mgo.Dial(*plainFlag)
	c.Assert(err, IsNil)
	defer session.Close()

	records := session.DB("records").C("records")

	c.Logf("Connected! Testing the need for authentication...")
	err = records.Find(nil).One(nil)
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	c.Logf("Connected! Testing the need for authentication...")
	err = records.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthPlainURL(c *C) {
	if *plainFlag == "" {
		c.Skip("no -plain")
	}
	c.Logf("Connecting to %s...", *plainFlag)
	session, err := mgo.Dial(fmt.Sprintf("%s:%s@%s?authMechanism=PLAIN", url.QueryEscape(plainUser), url.QueryEscape(plainPass), *plainFlag))
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Connected! Testing the need for authentication...")
	err = session.DB("records").C("records").Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

var (
	kerberosFlag = flag.Bool("kerberos", false, "Test Kerberos authentication (depends on custom environment)")
	kerberosHost = "ldaptest.10gen.cc"
	kerberosUser = "drivers@LDAPTEST.10GEN.CC"

	winKerberosPasswordEnv = "MGO_KERBEROS_PASSWORD"
)

// Kerberos has its own suite because it talks to a remote server
// that is prepared to authenticate against a kerberos deployment.
type KerberosSuite struct{}

var _ = Suite(&KerberosSuite{})

func (kerberosSuite *KerberosSuite) SetUpSuite(c *C) {
	mgo.SetDebug(true)
	mgo.SetStats(true)
}

func (kerberosSuite *KerberosSuite) TearDownSuite(c *C) {
	mgo.SetDebug(false)
	mgo.SetStats(false)
}

func (kerberosSuite *KerberosSuite) SetUpTest(c *C) {
	mgo.SetLogger((*cLogger)(c))
	mgo.ResetStats()
}

func (kerberosSuite *KerberosSuite) TearDownTest(c *C) {
	mgo.SetLogger(nil)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosCred(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}
	cred := &mgo.Credential{
		Username:  kerberosUser,
		Mechanism: "GSSAPI",
	}
	windowsAppendPasswordToCredential(cred)
	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Connected! Testing the need for authentication...")
	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, ErrorMatches, ".*authorized.*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err = session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosURL(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}
	c.Logf("Connecting to %s...", kerberosHost)
	connectUri := url.QueryEscape(kerberosUser) + "@" + kerberosHost + "?authMechanism=GSSAPI"
	if runtime.GOOS == "windows" {
		connectUri = url.QueryEscape(kerberosUser) + ":" + url.QueryEscape(getWindowsKerberosPassword()) + "@" + kerberosHost + "?authMechanism=GSSAPI"
	}
	session, err := mgo.Dial(connectUri)
	c.Assert(err, IsNil)
	defer session.Close()
	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosServiceName(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}

	wrongServiceName := "wrong"
	rightServiceName := "mongodb"

	cred := &mgo.Credential{
		Username:  kerberosUser,
		Mechanism: "GSSAPI",
		Service:   wrongServiceName,
	}
	windowsAppendPasswordToCredential(cred)

	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Authenticating with incorrect service name...")
	err = session.Login(cred)
	c.Assert(err, ErrorMatches, ".*@LDAPTEST.10GEN.CC not found.*")

	cred.Service = rightServiceName
	c.Logf("Authenticating with correct service name...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosServiceHost(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}

	wrongServiceHost := "eggs.bacon.tk"
	rightServiceHost := kerberosHost

	cred := &mgo.Credential{
		Username:    kerberosUser,
		Mechanism:   "GSSAPI",
		ServiceHost: wrongServiceHost,
	}
	windowsAppendPasswordToCredential(cred)

	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Authenticating with incorrect service host...")
	err = session.Login(cred)
	c.Assert(err, ErrorMatches, ".*@LDAPTEST.10GEN.CC not found.*")

	cred.ServiceHost = rightServiceHost
	c.Logf("Authenticating with correct service host...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

// No kinit on SSPI-style Kerberos, so we need to provide a password. In order
// to avoid inlining password, require it to be set as an environment variable,
// for instance: `SET MGO_KERBEROS_PASSWORD=this_isnt_the_password`
func getWindowsKerberosPassword() string {
	pw := os.Getenv(winKerberosPasswordEnv)
	if pw == "" {
		panic(fmt.Sprintf("Need to set %v environment variable to run Kerberos tests on Windows", winKerberosPasswordEnv))
	}
	return pw
}

func windowsAppendPasswordToCredential(cred *mgo.Credential) {
	if runtime.GOOS == "windows" {
		cred.Password = getWindowsKerberosPassword()
	}
}
