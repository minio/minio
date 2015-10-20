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
	"flag"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func (s *S) TestRunString(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestRunValue(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run(M{"ping": 1}, &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestPing(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Just ensure the nonce has been received.
	result := struct{}{}
	err = session.Run("ping", &result)

	mgo.ResetStats()

	err = session.Ping()
	c.Assert(err, IsNil)

	// Pretty boring.
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 1)
	c.Assert(stats.ReceivedOps, Equals, 1)
}

func (s *S) TestDialIPAddress(c *C) {
	session, err := mgo.Dial("127.0.0.1:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	session, err = mgo.Dial("[::1%]:40001")
	c.Assert(err, IsNil)
	defer session.Close()
}

func (s *S) TestURLSingle(c *C) {
	session, err := mgo.Dial("mongodb://localhost:40001/")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestURLMany(c *C) {
	session, err := mgo.Dial("mongodb://localhost:40011,localhost:40012/")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestURLParsing(c *C) {
	urls := []string{
		"localhost:40001?foo=1&bar=2",
		"localhost:40001?foo=1;bar=2",
	}
	for _, url := range urls {
		session, err := mgo.Dial(url)
		if session != nil {
			session.Close()
		}
		c.Assert(err, ErrorMatches, "unsupported connection URL option: (foo=1|bar=2)")
	}
}

func (s *S) TestInsertFindOne(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 1, "b": 3})
	c.Assert(err, IsNil)

	result := struct{ A, B int }{}

	err = coll.Find(M{"a": 1}).Sort("b").One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 1)
	c.Assert(result.B, Equals, 2)
}

func (s *S) TestInsertFindOneNil(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Find(nil).One(nil)
	c.Assert(err, ErrorMatches, "unauthorized.*|not authorized.*")
}

func (s *S) TestInsertFindOneMap(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	result := make(M)
	err = coll.Find(M{"a": 1}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["a"], Equals, 1)
	c.Assert(result["b"], Equals, 2)
}

func (s *S) TestInsertFindAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 3, "b": 4})
	c.Assert(err, IsNil)

	type R struct{ A, B int }
	var result []R

	assertResult := func() {
		c.Assert(len(result), Equals, 2)
		c.Assert(result[0].A, Equals, 1)
		c.Assert(result[0].B, Equals, 2)
		c.Assert(result[1].A, Equals, 3)
		c.Assert(result[1].B, Equals, 4)
	}

	// nil slice
	err = coll.Find(nil).Sort("a").All(&result)
	c.Assert(err, IsNil)
	assertResult()

	// Previously allocated slice
	allocd := make([]R, 5)
	result = allocd
	err = coll.Find(nil).Sort("a").All(&result)
	c.Assert(err, IsNil)
	assertResult()

	// Ensure result is backed by the originally allocated array
	c.Assert(&result[0], Equals, &allocd[0])

	// Non-pointer slice error
	f := func() { coll.Find(nil).All(result) }
	c.Assert(f, Panics, "result argument must be a slice address")

	// Non-slice error
	f = func() { coll.Find(nil).All(new(int)) }
	c.Assert(f, Panics, "result argument must be a slice address")
}

func (s *S) TestFindRef(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	db1 := session.DB("db1")
	db1col1 := db1.C("col1")

	db2 := session.DB("db2")
	db2col1 := db2.C("col1")

	err = db1col1.Insert(M{"_id": 1, "n": 1})
	c.Assert(err, IsNil)
	err = db1col1.Insert(M{"_id": 2, "n": 2})
	c.Assert(err, IsNil)
	err = db2col1.Insert(M{"_id": 2, "n": 3})
	c.Assert(err, IsNil)

	result := struct{ N int }{}

	ref1 := &mgo.DBRef{Collection: "col1", Id: 1}
	ref2 := &mgo.DBRef{Collection: "col1", Id: 2, Database: "db2"}

	err = db1.FindRef(ref1).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)

	err = db1.FindRef(ref2).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 3)

	err = db2.FindRef(ref1).One(&result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = db2.FindRef(ref2).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 3)

	err = session.FindRef(ref2).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 3)

	f := func() { session.FindRef(ref1).One(&result) }
	c.Assert(f, PanicMatches, "Can't resolve database for &mgo.DBRef{Collection:\"col1\", Id:1, Database:\"\"}")
}

func (s *S) TestDatabaseAndCollectionNames(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	db1 := session.DB("db1")
	db1col1 := db1.C("col1")
	db1col2 := db1.C("col2")

	db2 := session.DB("db2")
	db2col1 := db2.C("col3")

	err = db1col1.Insert(M{"_id": 1})
	c.Assert(err, IsNil)
	err = db1col2.Insert(M{"_id": 1})
	c.Assert(err, IsNil)
	err = db2col1.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	names, err := session.DatabaseNames()
	c.Assert(err, IsNil)
	c.Assert(filterDBs(names), DeepEquals, []string{"db1", "db2"})

	// Try to exercise cursor logic. 2.8.0-rc3 still ignores this.
	session.SetBatch(2)

	names, err = db1.CollectionNames()
	c.Assert(err, IsNil)
	c.Assert(names, DeepEquals, []string{"col1", "col2", "system.indexes"})

	names, err = db2.CollectionNames()
	c.Assert(err, IsNil)
	c.Assert(names, DeepEquals, []string{"col3", "system.indexes"})
}

func (s *S) TestSelect(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	result := struct{ A, B int }{}

	err = coll.Find(M{"a": 1}).Select(M{"b": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 0)
	c.Assert(result.B, Equals, 2)
}

func (s *S) TestInlineMap(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	var v, result1 struct {
		A int
		M map[string]int ",inline"
	}

	v.A = 1
	v.M = map[string]int{"b": 2}
	err = coll.Insert(v)
	c.Assert(err, IsNil)

	noId := M{"_id": 0}

	err = coll.Find(nil).Select(noId).One(&result1)
	c.Assert(err, IsNil)
	c.Assert(result1.A, Equals, 1)
	c.Assert(result1.M, DeepEquals, map[string]int{"b": 2})

	var result2 M
	err = coll.Find(nil).Select(noId).One(&result2)
	c.Assert(err, IsNil)
	c.Assert(result2, DeepEquals, M{"a": 1, "b": 2})

}

func (s *S) TestUpdate(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	// No changes is a no-op and shouldn't return an error.
	err = coll.Update(M{"k": 42}, M{"$set": M{"n": 42}})
	c.Assert(err, IsNil)

	err = coll.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.Update(M{"k": 47}, M{"k": 47, "n": 47})
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"k": 47}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestUpdateId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"_id": n, "n": n})
		c.Assert(err, IsNil)
	}

	err = coll.UpdateId(42, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.FindId(42).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.UpdateId(47, M{"k": 47, "n": 47})
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.FindId(47).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestUpdateNil(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"k": 42, "n": 42})
	c.Assert(err, IsNil)
	err = coll.Update(nil, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.Insert(M{"k": 45, "n": 45})
	c.Assert(err, IsNil)
	_, err = coll.UpdateAll(nil, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)
	err = coll.Find(M{"k": 45}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 46)
}

func (s *S) TestUpsert(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	info, err := coll.Upsert(M{"k": 42}, M{"k": 42, "n": 24})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.UpsertedId, IsNil)

	result := M{}
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 24)

	// Insert with internally created id.
	info, err = coll.Upsert(M{"k": 47}, M{"k": 47, "n": 47})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 0)
	c.Assert(info.UpsertedId, NotNil)

	err = coll.Find(M{"k": 47}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 47)

	result = M{}
	err = coll.Find(M{"_id": info.UpsertedId}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 47)

	// Insert with provided id.
	info, err = coll.Upsert(M{"k": 48}, M{"k": 48, "n": 48, "_id": 48})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 0)
	if s.versionAtLeast(2, 6) {
		c.Assert(info.UpsertedId, Equals, 48)
	} else {
		c.Assert(info.UpsertedId, IsNil) // Unfortunate, but that's what Mongo gave us.
	}

	err = coll.Find(M{"k": 48}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 48)
}

func (s *S) TestUpsertId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"_id": n, "n": n})
		c.Assert(err, IsNil)
	}

	info, err := coll.UpsertId(42, M{"n": 24})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.UpsertedId, IsNil)

	result := M{}
	err = coll.FindId(42).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 24)

	info, err = coll.UpsertId(47, M{"_id": 47, "n": 47})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 0)
	if s.versionAtLeast(2, 6) {
		c.Assert(info.UpsertedId, Equals, 47)
	} else {
		c.Assert(info.UpsertedId, IsNil)
	}

	err = coll.FindId(47).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 47)
}

func (s *S) TestUpdateAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	// Don't actually modify the documents. Should still report 4 matching updates.
	info, err := coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$unset": M{"missing": 1}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 4)

	info, err = coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 4)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 42)

	err = coll.Find(M{"k": 43}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)

	err = coll.Find(M{"k": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 45)

	if !s.versionAtLeast(2, 6) {
		// 2.6 made this invalid.
		info, err = coll.UpdateAll(M{"k": 47}, M{"k": 47, "n": 47})
		c.Assert(err, Equals, nil)
		c.Assert(info.Updated, Equals, 0)
	}
}

func (s *S) TestRemove(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	err = coll.Remove(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)

	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 44)
}

func (s *S) TestRemoveId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"_id": 40}, M{"_id": 41}, M{"_id": 42})
	c.Assert(err, IsNil)

	err = coll.RemoveId(41)
	c.Assert(err, IsNil)

	c.Assert(coll.FindId(40).One(nil), IsNil)
	c.Assert(coll.FindId(41).One(nil), Equals, mgo.ErrNotFound)
	c.Assert(coll.FindId(42).One(nil), IsNil)
}

func (s *S) TestRemoveUnsafe(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetSafe(nil)

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"_id": 40}, M{"_id": 41}, M{"_id": 42})
	c.Assert(err, IsNil)

	err = coll.RemoveId(41)
	c.Assert(err, IsNil)

	c.Assert(coll.FindId(40).One(nil), IsNil)
	c.Assert(coll.FindId(41).One(nil), Equals, mgo.ErrNotFound)
	c.Assert(coll.FindId(42).One(nil), IsNil)
}

func (s *S) TestRemoveAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	info, err := coll.RemoveAll(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 0)
	c.Assert(info.Removed, Equals, 4)
	c.Assert(info.UpsertedId, IsNil)

	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, Equals, mgo.ErrNotFound)

	info, err = coll.RemoveAll(nil)
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 0)
	c.Assert(info.Removed, Equals, 3)
	c.Assert(info.UpsertedId, IsNil)

	n, err := coll.Find(nil).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *S) TestDropDatabase(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	db1 := session.DB("db1")
	db1.C("col").Insert(M{"_id": 1})

	db2 := session.DB("db2")
	db2.C("col").Insert(M{"_id": 1})

	err = db1.DropDatabase()
	c.Assert(err, IsNil)

	names, err := session.DatabaseNames()
	c.Assert(err, IsNil)
	c.Assert(filterDBs(names), DeepEquals, []string{"db2"})

	err = db2.DropDatabase()
	c.Assert(err, IsNil)

	names, err = session.DatabaseNames()
	c.Assert(err, IsNil)
	c.Assert(filterDBs(names), DeepEquals, []string{})
}

func filterDBs(dbs []string) []string {
	var i int
	for _, name := range dbs {
		switch name {
		case "admin", "local":
		default:
			dbs[i] = name
			i++
		}
	}
	if len(dbs) == 0 {
		return []string{}
	}
	return dbs[:i]
}

func (s *S) TestDropCollection(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("db1")
	db.C("col1").Insert(M{"_id": 1})
	db.C("col2").Insert(M{"_id": 1})

	err = db.C("col1").DropCollection()
	c.Assert(err, IsNil)

	names, err := db.CollectionNames()
	c.Assert(err, IsNil)
	c.Assert(names, DeepEquals, []string{"col2", "system.indexes"})

	err = db.C("col2").DropCollection()
	c.Assert(err, IsNil)

	names, err = db.CollectionNames()
	c.Assert(err, IsNil)
	c.Assert(names, DeepEquals, []string{"system.indexes"})
}

func (s *S) TestCreateCollectionCapped(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	info := &mgo.CollectionInfo{
		Capped:   true,
		MaxBytes: 1024,
		MaxDocs:  3,
	}
	err = coll.Create(info)
	c.Assert(err, IsNil)

	ns := []int{1, 2, 3, 4, 5}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(nil).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
}

func (s *S) TestCreateCollectionNoIndex(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	info := &mgo.CollectionInfo{
		DisableIdIndex: true,
	}
	err = coll.Create(info)
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	indexes, err := coll.Indexes()
	c.Assert(indexes, HasLen, 0)
}

func (s *S) TestCreateCollectionForceIndex(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	info := &mgo.CollectionInfo{
		ForceIdIndex: true,
		Capped:       true,
		MaxBytes:     1024,
	}
	err = coll.Create(info)
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	indexes, err := coll.Indexes()
	c.Assert(indexes, HasLen, 1)
}

func (s *S) TestIsDupValues(c *C) {
	c.Assert(mgo.IsDup(nil), Equals, false)
	c.Assert(mgo.IsDup(&mgo.LastError{Code: 1}), Equals, false)
	c.Assert(mgo.IsDup(&mgo.QueryError{Code: 1}), Equals, false)
	c.Assert(mgo.IsDup(&mgo.LastError{Code: 11000}), Equals, true)
	c.Assert(mgo.IsDup(&mgo.QueryError{Code: 11000}), Equals, true)
	c.Assert(mgo.IsDup(&mgo.LastError{Code: 11001}), Equals, true)
	c.Assert(mgo.IsDup(&mgo.QueryError{Code: 11001}), Equals, true)
	c.Assert(mgo.IsDup(&mgo.LastError{Code: 12582}), Equals, true)
	c.Assert(mgo.IsDup(&mgo.QueryError{Code: 12582}), Equals, true)
	lerr := &mgo.LastError{Code: 16460, Err: "error inserting 1 documents to shard ... caused by :: E11000 duplicate key error index: ..."}
	c.Assert(mgo.IsDup(lerr), Equals, true)
}

func (s *S) TestIsDupPrimary(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, ErrorMatches, ".*duplicate key error.*")
	c.Assert(mgo.IsDup(err), Equals, true)
}

func (s *S) TestIsDupUnique(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	index := mgo.Index{
		Key:    []string{"a", "b"},
		Unique: true,
	}

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(index)
	c.Assert(err, IsNil)

	err = coll.Insert(M{"a": 1, "b": 1})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 1, "b": 1})
	c.Assert(err, ErrorMatches, ".*duplicate key error.*")
	c.Assert(mgo.IsDup(err), Equals, true)
}

func (s *S) TestIsDupCapped(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	info := &mgo.CollectionInfo{
		ForceIdIndex: true,
		Capped:       true,
		MaxBytes:     1024,
	}
	err = coll.Create(info)
	c.Assert(err, IsNil)

	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"_id": 1})
	// The error was different for capped collections before 2.6.
	c.Assert(err, ErrorMatches, ".*duplicate key.*")
	// The issue is reduced by using IsDup.
	c.Assert(mgo.IsDup(err), Equals, true)
}

func (s *S) TestIsDupFindAndModify(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(mgo.Index{Key: []string{"n"}, Unique: true})
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"n": 2})
	c.Assert(err, IsNil)
	_, err = coll.Find(M{"n": 1}).Apply(mgo.Change{Update: M{"$inc": M{"n": 1}}}, bson.M{})
	c.Assert(err, ErrorMatches, ".*duplicate key error.*")
	c.Assert(mgo.IsDup(err), Equals, true)
}

func (s *S) TestFindAndModify(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"n": 42})

	session.SetMode(mgo.Monotonic, true)

	result := M{}
	info, err := coll.Find(M{"n": 42}).Apply(mgo.Change{Update: M{"$inc": M{"n": 1}}}, result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 42)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.UpsertedId, IsNil)

	// A nil result parameter should be acceptable.
	info, err = coll.Find(M{"n": 43}).Apply(mgo.Change{Update: M{"$unset": M{"missing": 1}}}, nil)
	c.Assert(err, IsNil)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.UpsertedId, IsNil)

	result = M{}
	info, err = coll.Find(M{"n": 43}).Apply(mgo.Change{Update: M{"$inc": M{"n": 1}}, ReturnNew: true}, result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.UpsertedId, IsNil)

	result = M{}
	info, err = coll.Find(M{"n": 50}).Apply(mgo.Change{Upsert: true, Update: M{"n": 51, "o": 52}}, result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], IsNil)
	c.Assert(info.Updated, Equals, 0)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.UpsertedId, NotNil)

	result = M{}
	info, err = coll.Find(nil).Sort("-n").Apply(mgo.Change{Update: M{"$inc": M{"n": 1}}, ReturnNew: true}, result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 52)
	c.Assert(info.Updated, Equals, 1)
	c.Assert(info.Removed, Equals, 0)
	c.Assert(info.UpsertedId, IsNil)

	result = M{}
	info, err = coll.Find(M{"n": 52}).Select(M{"o": 1}).Apply(mgo.Change{Remove: true}, result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], IsNil)
	c.Assert(result["o"], Equals, 52)
	c.Assert(info.Updated, Equals, 0)
	c.Assert(info.Removed, Equals, 1)
	c.Assert(info.UpsertedId, IsNil)

	result = M{}
	info, err = coll.Find(M{"n": 60}).Apply(mgo.Change{Remove: true}, result)
	c.Assert(err, Equals, mgo.ErrNotFound)
	c.Assert(len(result), Equals, 0)
	c.Assert(info, IsNil)
}

func (s *S) TestFindAndModifyBug997828(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"n": "not-a-number"})

	result := make(M)
	_, err = coll.Find(M{"n": "not-a-number"}).Apply(mgo.Change{Update: M{"$inc": M{"n": 1}}}, result)
	c.Assert(err, ErrorMatches, `(exception: )?Cannot apply \$inc .*`)
	if s.versionAtLeast(2, 1) {
		qerr, _ := err.(*mgo.QueryError)
		c.Assert(qerr, NotNil, Commentf("err: %#v", err))
		if s.versionAtLeast(2, 6) {
			// Oh, the dance of error codes. :-(
			c.Assert(qerr.Code, Equals, 16837)
		} else {
			c.Assert(qerr.Code, Equals, 10140)
		}
	} else {
		lerr, _ := err.(*mgo.LastError)
		c.Assert(lerr, NotNil, Commentf("err: %#v", err))
		c.Assert(lerr.Code, Equals, 10140)
	}
}

func (s *S) TestCountCollection(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
}

func (s *S) TestCountQuery(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(M{"n": M{"$gt": 40}}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestCountQuerySorted(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(M{"n": M{"$gt": 40}}).Sort("n").Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestCountSkipLimit(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(nil).Skip(1).Limit(3).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)

	n, err = coll.Find(nil).Skip(1).Limit(5).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 4)
}

func (s *S) TestQueryExplain(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	m := M{}
	query := coll.Find(nil).Limit(2)
	err = query.Explain(m)
	c.Assert(err, IsNil)
	if m["queryPlanner"] != nil {
		c.Assert(m["executionStats"].(M)["totalDocsExamined"], Equals, 2)
	} else {
		c.Assert(m["cursor"], Equals, "BasicCursor")
		c.Assert(m["nscanned"], Equals, 2)
		c.Assert(m["n"], Equals, 2)
	}

	n := 0
	var result M
	iter := query.Iter()
	for iter.Next(&result) {
		n++
	}
	c.Assert(iter.Close(), IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestQuerySetMaxScan(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	query := coll.Find(nil).SetMaxScan(2)
	var result []M
	err = query.All(&result)
	c.Assert(err, IsNil)
	c.Assert(result, HasLen, 2)
}

func (s *S) TestQuerySetMaxTime(c *C) {
	if !s.versionAtLeast(2, 6) {
		c.Skip("SetMaxTime only supported in 2.6+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	coll := session.DB("mydb").C("mycoll")

	for i := 0; i < 1000; i++ {
		err := coll.Insert(M{"n": i})
		c.Assert(err, IsNil)
	}

	query := coll.Find(nil)
	query.SetMaxTime(1 * time.Millisecond)
	query.Batch(2)
	var result []M
	err = query.All(&result)
	c.Assert(err, ErrorMatches, "operation exceeded time limit")
}

func (s *S) TestQueryHint(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.EnsureIndexKey("a")

	m := M{}
	err = coll.Find(nil).Hint("a").Explain(m)
	c.Assert(err, IsNil)

	if m["queryPlanner"] != nil {
		m = m["queryPlanner"].(M)
		m = m["winningPlan"].(M)
		m = m["inputStage"].(M)
		c.Assert(m["indexName"], Equals, "a_1")
	} else {
		c.Assert(m["indexBounds"], NotNil)
		c.Assert(m["indexBounds"].(M)["a"], NotNil)
	}
}

func (s *S) TestQueryComment(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")
	coll := db.C("mycoll")

	err = db.Run(bson.M{"profile": 2}, nil)
	c.Assert(err, IsNil)

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	query := coll.Find(bson.M{"n": 41})
	query.Comment("some comment")
	err = query.One(nil)
	c.Assert(err, IsNil)

	query = coll.Find(bson.M{"n": 41})
	query.Comment("another comment")
	err = query.One(nil)
	c.Assert(err, IsNil)

	n, err := session.DB("mydb").C("system.profile").Find(bson.M{"query.$query.n": 41, "query.$comment": "some comment"}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (s *S) TestFindOneNotFound(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct{ A, B int }{}
	err = coll.Find(M{"a": 1}).One(&result)
	c.Assert(err, Equals, mgo.ErrNotFound)
	c.Assert(err, ErrorMatches, "not found")
	c.Assert(err == mgo.ErrNotFound, Equals, true)
}

func (s *S) TestFindNil(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	result := struct{ N int }{}

	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestFindId(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"_id": 41, "n": 41})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"_id": 42, "n": 42})
	c.Assert(err, IsNil)

	result := struct{ N int }{}

	err = coll.FindId(42).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)
}

func (s *S) TestFindIterAll(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	iter := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2).Iter()
	result := struct{ N int }{}
	for i := 2; i < 7; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindIterTwiceWithSameQuery(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for i := 40; i != 47; i++ {
		coll.Insert(M{"n": i})
	}

	query := coll.Find(M{}).Sort("n")

	result1 := query.Skip(1).Iter()
	result2 := query.Skip(2).Iter()

	result := struct{ N int }{}
	ok := result2.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(result.N, Equals, 42)
	ok = result1.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(result.N, Equals, 41)
}

func (s *S) TestFindIterWithoutResults(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"n": 42})

	iter := coll.Find(M{"n": 0}).Iter()

	result := struct{ N int }{}
	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)
	c.Assert(result.N, Equals, 0)
}

func (s *S) TestFindIterLimit(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Limit(3)
	iter := query.Iter()

	result := struct{ N int }{}
	for i := 2; i < 5; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
	}

	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 2)     // 1*QUERY_OP + 1*KILL_CURSORS_OP
	c.Assert(stats.ReceivedOps, Equals, 1) // and its REPLY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

var cursorTimeout = flag.Bool("cursor-timeout", false, "Enable cursor timeout test")

func (s *S) TestFindIterCursorTimeout(c *C) {
	if !*cursorTimeout {
		c.Skip("-cursor-timeout")
	}
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	type Doc struct {
		Id int "_id"
	}

	coll := session.DB("test").C("test")
	coll.Remove(nil)
	for i := 0; i < 100; i++ {
		err = coll.Insert(Doc{i})
		c.Assert(err, IsNil)
	}

	session.SetBatch(1)
	iter := coll.Find(nil).Iter()
	var doc Doc
	if !iter.Next(&doc) {
		c.Fatalf("iterator failed to return any documents")
	}

	for i := 10; i > 0; i-- {
		c.Logf("Sleeping... %d minutes to go...", i)
		time.Sleep(1*time.Minute + 2*time.Second)
	}

	// Drain any existing documents that were fetched.
	if !iter.Next(&doc) {
		c.Fatalf("iterator with timed out cursor failed to return previously cached document")
	}
	if iter.Next(&doc) {
		c.Fatalf("timed out cursor returned document")
	}

	c.Assert(iter.Err(), Equals, mgo.ErrCursor)
}

func (s *S) TestTooManyItemsLimitBug(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(runtime.NumCPU()))

	mgo.SetDebug(false)
	coll := session.DB("mydb").C("mycoll")
	words := strings.Split("foo bar baz", " ")
	for i := 0; i < 5; i++ {
		words = append(words, words...)
	}
	doc := bson.D{{"words", words}}
	inserts := 10000
	limit := 5000
	iters := 0
	c.Assert(inserts > limit, Equals, true)
	for i := 0; i < inserts; i++ {
		err := coll.Insert(&doc)
		c.Assert(err, IsNil)
	}
	iter := coll.Find(nil).Limit(limit).Iter()
	for iter.Next(&doc) {
		if iters%100 == 0 {
			c.Logf("Seen %d docments", iters)
		}
		iters++
	}
	c.Assert(iter.Close(), IsNil)
	c.Assert(iters, Equals, limit)
}

func serverCursorsOpen(session *mgo.Session) int {
	var result struct {
		Cursors struct {
			TotalOpen int `bson:"totalOpen"`
			TimedOut  int `bson:"timedOut"`
		}
	}
	err := session.Run("serverStatus", &result)
	if err != nil {
		panic(err)
	}
	return result.Cursors.TotalOpen
}

func (s *S) TestFindIterLimitWithMore(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	// Insane amounts of logging otherwise due to the
	// amount of data being shuffled.
	mgo.SetDebug(false)
	defer mgo.SetDebug(true)

	// Should amount to more than 4MB bson payload,
	// the default limit per result chunk.
	const total = 4096
	var d struct{ A [1024]byte }
	docs := make([]interface{}, total)
	for i := 0; i < total; i++ {
		docs[i] = &d
	}
	err = coll.Insert(docs...)
	c.Assert(err, IsNil)

	n, err := coll.Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, total)

	// First, try restricting to a single chunk with a negative limit.
	nresults := 0
	iter := coll.Find(nil).Limit(-total).Iter()
	var discard struct{}
	for iter.Next(&discard) {
		nresults++
	}
	if nresults < total/2 || nresults >= total {
		c.Fatalf("Bad result size with negative limit: %d", nresults)
	}

	cursorsOpen := serverCursorsOpen(session)

	// Try again, with a positive limit. Should reach the end now,
	// using multiple chunks.
	nresults = 0
	iter = coll.Find(nil).Limit(total).Iter()
	for iter.Next(&discard) {
		nresults++
	}
	c.Assert(nresults, Equals, total)

	// Ensure the cursor used is properly killed.
	c.Assert(serverCursorsOpen(session), Equals, cursorsOpen)

	// Edge case, -MinInt == -MinInt.
	nresults = 0
	iter = coll.Find(nil).Limit(math.MinInt32).Iter()
	for iter.Next(&discard) {
		nresults++
	}
	if nresults < total/2 || nresults >= total {
		c.Fatalf("Bad result size with MinInt32 limit: %d", nresults)
	}
}

func (s *S) TestFindIterLimitWithBatch(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	// Ping the database to ensure the nonce has been received already.
	c.Assert(session.Ping(), IsNil)

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Limit(3).Batch(2)
	iter := query.Iter()
	result := struct{ N int }{}
	for i := 2; i < 5; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 1*GET_MORE_OP + 1*KILL_CURSORS_OP
	c.Assert(stats.ReceivedOps, Equals, 2) // and its REPLY_OPs
	c.Assert(stats.ReceivedDocs, Equals, 3)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindIterSortWithBatch(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	// Without this, the logic above breaks because Mongo refuses to
	// return a cursor with an in-memory sort.
	coll.EnsureIndexKey("n")

	// Ping the database to ensure the nonce has been received already.
	c.Assert(session.Ping(), IsNil)

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$lte": 44}}).Sort("-n").Batch(2)
	iter := query.Iter()
	ns = []int{46, 45, 44, 43, 42, 41, 40}
	result := struct{ N int }{}
	for i := 2; i < len(ns); i++ {
		c.Logf("i=%d", i)
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and its REPLY_OPs
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

// Test tailable cursors in a situation where Next has to sleep to
// respect the timeout requested on Tail.
func (s *S) TestFindTailTimeoutWithSleep(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	timeout := 3 * time.Second

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2)
	iter := query.Tail(timeout)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(iter.Err(), IsNil)
		c.Assert(iter.Timeout(), Equals, false)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		// The internal AwaitData timing of MongoDB is around 2 seconds,
		// so this should force mgo to sleep at least once by itself to
		// respect the requested timeout.
		time.Sleep(timeout + 5e8*time.Nanosecond)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	ok := iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(iter.Err(), IsNil)
	c.Assert(iter.Timeout(), Equals, false)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY for nonce + 1*GET_MORE_OP on Next + 1*GET_MORE_OP on Next after sleep +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	if s.versionAtLeast(2, 6) {
		c.Assert(stats.SentOps, Equals, 4)
	} else {
		c.Assert(stats.SentOps, Equals, 5)
	}
	c.Assert(stats.ReceivedOps, Equals, 4)  // REPLY_OPs for 1*QUERY_OP for nonce + 2*GET_MORE_OPs + 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	started := time.Now()
	ok = iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Err(), IsNil)
	c.Assert(iter.Timeout(), Equals, true)
	c.Assert(started.Before(time.Now().Add(-timeout)), Equals, true)

	c.Log("Will now reuse the timed out tail cursor...")

	coll.Insert(M{"n": 48})
	ok = iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(iter.Close(), IsNil)
	c.Assert(iter.Timeout(), Equals, false)
	c.Assert(result.N, Equals, 48)
}

// Test tailable cursors in a situation where Next never gets to sleep once
// to respect the timeout requested on Tail.
func (s *S) TestFindTailTimeoutNoSleep(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	timeout := 1 * time.Second

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2)
	iter := query.Tail(timeout)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(iter.Err(), IsNil)
		c.Assert(iter.Timeout(), Equals, false)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		// The internal AwaitData timing of MongoDB is around 2 seconds,
		// so this item should arrive within the AwaitData threshold.
		time.Sleep(5e8)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	ok := iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(iter.Err(), IsNil)
	c.Assert(iter.Timeout(), Equals, false)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY_OP for nonce + 1*GET_MORE_OP on Next +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	if s.versionAtLeast(2, 6) {
		c.Assert(stats.SentOps, Equals, 3)
	} else {
		c.Assert(stats.SentOps, Equals, 4)
	}
	c.Assert(stats.ReceivedOps, Equals, 3)  // REPLY_OPs for 1*QUERY_OP for nonce + 1*GET_MORE_OPs and 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	started := time.Now()
	ok = iter.Next(&result)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Err(), IsNil)
	c.Assert(iter.Timeout(), Equals, true)
	c.Assert(started.Before(time.Now().Add(-timeout)), Equals, true)

	c.Log("Will now reuse the timed out tail cursor...")

	coll.Insert(M{"n": 48})
	ok = iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(iter.Close(), IsNil)
	c.Assert(iter.Timeout(), Equals, false)
	c.Assert(result.N, Equals, 48)
}

// Test tailable cursors in a situation where Next never gets to sleep once
// to respect the timeout requested on Tail.
func (s *S) TestFindTailNoTimeout(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2)
	iter := query.Tail(-1)
	c.Assert(err, IsNil)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		time.Sleep(5e8)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	ok := iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(iter.Err(), IsNil)
	c.Assert(iter.Timeout(), Equals, false)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY_OP for nonce + 1*GET_MORE_OP on Next +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	if s.versionAtLeast(2, 6) {
		c.Assert(stats.SentOps, Equals, 3)
	} else {
		c.Assert(stats.SentOps, Equals, 4)
	}
	c.Assert(stats.ReceivedOps, Equals, 3)  // REPLY_OPs for 1*QUERY_OP for nonce + 1*GET_MORE_OPs and 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	gotNext := make(chan bool)
	go func() {
		ok := iter.Next(&result)
		gotNext <- ok
	}()

	select {
	case ok := <-gotNext:
		c.Fatalf("Next returned: %v", ok)
	case <-time.After(3e9):
		// Good. Should still be sleeping at that point.
	}

	// Closing the session should cause Next to return.
	session.Close()

	select {
	case ok := <-gotNext:
		c.Assert(ok, Equals, false)
		c.Assert(iter.Err(), ErrorMatches, "Closed explicitly")
		c.Assert(iter.Timeout(), Equals, false)
	case <-time.After(1e9):
		c.Fatal("Closing the session did not unblock Next")
	}
}

func (s *S) TestIterNextResetsResult(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{1, 2, 3}
	for _, n := range ns {
		coll.Insert(M{"n" + strconv.Itoa(n): n})
	}

	query := coll.Find(nil).Sort("$natural")

	i := 0
	var sresult *struct{ N1, N2, N3 int }
	iter := query.Iter()
	for iter.Next(&sresult) {
		switch i {
		case 0:
			c.Assert(sresult.N1, Equals, 1)
			c.Assert(sresult.N2+sresult.N3, Equals, 0)
		case 1:
			c.Assert(sresult.N2, Equals, 2)
			c.Assert(sresult.N1+sresult.N3, Equals, 0)
		case 2:
			c.Assert(sresult.N3, Equals, 3)
			c.Assert(sresult.N1+sresult.N2, Equals, 0)
		}
		i++
	}
	c.Assert(iter.Close(), IsNil)

	i = 0
	var mresult M
	iter = query.Iter()
	for iter.Next(&mresult) {
		delete(mresult, "_id")
		switch i {
		case 0:
			c.Assert(mresult, DeepEquals, M{"n1": 1})
		case 1:
			c.Assert(mresult, DeepEquals, M{"n2": 2})
		case 2:
			c.Assert(mresult, DeepEquals, M{"n3": 3})
		}
		i++
	}
	c.Assert(iter.Close(), IsNil)

	i = 0
	var iresult interface{}
	iter = query.Iter()
	for iter.Next(&iresult) {
		mresult, ok := iresult.(bson.M)
		c.Assert(ok, Equals, true, Commentf("%#v", iresult))
		delete(mresult, "_id")
		switch i {
		case 0:
			c.Assert(mresult, DeepEquals, bson.M{"n1": 1})
		case 1:
			c.Assert(mresult, DeepEquals, bson.M{"n2": 2})
		case 2:
			c.Assert(mresult, DeepEquals, bson.M{"n3": 3})
		}
		i++
	}
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestFindForOnIter(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2)
	iter := query.Iter()

	i := 2
	var result *struct{ N int }
	err = iter.For(&result, func() error {
		c.Assert(i < 7, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
		i++
		return nil
	})
	c.Assert(err, IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindFor(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort("$natural").Prefetch(0).Batch(2)

	i := 2
	var result *struct{ N int }
	err = query.For(&result, func() error {
		c.Assert(i < 7, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
		i++
		return nil
	})
	c.Assert(err, IsNil)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindForStopOnError(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	query := coll.Find(M{"n": M{"$gte": 42}})
	i := 2
	var result *struct{ N int }
	err = query.For(&result, func() error {
		c.Assert(i < 4, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 {
			return fmt.Errorf("stop!")
		}
		i++
		return nil
	})
	c.Assert(err, ErrorMatches, "stop!")
}

func (s *S) TestFindForResetsResult(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{1, 2, 3}
	for _, n := range ns {
		coll.Insert(M{"n" + strconv.Itoa(n): n})
	}

	query := coll.Find(nil).Sort("$natural")

	i := 0
	var sresult *struct{ N1, N2, N3 int }
	err = query.For(&sresult, func() error {
		switch i {
		case 0:
			c.Assert(sresult.N1, Equals, 1)
			c.Assert(sresult.N2+sresult.N3, Equals, 0)
		case 1:
			c.Assert(sresult.N2, Equals, 2)
			c.Assert(sresult.N1+sresult.N3, Equals, 0)
		case 2:
			c.Assert(sresult.N3, Equals, 3)
			c.Assert(sresult.N1+sresult.N2, Equals, 0)
		}
		i++
		return nil
	})
	c.Assert(err, IsNil)

	i = 0
	var mresult M
	err = query.For(&mresult, func() error {
		delete(mresult, "_id")
		switch i {
		case 0:
			c.Assert(mresult, DeepEquals, M{"n1": 1})
		case 1:
			c.Assert(mresult, DeepEquals, M{"n2": 2})
		case 2:
			c.Assert(mresult, DeepEquals, M{"n3": 3})
		}
		i++
		return nil
	})
	c.Assert(err, IsNil)

	i = 0
	var iresult interface{}
	err = query.For(&iresult, func() error {
		mresult, ok := iresult.(bson.M)
		c.Assert(ok, Equals, true, Commentf("%#v", iresult))
		delete(mresult, "_id")
		switch i {
		case 0:
			c.Assert(mresult, DeepEquals, bson.M{"n1": 1})
		case 1:
			c.Assert(mresult, DeepEquals, bson.M{"n2": 2})
		case 2:
			c.Assert(mresult, DeepEquals, bson.M{"n3": 3})
		}
		i++
		return nil
	})
	c.Assert(err, IsNil)
}

func (s *S) TestFindIterSnapshot(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Insane amounts of logging otherwise due to the
	// amount of data being shuffled.
	mgo.SetDebug(false)
	defer mgo.SetDebug(true)

	coll := session.DB("mydb").C("mycoll")

	var a [1024000]byte

	for n := 0; n < 10; n++ {
		err := coll.Insert(M{"_id": n, "n": n, "a1": &a})
		c.Assert(err, IsNil)
	}

	query := coll.Find(M{"n": M{"$gt": -1}}).Batch(2).Prefetch(0)
	query.Snapshot()
	iter := query.Iter()

	seen := map[int]bool{}
	result := struct {
		Id int "_id"
	}{}
	for iter.Next(&result) {
		if len(seen) == 2 {
			// Grow all entries so that they have to move.
			// Backwards so that the order is inverted.
			for n := 10; n >= 0; n-- {
				_, err := coll.Upsert(M{"_id": n}, M{"$set": M{"a2": &a}})
				c.Assert(err, IsNil)
			}
		}
		if seen[result.Id] {
			c.Fatalf("seen duplicated key: %d", result.Id)
		}
		seen[result.Id] = true
	}
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestSort(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	coll.Insert(M{"a": 1, "b": 1})
	coll.Insert(M{"a": 2, "b": 2})
	coll.Insert(M{"a": 2, "b": 1})
	coll.Insert(M{"a": 0, "b": 1})
	coll.Insert(M{"a": 2, "b": 0})
	coll.Insert(M{"a": 0, "b": 2})
	coll.Insert(M{"a": 1, "b": 2})
	coll.Insert(M{"a": 0, "b": 0})
	coll.Insert(M{"a": 1, "b": 0})

	query := coll.Find(M{})
	query.Sort("-a") // Should be ignored.
	query.Sort("-b", "a")
	iter := query.Iter()

	l := make([]int, 18)
	r := struct{ A, B int }{}
	for i := 0; i != len(l); i += 2 {
		ok := iter.Next(&r)
		c.Assert(ok, Equals, true)
		c.Assert(err, IsNil)
		l[i] = r.A
		l[i+1] = r.B
	}

	c.Assert(l, DeepEquals, []int{0, 2, 1, 2, 2, 2, 0, 1, 1, 1, 2, 1, 0, 0, 1, 0, 2, 0})
}

func (s *S) TestSortWithBadArgs(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	f1 := func() { coll.Find(nil).Sort("") }
	f2 := func() { coll.Find(nil).Sort("+") }
	f3 := func() { coll.Find(nil).Sort("foo", "-") }

	for _, f := range []func(){f1, f2, f3} {
		c.Assert(f, PanicMatches, "Sort: empty field name")
	}
}

func (s *S) TestSortScoreText(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(mgo.Index{
		Key: []string{"$text:a", "$text:b"},
	})
	msg := "text search not enabled"
	if err != nil && strings.Contains(err.Error(), msg) {
		c.Skip(msg)
	}
	c.Assert(err, IsNil)

	err = coll.Insert(M{
		"a": "none",
		"b": "twice: foo foo",
	})
	c.Assert(err, IsNil)
	err = coll.Insert(M{
		"a": "just once: foo",
		"b": "none",
	})
	c.Assert(err, IsNil)
	err = coll.Insert(M{
		"a": "many: foo foo foo",
		"b": "none",
	})
	c.Assert(err, IsNil)
	err = coll.Insert(M{
		"a": "none",
		"b": "none",
		"c": "ignore: foo",
	})
	c.Assert(err, IsNil)

	query := coll.Find(M{"$text": M{"$search": "foo"}})
	query.Select(M{"score": M{"$meta": "textScore"}})
	query.Sort("$textScore:score")
	iter := query.Iter()

	var r struct{ A, B string }
	var results []string
	for iter.Next(&r) {
		results = append(results, r.A, r.B)
	}

	c.Assert(results, DeepEquals, []string{
		"many: foo foo foo", "none",
		"none", "twice: foo foo",
		"just once: foo", "none",
	})
}

func (s *S) TestPrefetching(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	const total = 600
	mgo.SetDebug(false)
	docs := make([]interface{}, total)
	for i := 0; i != total; i++ {
		docs[i] = bson.D{{"n", i}}
	}
	err = coll.Insert(docs...)
	c.Assert(err, IsNil)

	for testi := 0; testi < 5; testi++ {
		mgo.ResetStats()

		var iter *mgo.Iter
		var beforeMore int

		switch testi {
		case 0: // The default session value.
			session.SetBatch(100)
			iter = coll.Find(M{}).Iter()
			beforeMore = 75

		case 2: // Changing the session value.
			session.SetBatch(100)
			session.SetPrefetch(0.27)
			iter = coll.Find(M{}).Iter()
			beforeMore = 73

		case 1: // Changing via query methods.
			iter = coll.Find(M{}).Prefetch(0.27).Batch(100).Iter()
			beforeMore = 73

		case 3: // With prefetch on first document.
			iter = coll.Find(M{}).Prefetch(1.0).Batch(100).Iter()
			beforeMore = 0

		case 4: // Without prefetch.
			iter = coll.Find(M{}).Prefetch(0).Batch(100).Iter()
			beforeMore = 100
		}

		pings := 0
		for batchi := 0; batchi < len(docs)/100-1; batchi++ {
			c.Logf("Iterating over %d documents on batch %d", beforeMore, batchi)
			var result struct{ N int }
			for i := 0; i < beforeMore; i++ {
				ok := iter.Next(&result)
				c.Assert(ok, Equals, true, Commentf("iter.Err: %v", iter.Err()))
			}
			beforeMore = 99
			c.Logf("Done iterating.")

			session.Run("ping", nil) // Roundtrip to settle down.
			pings++

			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, (batchi+1)*100+pings)

			c.Logf("Iterating over one more document on batch %d", batchi)
			ok := iter.Next(&result)
			c.Assert(ok, Equals, true, Commentf("iter.Err: %v", iter.Err()))
			c.Logf("Done iterating.")

			session.Run("ping", nil) // Roundtrip to settle down.
			pings++

			stats = mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, (batchi+2)*100+pings)
		}
	}
}

func (s *S) TestSafeSetting(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Check the default
	safe := session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 0)
	c.Assert(safe.FSync, Equals, false)
	c.Assert(safe.J, Equals, false)

	// Tweak it
	session.SetSafe(&mgo.Safe{W: 1, WTimeout: 2, FSync: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 1)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 2)
	c.Assert(safe.FSync, Equals, true)
	c.Assert(safe.J, Equals, false)

	// Reset it again.
	session.SetSafe(&mgo.Safe{})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 0)
	c.Assert(safe.FSync, Equals, false)
	c.Assert(safe.J, Equals, false)

	// Ensure safety to something more conservative.
	session.SetSafe(&mgo.Safe{W: 5, WTimeout: 6, J: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 5)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 6)
	c.Assert(safe.FSync, Equals, false)
	c.Assert(safe.J, Equals, true)

	// Ensure safety to something less conservative won't change it.
	session.EnsureSafe(&mgo.Safe{W: 4, WTimeout: 7})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 5)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 6)
	c.Assert(safe.FSync, Equals, false)
	c.Assert(safe.J, Equals, true)

	// But to something more conservative will.
	session.EnsureSafe(&mgo.Safe{W: 6, WTimeout: 4, FSync: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 6)
	c.Assert(safe.WMode, Equals, "")
	c.Assert(safe.WTimeout, Equals, 4)
	c.Assert(safe.FSync, Equals, true)
	c.Assert(safe.J, Equals, false)

	// Even more conservative.
	session.EnsureSafe(&mgo.Safe{WMode: "majority", WTimeout: 2})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WMode, Equals, "majority")
	c.Assert(safe.WTimeout, Equals, 2)
	c.Assert(safe.FSync, Equals, true)
	c.Assert(safe.J, Equals, false)

	// WMode always overrides, whatever it is, but J doesn't.
	session.EnsureSafe(&mgo.Safe{WMode: "something", J: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WMode, Equals, "something")
	c.Assert(safe.WTimeout, Equals, 2)
	c.Assert(safe.FSync, Equals, true)
	c.Assert(safe.J, Equals, false)

	// EnsureSafe with nil does nothing.
	session.EnsureSafe(nil)
	safe = session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WMode, Equals, "something")
	c.Assert(safe.WTimeout, Equals, 2)
	c.Assert(safe.FSync, Equals, true)
	c.Assert(safe.J, Equals, false)

	// Changing the safety of a cloned session doesn't touch the original.
	clone := session.Clone()
	defer clone.Close()
	clone.EnsureSafe(&mgo.Safe{WMode: "foo"})
	safe = session.Safe()
	c.Assert(safe.WMode, Equals, "something")
}

func (s *S) TestSafeInsert(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	// Insert an element with a predefined key.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	mgo.ResetStats()

	// Session should be safe by default, so inserting it again must fail.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, ErrorMatches, ".*E11000 duplicate.*")
	c.Assert(err.(*mgo.LastError).Code, Equals, 11000)

	// It must have sent two operations (INSERT_OP + getLastError QUERY_OP)
	stats := mgo.GetStats()

	if s.versionAtLeast(2, 6) {
		c.Assert(stats.SentOps, Equals, 1)
	} else {
		c.Assert(stats.SentOps, Equals, 2)
	}

	mgo.ResetStats()

	// If we disable safety, though, it won't complain.
	session.SetSafe(nil)
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Must have sent a single operation this time (just the INSERT_OP)
	stats = mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 1)
}

func (s *S) TestSafeParameters(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	// Tweak the safety parameters to something unachievable.
	session.SetSafe(&mgo.Safe{W: 4, WTimeout: 100})
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, ErrorMatches, "timeout|timed out waiting for slaves|Not enough data-bearing nodes|waiting for replication timed out") // :-(
	if !s.versionAtLeast(2, 6) {
		// 2.6 turned it into a query error.
		c.Assert(err.(*mgo.LastError).WTimeout, Equals, true)
	}
}

func (s *S) TestQueryErrorOne(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct {
		Err string "$err"
	}{}

	err = coll.Find(M{"a": 1}).Select(M{"a": M{"b": 1}}).One(&result)
	c.Assert(err, ErrorMatches, ".*Unsupported projection option:.*")
	c.Assert(err.(*mgo.QueryError).Message, Matches, ".*Unsupported projection option:.*")
	if s.versionAtLeast(2, 6) {
		// Oh, the dance of error codes. :-(
		c.Assert(err.(*mgo.QueryError).Code, Equals, 17287)
	} else {
		c.Assert(err.(*mgo.QueryError).Code, Equals, 13097)
	}

	// The result should be properly unmarshalled with QueryError
	c.Assert(result.Err, Matches, ".*Unsupported projection option:.*")
}

func (s *S) TestQueryErrorNext(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct {
		Err string "$err"
	}{}

	iter := coll.Find(M{"a": 1}).Select(M{"a": M{"b": 1}}).Iter()

	ok := iter.Next(&result)
	c.Assert(ok, Equals, false)

	err = iter.Close()
	c.Assert(err, ErrorMatches, ".*Unsupported projection option:.*")
	c.Assert(err.(*mgo.QueryError).Message, Matches, ".*Unsupported projection option:.*")
	if s.versionAtLeast(2, 6) {
		// Oh, the dance of error codes. :-(
		c.Assert(err.(*mgo.QueryError).Code, Equals, 17287)
	} else {
		c.Assert(err.(*mgo.QueryError).Code, Equals, 13097)
	}
	c.Assert(iter.Err(), Equals, err)

	// The result should be properly unmarshalled with QueryError
	c.Assert(result.Err, Matches, ".*Unsupported projection option:.*")
}

var indexTests = []struct {
	index    mgo.Index
	expected M
}{{
	mgo.Index{
		Key:        []string{"a"},
		Background: true,
	},
	M{
		"name":       "a_1",
		"key":        M{"a": 1},
		"ns":         "mydb.mycoll",
		"background": true,
	},
}, {
	mgo.Index{
		Key:      []string{"a", "-b"},
		Unique:   true,
		DropDups: true,
	},
	M{
		"name":     "a_1_b_-1",
		"key":      M{"a": 1, "b": -1},
		"ns":       "mydb.mycoll",
		"unique":   true,
		"dropDups": true,
	},
}, {
	mgo.Index{
		Key:  []string{"@loc_old"}, // Obsolete
		Min:  -500,
		Max:  500,
		Bits: 32,
	},
	M{
		"name": "loc_old_2d",
		"key":  M{"loc_old": "2d"},
		"ns":   "mydb.mycoll",
		"min":  -500.0,
		"max":  500.0,
		"bits": 32,
	},
}, {
	mgo.Index{
		Key:  []string{"$2d:loc"},
		Min:  -500,
		Max:  500,
		Bits: 32,
	},
	M{
		"name": "loc_2d",
		"key":  M{"loc": "2d"},
		"ns":   "mydb.mycoll",
		"min":  -500.0,
		"max":  500.0,
		"bits": 32,
	},
}, {
	mgo.Index{
		Key:  []string{"$2d:loc"},
		Minf: -500.1,
		Maxf: 500.1,
		Min:  1, // Should be ignored
		Max:  2,
		Bits: 32,
	},
	M{
		"name": "loc_2d",
		"key":  M{"loc": "2d"},
		"ns":   "mydb.mycoll",
		"min":  -500.1,
		"max":  500.1,
		"bits": 32,
	},
}, {
	mgo.Index{
		Key:        []string{"$geoHaystack:loc", "type"},
		BucketSize: 1,
	},
	M{
		"name":       "loc_geoHaystack_type_1",
		"key":        M{"loc": "geoHaystack", "type": 1},
		"ns":         "mydb.mycoll",
		"bucketSize": 1.0,
	},
}, {
	mgo.Index{
		Key:     []string{"$text:a", "$text:b"},
		Weights: map[string]int{"b": 42},
	},
	M{
		"name":              "a_text_b_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"a": 1, "b": 42},
		"default_language":  "english",
		"language_override": "language",
		"textIndexVersion":  2,
	},
}, {
	mgo.Index{
		Key:              []string{"$text:a"},
		DefaultLanguage:  "portuguese",
		LanguageOverride: "idioma",
	},
	M{
		"name":              "a_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"a": 1},
		"default_language":  "portuguese",
		"language_override": "idioma",
		"textIndexVersion":  2,
	},
}, {
	mgo.Index{
		Key: []string{"$text:$**"},
	},
	M{
		"name":              "$**_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"$**": 1},
		"default_language":  "english",
		"language_override": "language",
		"textIndexVersion":  2,
	},
}, {
	mgo.Index{
		Key:  []string{"cn"},
		Name: "CustomName",
	},
	M{
		"name": "CustomName",
		"key":  M{"cn": 1},
		"ns":   "mydb.mycoll",
	},
}}

func (s *S) TestEnsureIndex(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	idxs := session.DB("mydb").C("system.indexes")

	for _, test := range indexTests {
		err = coll.EnsureIndex(test.index)
		msg := "text search not enabled"
		if err != nil && strings.Contains(err.Error(), msg) {
			continue
		}
		c.Assert(err, IsNil)

		expectedName := test.index.Name
		if expectedName == "" {
			expectedName, _ = test.expected["name"].(string)
		}

		obtained := M{}
		err = idxs.Find(M{"name": expectedName}).One(obtained)
		c.Assert(err, IsNil)

		delete(obtained, "v")

		if s.versionAtLeast(2, 7) {
			// Was deprecated in 2.6, and not being reported by 2.7+.
			delete(test.expected, "dropDups")
			test.index.DropDups = false
		}

		c.Assert(obtained, DeepEquals, test.expected)

		// The result of Indexes must match closely what was used to create the index.
		indexes, err := coll.Indexes()
		c.Assert(err, IsNil)
		c.Assert(indexes, HasLen, 2)
		gotIndex := indexes[0]
		if gotIndex.Name == "_id_" {
			gotIndex = indexes[1]
		}
		wantIndex := test.index
		if wantIndex.Name == "" {
			wantIndex.Name = gotIndex.Name
		}
		if strings.HasPrefix(wantIndex.Key[0], "@") {
			wantIndex.Key[0] = "$2d:" + wantIndex.Key[0][1:]
		}
		if wantIndex.Minf == 0 && wantIndex.Maxf == 0 {
			wantIndex.Minf = float64(wantIndex.Min)
			wantIndex.Maxf = float64(wantIndex.Max)
		} else {
			wantIndex.Min = gotIndex.Min
			wantIndex.Max = gotIndex.Max
		}
		if wantIndex.DefaultLanguage == "" {
			wantIndex.DefaultLanguage = gotIndex.DefaultLanguage
		}
		if wantIndex.LanguageOverride == "" {
			wantIndex.LanguageOverride = gotIndex.LanguageOverride
		}
		for name, _ := range gotIndex.Weights {
			if _, ok := wantIndex.Weights[name]; !ok {
				if wantIndex.Weights == nil {
					wantIndex.Weights = make(map[string]int)
				}
				wantIndex.Weights[name] = 1
			}
		}
		c.Assert(gotIndex, DeepEquals, wantIndex)

		// Drop created index by key or by name if a custom name was used.
		if test.index.Name == "" {
			err = coll.DropIndex(test.index.Key...)
			c.Assert(err, IsNil)
		} else {
			err = coll.DropIndexName(test.index.Name)
			c.Assert(err, IsNil)
		}
	}
}

func (s *S) TestEnsureIndexWithBadInfo(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(mgo.Index{})
	c.Assert(err, ErrorMatches, "invalid index key:.*")

	err = coll.EnsureIndex(mgo.Index{Key: []string{""}})
	c.Assert(err, ErrorMatches, "invalid index key:.*")
}

func (s *S) TestEnsureIndexWithUnsafeSession(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetSafe(nil)

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	// Should fail since there are duplicated entries.
	index := mgo.Index{
		Key:    []string{"a"},
		Unique: true,
	}

	err = coll.EnsureIndex(index)
	c.Assert(err, ErrorMatches, ".*duplicate key error.*")
}

func (s *S) TestEnsureIndexKey(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey("a", "-b")
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")

	result1 := M{}
	err = sysidx.Find(M{"name": "a_1"}).One(result1)
	c.Assert(err, IsNil)

	result2 := M{}
	err = sysidx.Find(M{"name": "a_1_b_-1"}).One(result2)
	c.Assert(err, IsNil)

	delete(result1, "v")
	expected1 := M{
		"name": "a_1",
		"key":  M{"a": 1},
		"ns":   "mydb.mycoll",
	}
	c.Assert(result1, DeepEquals, expected1)

	delete(result2, "v")
	expected2 := M{
		"name": "a_1_b_-1",
		"key":  M{"a": 1, "b": -1},
		"ns":   "mydb.mycoll",
	}
	c.Assert(result2, DeepEquals, expected2)
}

func (s *S) TestEnsureIndexDropIndex(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey("-b")
	c.Assert(err, IsNil)

	err = coll.DropIndex("-b")
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")

	err = sysidx.Find(M{"name": "a_1"}).One(nil)
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "b_1"}).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.DropIndex("a")
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "a_1"}).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.DropIndex("a")
	c.Assert(err, ErrorMatches, "index not found.*")
}

func (s *S) TestEnsureIndexDropIndexName(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	err = coll.EnsureIndex(mgo.Index{Key: []string{"b"}, Name: "a"})
	c.Assert(err, IsNil)

	err = coll.DropIndexName("a")
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")

	err = sysidx.Find(M{"name": "a_1"}).One(nil)
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "a"}).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.DropIndexName("a_1")
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "a_1"}).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = coll.DropIndexName("a_1")
	c.Assert(err, ErrorMatches, "index not found.*")
}

func (s *S) TestEnsureIndexCaching(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	mgo.ResetStats()

	// Second EnsureIndex should be cached and do nothing.
	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 0)

	// Resetting the cache should make it contact the server again.
	session.ResetIndexCache()

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps > 0, Equals, true)

	// Dropping the index should also drop the cached index key.
	err = coll.DropIndex("a")
	c.Assert(err, IsNil)

	mgo.ResetStats()

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps > 0, Equals, true)
}

func (s *S) TestEnsureIndexGetIndexes(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey("-b")
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	// Obsolete.
	err = coll.EnsureIndexKey("@c")
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey("$2d:d")
	c.Assert(err, IsNil)

	// Try to exercise cursor logic. 2.8.0-rc3 still ignores this.
	session.SetBatch(2)

	indexes, err := coll.Indexes()
	c.Assert(err, IsNil)

	c.Assert(indexes[0].Name, Equals, "_id_")
	c.Assert(indexes[1].Name, Equals, "a_1")
	c.Assert(indexes[1].Key, DeepEquals, []string{"a"})
	c.Assert(indexes[2].Name, Equals, "b_-1")
	c.Assert(indexes[2].Key, DeepEquals, []string{"-b"})
	c.Assert(indexes[3].Name, Equals, "c_2d")
	c.Assert(indexes[3].Key, DeepEquals, []string{"$2d:c"})
	c.Assert(indexes[4].Name, Equals, "d_2d")
	c.Assert(indexes[4].Key, DeepEquals, []string{"$2d:d"})
}

func (s *S) TestEnsureIndexNameCaching(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(mgo.Index{Key: []string{"a"}, Name: "custom"})
	c.Assert(err, IsNil)

	mgo.ResetStats()

	// Second EnsureIndex should be cached and do nothing.
	err = coll.EnsureIndexKey("a")
	c.Assert(err, IsNil)

	err = coll.EnsureIndex(mgo.Index{Key: []string{"a"}, Name: "custom"})
	c.Assert(err, IsNil)

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 0)

	// Resetting the cache should make it contact the server again.
	session.ResetIndexCache()

	err = coll.EnsureIndex(mgo.Index{Key: []string{"a"}, Name: "custom"})
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps > 0, Equals, true)

	// Dropping the index should also drop the cached index key.
	err = coll.DropIndexName("custom")
	c.Assert(err, IsNil)

	mgo.ResetStats()

	err = coll.EnsureIndex(mgo.Index{Key: []string{"a"}, Name: "custom"})
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps > 0, Equals, true)
}

func (s *S) TestEnsureIndexEvalGetIndexes(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = session.Run(bson.D{{"eval", "db.getSiblingDB('mydb').mycoll.ensureIndex({b: -1})"}}, nil)
	c.Assert(err, IsNil)
	err = session.Run(bson.D{{"eval", "db.getSiblingDB('mydb').mycoll.ensureIndex({a: 1})"}}, nil)
	c.Assert(err, IsNil)
	err = session.Run(bson.D{{"eval", "db.getSiblingDB('mydb').mycoll.ensureIndex({c: -1, e: 1})"}}, nil)
	c.Assert(err, IsNil)
	err = session.Run(bson.D{{"eval", "db.getSiblingDB('mydb').mycoll.ensureIndex({d: '2d'})"}}, nil)
	c.Assert(err, IsNil)

	indexes, err := coll.Indexes()
	c.Assert(err, IsNil)

	c.Assert(indexes[0].Name, Equals, "_id_")
	c.Assert(indexes[1].Name, Equals, "a_1")
	c.Assert(indexes[1].Key, DeepEquals, []string{"a"})
	c.Assert(indexes[2].Name, Equals, "b_-1")
	c.Assert(indexes[2].Key, DeepEquals, []string{"-b"})
	c.Assert(indexes[3].Name, Equals, "c_-1_e_1")
	c.Assert(indexes[3].Key, DeepEquals, []string{"-c", "e"})
	if s.versionAtLeast(2, 2) {
		c.Assert(indexes[4].Name, Equals, "d_2d")
		c.Assert(indexes[4].Key, DeepEquals, []string{"$2d:d"})
	} else {
		c.Assert(indexes[4].Name, Equals, "d_")
		c.Assert(indexes[4].Key, DeepEquals, []string{"$2d:d"})
	}
}

var testTTL = flag.Bool("test-ttl", false, "test TTL collections (may take 1 minute)")

func (s *S) TestEnsureIndexExpireAfter(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetSafe(nil)

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"n": 1, "t": time.Now().Add(-120 * time.Second)})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"n": 2, "t": time.Now()})
	c.Assert(err, IsNil)

	// Should fail since there are duplicated entries.
	index := mgo.Index{
		Key:         []string{"t"},
		ExpireAfter: 1 * time.Minute,
	}

	err = coll.EnsureIndex(index)
	c.Assert(err, IsNil)

	indexes, err := coll.Indexes()
	c.Assert(err, IsNil)
	c.Assert(indexes[1].Name, Equals, "t_1")
	c.Assert(indexes[1].ExpireAfter, Equals, 1*time.Minute)

	if *testTTL {
		worked := false
		stop := time.Now().Add(70 * time.Second)
		for time.Now().Before(stop) {
			n, err := coll.Count()
			c.Assert(err, IsNil)
			if n == 1 {
				worked = true
				break
			}
			c.Assert(n, Equals, 2)
			c.Logf("Still has 2 entries...")
			time.Sleep(1 * time.Second)
		}
		if !worked {
			c.Fatalf("TTL index didn't work")
		}
	}
}

func (s *S) TestDistinct(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	var result []int
	err = coll.Find(M{"n": M{"$gt": 2}}).Sort("n").Distinct("n", &result)

	sort.IntSlice(result).Sort()
	c.Assert(result, DeepEquals, []int{3, 4, 6})
}

func (s *S) TestMapReduce(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, 1); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
	}
	var result []struct {
		Id    int "_id"
		Value int
	}

	info, err := coll.Find(M{"n": M{"$gt": 2}}).MapReduce(job, &result)
	c.Assert(err, IsNil)
	c.Assert(info.InputCount, Equals, 4)
	c.Assert(info.EmitCount, Equals, 4)
	c.Assert(info.OutputCount, Equals, 3)
	c.Assert(info.VerboseTime, IsNil)

	expected := map[int]int{3: 1, 4: 2, 6: 1}
	for _, item := range result {
		c.Logf("Item: %#v", &item)
		c.Assert(item.Value, Equals, expected[item.Id])
		expected[item.Id] = -1
	}
}

func (s *S) TestMapReduceFinalize(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:      "function() { emit(this.n, 1) }",
		Reduce:   "function(key, values) { return Array.sum(values) }",
		Finalize: "function(key, count) { return {count: count} }",
	}
	var result []struct {
		Id    int "_id"
		Value struct{ Count int }
	}
	_, err = coll.Find(nil).MapReduce(job, &result)
	c.Assert(err, IsNil)

	expected := map[int]int{1: 1, 2: 2, 3: 1, 4: 2, 6: 1}
	for _, item := range result {
		c.Logf("Item: %#v", &item)
		c.Assert(item.Value.Count, Equals, expected[item.Id])
		expected[item.Id] = -1
	}
}

func (s *S) TestMapReduceToCollection(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, 1); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
		Out:    "mr",
	}

	info, err := coll.Find(nil).MapReduce(job, nil)
	c.Assert(err, IsNil)
	c.Assert(info.InputCount, Equals, 7)
	c.Assert(info.EmitCount, Equals, 7)
	c.Assert(info.OutputCount, Equals, 5)
	c.Assert(info.Collection, Equals, "mr")
	c.Assert(info.Database, Equals, "mydb")

	expected := map[int]int{1: 1, 2: 2, 3: 1, 4: 2, 6: 1}
	var item *struct {
		Id    int "_id"
		Value int
	}
	mr := session.DB("mydb").C("mr")
	iter := mr.Find(nil).Iter()
	for iter.Next(&item) {
		c.Logf("Item: %#v", &item)
		c.Assert(item.Value, Equals, expected[item.Id])
		expected[item.Id] = -1
	}
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestMapReduceToOtherDb(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, 1); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
		Out:    bson.D{{"replace", "mr"}, {"db", "otherdb"}},
	}

	info, err := coll.Find(nil).MapReduce(job, nil)
	c.Assert(err, IsNil)
	c.Assert(info.InputCount, Equals, 7)
	c.Assert(info.EmitCount, Equals, 7)
	c.Assert(info.OutputCount, Equals, 5)
	c.Assert(info.Collection, Equals, "mr")
	c.Assert(info.Database, Equals, "otherdb")

	expected := map[int]int{1: 1, 2: 2, 3: 1, 4: 2, 6: 1}
	var item *struct {
		Id    int "_id"
		Value int
	}
	mr := session.DB("otherdb").C("mr")
	iter := mr.Find(nil).Iter()
	for iter.Next(&item) {
		c.Logf("Item: %#v", &item)
		c.Assert(item.Value, Equals, expected[item.Id])
		expected[item.Id] = -1
	}
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestMapReduceOutOfOrder(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, 1); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
		Out:    bson.M{"a": "a", "z": "z", "replace": "mr", "db": "otherdb", "b": "b", "y": "y"},
	}

	info, err := coll.Find(nil).MapReduce(job, nil)
	c.Assert(err, IsNil)
	c.Assert(info.Collection, Equals, "mr")
	c.Assert(info.Database, Equals, "otherdb")
}

func (s *S) TestMapReduceScope(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	coll.Insert(M{"n": 1})

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, x); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
		Scope:  M{"x": 42},
	}

	var result []bson.M
	_, err = coll.Find(nil).MapReduce(job, &result)
	c.Assert(len(result), Equals, 1)
	c.Assert(result[0]["value"], Equals, 42.0)
}

func (s *S) TestMapReduceVerbose(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for i := 0; i < 100; i++ {
		err = coll.Insert(M{"n": i})
		c.Assert(err, IsNil)
	}

	job := &mgo.MapReduce{
		Map:     "function() { emit(this.n, 1); }",
		Reduce:  "function(key, values) { return Array.sum(values); }",
		Verbose: true,
	}

	info, err := coll.Find(nil).MapReduce(job, nil)
	c.Assert(err, IsNil)
	c.Assert(info.VerboseTime, NotNil)
}

func (s *S) TestMapReduceLimit(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
		coll.Insert(M{"n": i})
	}

	job := &mgo.MapReduce{
		Map:    "function() { emit(this.n, 1); }",
		Reduce: "function(key, values) { return Array.sum(values); }",
	}

	var result []bson.M
	_, err = coll.Find(nil).Limit(3).MapReduce(job, &result)
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
}

func (s *S) TestBuildInfo(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	info, err := session.BuildInfo()
	c.Assert(err, IsNil)

	var v []int
	for i, a := range strings.Split(info.Version, ".") {
		for _, token := range []string{"-rc", "-pre"} {
			if i == 2 && strings.Contains(a, token) {
				a = a[:strings.Index(a, token)]
				info.VersionArray[len(info.VersionArray)-1] = 0
			}
		}
		n, err := strconv.Atoi(a)
		c.Assert(err, IsNil)
		v = append(v, n)
	}
	for len(v) < 4 {
		v = append(v, 0)
	}

	c.Assert(info.VersionArray, DeepEquals, v)
	c.Assert(info.GitVersion, Matches, "[a-z0-9]+")
	c.Assert(info.SysInfo, Matches, ".*[0-9:]+.*")
	if info.Bits != 32 && info.Bits != 64 {
		c.Fatalf("info.Bits is %d", info.Bits)
	}
	if info.MaxObjectSize < 8192 {
		c.Fatalf("info.MaxObjectSize seems too small: %d", info.MaxObjectSize)
	}
}

func (s *S) TestZeroTimeRoundtrip(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	var d struct{ T time.Time }
	conn := session.DB("mydb").C("mycoll")
	err = conn.Insert(d)
	c.Assert(err, IsNil)

	var result bson.M
	err = conn.Find(nil).One(&result)
	c.Assert(err, IsNil)
	t, isTime := result["t"].(time.Time)
	c.Assert(isTime, Equals, true)
	c.Assert(t, Equals, time.Time{})
}

func (s *S) TestFsyncLock(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	clone := session.Clone()
	defer clone.Close()

	err = session.FsyncLock()
	c.Assert(err, IsNil)

	done := make(chan time.Time)
	go func() {
		time.Sleep(3e9)
		now := time.Now()
		err := session.FsyncUnlock()
		c.Check(err, IsNil)
		done <- now
	}()

	err = clone.DB("mydb").C("mycoll").Insert(bson.M{"n": 1})
	unlocked := time.Now()
	unlocking := <-done
	c.Assert(err, IsNil)

	c.Assert(unlocked.After(unlocking), Equals, true)
	c.Assert(unlocked.Sub(unlocking) < 1e9, Equals, true)
}

func (s *S) TestFsync(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Not much to do here. Just a smoke check.
	err = session.Fsync(false)
	c.Assert(err, IsNil)
	err = session.Fsync(true)
	c.Assert(err, IsNil)
}

func (s *S) TestRepairCursor(c *C) {
	if !s.versionAtLeast(2, 7) {
		c.Skip("RepairCursor only works on 2.7+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	session.SetBatch(2)

	coll := session.DB("mydb").C("mycoll3")
	err = coll.DropCollection()

	ns := []int{0, 10, 20, 30, 40, 50}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	repairIter := coll.Repair()

	c.Assert(repairIter.Err(), IsNil)

	result := struct{ N int }{}
	resultCounts := map[int]int{}
	for repairIter.Next(&result) {
		resultCounts[result.N]++
	}

	c.Assert(repairIter.Next(&result), Equals, false)
	c.Assert(repairIter.Err(), IsNil)
	c.Assert(repairIter.Close(), IsNil)

	// Verify that the results of the repair cursor are valid.
	// The repair cursor can return multiple copies
	// of the same document, so to check correctness we only
	// need to verify that at least 1 of each document was returned.

	for _, key := range ns {
		c.Assert(resultCounts[key] > 0, Equals, true)
	}
}

func (s *S) TestPipeIter(c *C) {
	if !s.versionAtLeast(2, 1) {
		c.Skip("Pipe only works on 2.1+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	pipe := coll.Pipe([]M{{"$match": M{"n": M{"$gte": 42}}}})

	// Ensure cursor logic is working by forcing a small batch.
	pipe.Batch(2)

	// Smoke test for AllowDiskUse.
	pipe.AllowDiskUse()

	iter := pipe.Iter()
	result := struct{ N int }{}
	for i := 2; i < 7; i++ {
		ok := iter.Next(&result)
		c.Assert(ok, Equals, true)
		c.Assert(result.N, Equals, ns[i])
	}

	c.Assert(iter.Next(&result), Equals, false)
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestPipeAll(c *C) {
	if !s.versionAtLeast(2, 1) {
		c.Skip("Pipe only works on 2.1+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	var result []struct{ N int }
	err = coll.Pipe([]M{{"$match": M{"n": M{"$gte": 42}}}}).All(&result)
	c.Assert(err, IsNil)
	for i := 2; i < 7; i++ {
		c.Assert(result[i-2].N, Equals, ns[i])
	}
}

func (s *S) TestPipeOne(c *C) {
	if !s.versionAtLeast(2, 1) {
		c.Skip("Pipe only works on 2.1+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	result := struct{ A, B int }{}

	pipe := coll.Pipe([]M{{"$project": M{"a": 1, "b": M{"$add": []interface{}{"$b", 1}}}}})
	err = pipe.One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 1)
	c.Assert(result.B, Equals, 3)

	pipe = coll.Pipe([]M{{"$match": M{"a": 2}}})
	err = pipe.One(&result)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestPipeExplain(c *C) {
	if !s.versionAtLeast(2, 1) {
		c.Skip("Pipe only works on 2.1+")
	}

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	pipe := coll.Pipe([]M{{"$project": M{"a": 1, "b": M{"$add": []interface{}{"$b", 1}}}}})

	// The explain command result changes across versions.
	var result struct{ Ok int }
	err = pipe.Explain(&result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestBatch1Bug(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for i := 0; i < 3; i++ {
		err := coll.Insert(M{"n": i})
		c.Assert(err, IsNil)
	}

	var ns []struct{ N int }
	err = coll.Find(nil).Batch(1).All(&ns)
	c.Assert(err, IsNil)
	c.Assert(len(ns), Equals, 3)

	session.SetBatch(1)
	err = coll.Find(nil).All(&ns)
	c.Assert(err, IsNil)
	c.Assert(len(ns), Equals, 3)
}

func (s *S) TestInterfaceIterBug(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	for i := 0; i < 3; i++ {
		err := coll.Insert(M{"n": i})
		c.Assert(err, IsNil)
	}

	var result interface{}

	i := 0
	iter := coll.Find(nil).Sort("n").Iter()
	for iter.Next(&result) {
		c.Assert(result.(bson.M)["n"], Equals, i)
		i++
	}
	c.Assert(iter.Close(), IsNil)
}

func (s *S) TestFindIterCloseKillsCursor(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cursors := serverCursorsOpen(session)

	coll := session.DB("mydb").C("mycoll")
	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err = coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	iter := coll.Find(nil).Batch(2).Iter()
	c.Assert(iter.Next(bson.M{}), Equals, true)

	c.Assert(iter.Close(), IsNil)
	c.Assert(serverCursorsOpen(session), Equals, cursors)
}

func (s *S) TestLogReplay(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	for i := 0; i < 5; i++ {
		err = coll.Insert(M{"ts": time.Now()})
		c.Assert(err, IsNil)
	}

	iter := coll.Find(nil).LogReplay().Iter()
	if s.versionAtLeast(2, 6) {
		// This used to fail in 2.4. Now it's just a smoke test.
		c.Assert(iter.Err(), IsNil)
	} else {
		c.Assert(iter.Next(bson.M{}), Equals, false)
		c.Assert(iter.Err(), ErrorMatches, "no ts field in query")
	}
}

func (s *S) TestSetCursorTimeout(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 42})

	// This is just a smoke test. Won't wait 10 minutes for an actual timeout.

	session.SetCursorTimeout(0)

	var result struct{ N int }
	iter := coll.Find(nil).Iter()
	c.Assert(iter.Next(&result), Equals, true)
	c.Assert(result.N, Equals, 42)
	c.Assert(iter.Next(&result), Equals, false)
}

func (s *S) TestNewIterNoServer(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	data, err := bson.Marshal(bson.M{"a": 1})

	coll := session.DB("mydb").C("mycoll")
	iter := coll.NewIter(nil, []bson.Raw{{3, data}}, 42, nil)

	var result struct{ A int }
	ok := iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(result.A, Equals, 1)

	ok = iter.Next(&result)
	c.Assert(ok, Equals, false)

	c.Assert(iter.Err(), ErrorMatches, "server not available")
}

func (s *S) TestNewIterNoServerPresetErr(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	data, err := bson.Marshal(bson.M{"a": 1})

	coll := session.DB("mydb").C("mycoll")
	iter := coll.NewIter(nil, []bson.Raw{{3, data}}, 42, fmt.Errorf("my error"))

	var result struct{ A int }
	ok := iter.Next(&result)
	c.Assert(ok, Equals, true)
	c.Assert(result.A, Equals, 1)

	ok = iter.Next(&result)
	c.Assert(ok, Equals, false)

	c.Assert(iter.Err(), ErrorMatches, "my error")
}

// --------------------------------------------------------------------------
// Some benchmarks that require a running database.

func (s *S) BenchmarkFindIterRaw(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	doc := bson.D{
		{"f2", "a short string"},
		{"f3", bson.D{{"1", "one"}, {"2", 2.0}}},
		{"f4", []string{"a", "b", "c", "d", "e", "f", "g"}},
	}

	for i := 0; i < c.N+1; i++ {
		err := coll.Insert(doc)
		c.Assert(err, IsNil)
	}

	session.SetBatch(c.N)

	var raw bson.Raw
	iter := coll.Find(nil).Iter()
	iter.Next(&raw)
	c.ResetTimer()
	i := 0
	for iter.Next(&raw) {
		i++
	}
	c.StopTimer()
	c.Assert(iter.Err(), IsNil)
	c.Assert(i, Equals, c.N)
}
