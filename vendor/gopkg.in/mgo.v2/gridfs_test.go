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
	"io"
	"os"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func (s *S) TestGridFSCreate(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	before := bson.Now()

	gfs := db.GridFS("fs")
	file, err := gfs.Create("")
	c.Assert(err, IsNil)

	n, err := file.Write([]byte("some data"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 9)

	err = file.Close()
	c.Assert(err, IsNil)

	after := bson.Now()

	// Check the file information.
	result := M{}
	err = db.C("fs.files").Find(nil).One(result)
	c.Assert(err, IsNil)

	fileId, ok := result["_id"].(bson.ObjectId)
	c.Assert(ok, Equals, true)
	c.Assert(fileId.Valid(), Equals, true)
	result["_id"] = "<id>"

	ud, ok := result["uploadDate"].(time.Time)
	c.Assert(ok, Equals, true)
	c.Assert(ud.After(before) && ud.Before(after), Equals, true)
	result["uploadDate"] = "<timestamp>"

	expected := M{
		"_id":        "<id>",
		"length":     9,
		"chunkSize":  255 * 1024,
		"uploadDate": "<timestamp>",
		"md5":        "1e50210a0202497fb79bc38b6ade6c34",
	}
	c.Assert(result, DeepEquals, expected)

	// Check the chunk.
	result = M{}
	err = db.C("fs.chunks").Find(nil).One(result)
	c.Assert(err, IsNil)

	chunkId, ok := result["_id"].(bson.ObjectId)
	c.Assert(ok, Equals, true)
	c.Assert(chunkId.Valid(), Equals, true)
	result["_id"] = "<id>"

	expected = M{
		"_id":      "<id>",
		"files_id": fileId,
		"n":        0,
		"data":     []byte("some data"),
	}
	c.Assert(result, DeepEquals, expected)

	// Check that an index was created.
	indexes, err := db.C("fs.chunks").Indexes()
	c.Assert(err, IsNil)
	c.Assert(len(indexes), Equals, 2)
	c.Assert(indexes[1].Key, DeepEquals, []string{"files_id", "n"})
}

func (s *S) TestGridFSFileDetails(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("myfile1.txt")
	c.Assert(err, IsNil)

	n, err := file.Write([]byte("some"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 4)

	c.Assert(file.Size(), Equals, int64(4))

	n, err = file.Write([]byte(" data"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)

	c.Assert(file.Size(), Equals, int64(9))

	id, _ := file.Id().(bson.ObjectId)
	c.Assert(id.Valid(), Equals, true)
	c.Assert(file.Name(), Equals, "myfile1.txt")
	c.Assert(file.ContentType(), Equals, "")

	var info interface{}
	err = file.GetMeta(&info)
	c.Assert(err, IsNil)
	c.Assert(info, IsNil)

	file.SetId("myid")
	file.SetName("myfile2.txt")
	file.SetContentType("text/plain")
	file.SetMeta(M{"any": "thing"})

	c.Assert(file.Id(), Equals, "myid")
	c.Assert(file.Name(), Equals, "myfile2.txt")
	c.Assert(file.ContentType(), Equals, "text/plain")

	err = file.GetMeta(&info)
	c.Assert(err, IsNil)
	c.Assert(info, DeepEquals, bson.M{"any": "thing"})

	err = file.Close()
	c.Assert(err, IsNil)

	c.Assert(file.MD5(), Equals, "1e50210a0202497fb79bc38b6ade6c34")

	ud := file.UploadDate()
	now := time.Now()
	c.Assert(ud.Before(now), Equals, true)
	c.Assert(ud.After(now.Add(-3*time.Second)), Equals, true)

	result := M{}
	err = db.C("fs.files").Find(nil).One(result)
	c.Assert(err, IsNil)

	result["uploadDate"] = "<timestamp>"

	expected := M{
		"_id":         "myid",
		"length":      9,
		"chunkSize":   255 * 1024,
		"uploadDate":  "<timestamp>",
		"md5":         "1e50210a0202497fb79bc38b6ade6c34",
		"filename":    "myfile2.txt",
		"contentType": "text/plain",
		"metadata":    M{"any": "thing"},
	}
	c.Assert(result, DeepEquals, expected)
}

func (s *S) TestGridFSSetUploadDate(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")
	file, err := gfs.Create("")
	c.Assert(err, IsNil)

	t := time.Date(2014, 1, 1, 1, 1, 1, 0, time.Local)
	file.SetUploadDate(t)

	err = file.Close()
	c.Assert(err, IsNil)

	// Check the file information.
	result := M{}
	err = db.C("fs.files").Find(nil).One(result)
	c.Assert(err, IsNil)

	ud := result["uploadDate"].(time.Time)
	if !ud.Equal(t) {
		c.Fatalf("want upload date %s, got %s", t, ud)
	}
}

func (s *S) TestGridFSCreateWithChunking(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("")
	c.Assert(err, IsNil)

	file.SetChunkSize(5)

	// Smaller than the chunk size.
	n, err := file.Write([]byte("abc"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)

	// Boundary in the middle.
	n, err = file.Write([]byte("defg"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 4)

	// Boundary at the end.
	n, err = file.Write([]byte("hij"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)

	// Larger than the chunk size, with 3 chunks.
	n, err = file.Write([]byte("klmnopqrstuv"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 12)

	err = file.Close()
	c.Assert(err, IsNil)

	// Check the file information.
	result := M{}
	err = db.C("fs.files").Find(nil).One(result)
	c.Assert(err, IsNil)

	fileId, _ := result["_id"].(bson.ObjectId)
	c.Assert(fileId.Valid(), Equals, true)
	result["_id"] = "<id>"
	result["uploadDate"] = "<timestamp>"

	expected := M{
		"_id":        "<id>",
		"length":     22,
		"chunkSize":  5,
		"uploadDate": "<timestamp>",
		"md5":        "44a66044834cbe55040089cabfc102d5",
	}
	c.Assert(result, DeepEquals, expected)

	// Check the chunks.
	iter := db.C("fs.chunks").Find(nil).Sort("n").Iter()
	dataChunks := []string{"abcde", "fghij", "klmno", "pqrst", "uv"}
	for i := 0; ; i++ {
		result = M{}
		if !iter.Next(result) {
			if i != 5 {
				c.Fatalf("Expected 5 chunks, got %d", i)
			}
			break
		}
		c.Assert(iter.Close(), IsNil)

		result["_id"] = "<id>"

		expected = M{
			"_id":      "<id>",
			"files_id": fileId,
			"n":        i,
			"data":     []byte(dataChunks[i]),
		}
		c.Assert(result, DeepEquals, expected)
	}
}

func (s *S) TestGridFSAbort(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")
	file, err := gfs.Create("")
	c.Assert(err, IsNil)

	file.SetChunkSize(5)

	n, err := file.Write([]byte("some data"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 9)

	var count int
	for i := 0; i < 10; i++ {
		count, err = db.C("fs.chunks").Count()
		if count > 0 || err != nil {
			break
		}
	}
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)

	file.Abort()

	err = file.Close()
	c.Assert(err, ErrorMatches, "write aborted")

	count, err = db.C("fs.chunks").Count()
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)
}

func (s *S) TestGridFSCloseConflict(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	db.C("fs.files").EnsureIndex(mgo.Index{Key: []string{"filename"}, Unique: true})

	// For a closing-time conflict
	err = db.C("fs.files").Insert(M{"filename": "foo.txt"})
	c.Assert(err, IsNil)

	gfs := db.GridFS("fs")
	file, err := gfs.Create("foo.txt")
	c.Assert(err, IsNil)

	_, err = file.Write([]byte("some data"))
	c.Assert(err, IsNil)

	err = file.Close()
	c.Assert(mgo.IsDup(err), Equals, true)

	count, err := db.C("fs.chunks").Count()
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0)
}

func (s *S) TestGridFSOpenNotFound(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")
	file, err := gfs.OpenId("non-existent")
	c.Assert(err == mgo.ErrNotFound, Equals, true)
	c.Assert(file, IsNil)

	file, err = gfs.Open("non-existent")
	c.Assert(err == mgo.ErrNotFound, Equals, true)
	c.Assert(file, IsNil)
}

func (s *S) TestGridFSReadAll(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")
	file, err := gfs.Create("")
	c.Assert(err, IsNil)
	id := file.Id()

	file.SetChunkSize(5)

	n, err := file.Write([]byte("abcdefghijklmnopqrstuv"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 22)

	err = file.Close()
	c.Assert(err, IsNil)

	file, err = gfs.OpenId(id)
	c.Assert(err, IsNil)

	b := make([]byte, 30)
	n, err = file.Read(b)
	c.Assert(n, Equals, 22)
	c.Assert(err, IsNil)

	n, err = file.Read(b)
	c.Assert(n, Equals, 0)
	c.Assert(err == io.EOF, Equals, true)

	err = file.Close()
	c.Assert(err, IsNil)
}

func (s *S) TestGridFSReadChunking(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("")
	c.Assert(err, IsNil)

	id := file.Id()

	file.SetChunkSize(5)

	n, err := file.Write([]byte("abcdefghijklmnopqrstuv"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 22)

	err = file.Close()
	c.Assert(err, IsNil)

	file, err = gfs.OpenId(id)
	c.Assert(err, IsNil)

	b := make([]byte, 30)

	// Smaller than the chunk size.
	n, err = file.Read(b[:3])
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(b[:3], DeepEquals, []byte("abc"))

	// Boundary in the middle.
	n, err = file.Read(b[:4])
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 4)
	c.Assert(b[:4], DeepEquals, []byte("defg"))

	// Boundary at the end.
	n, err = file.Read(b[:3])
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
	c.Assert(b[:3], DeepEquals, []byte("hij"))

	// Larger than the chunk size, with 3 chunks.
	n, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 12)
	c.Assert(b[:12], DeepEquals, []byte("klmnopqrstuv"))

	n, err = file.Read(b)
	c.Assert(n, Equals, 0)
	c.Assert(err == io.EOF, Equals, true)

	err = file.Close()
	c.Assert(err, IsNil)
}

func (s *S) TestGridFSOpen(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'1'})
	file.Close()

	file, err = gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'2'})
	file.Close()

	file, err = gfs.Open("myfile.txt")
	c.Assert(err, IsNil)
	defer file.Close()

	var b [1]byte

	_, err = file.Read(b[:])
	c.Assert(err, IsNil)
	c.Assert(string(b[:]), Equals, "2")
}

func (s *S) TestGridFSSeek(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")
	file, err := gfs.Create("")
	c.Assert(err, IsNil)
	id := file.Id()

	file.SetChunkSize(5)

	n, err := file.Write([]byte("abcdefghijklmnopqrstuv"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 22)

	err = file.Close()
	c.Assert(err, IsNil)

	b := make([]byte, 5)

	file, err = gfs.OpenId(id)
	c.Assert(err, IsNil)

	o, err := file.Seek(3, os.SEEK_SET)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(3))
	_, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, []byte("defgh"))

	o, err = file.Seek(5, os.SEEK_CUR)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(13))
	_, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, []byte("nopqr"))

	o, err = file.Seek(0, os.SEEK_END)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(22))
	n, err = file.Read(b)
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)

	o, err = file.Seek(-10, os.SEEK_END)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(12))
	_, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, []byte("mnopq"))

	o, err = file.Seek(8, os.SEEK_SET)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(8))
	_, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, []byte("ijklm"))

	// Trivial seek forward within same chunk. Already
	// got the data, shouldn't touch the database.
	sent := mgo.GetStats().SentOps
	o, err = file.Seek(1, os.SEEK_CUR)
	c.Assert(err, IsNil)
	c.Assert(o, Equals, int64(14))
	c.Assert(mgo.GetStats().SentOps, Equals, sent)
	_, err = file.Read(b)
	c.Assert(err, IsNil)
	c.Assert(b, DeepEquals, []byte("opqrs"))

	// Try seeking past end of file.
	file.Seek(3, os.SEEK_SET)
	o, err = file.Seek(23, os.SEEK_SET)
	c.Assert(err, ErrorMatches, "seek past end of file")
	c.Assert(o, Equals, int64(3))
}

func (s *S) TestGridFSRemoveId(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'1'})
	file.Close()

	file, err = gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'2'})
	id := file.Id()
	file.Close()

	err = gfs.RemoveId(id)
	c.Assert(err, IsNil)

	file, err = gfs.Open("myfile.txt")
	c.Assert(err, IsNil)
	defer file.Close()

	var b [1]byte

	_, err = file.Read(b[:])
	c.Assert(err, IsNil)
	c.Assert(string(b[:]), Equals, "1")

	n, err := db.C("fs.chunks").Find(M{"files_id": id}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *S) TestGridFSRemove(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'1'})
	file.Close()

	file, err = gfs.Create("myfile.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'2'})
	file.Close()

	err = gfs.Remove("myfile.txt")
	c.Assert(err, IsNil)

	_, err = gfs.Open("myfile.txt")
	c.Assert(err == mgo.ErrNotFound, Equals, true)

	n, err := db.C("fs.chunks").Find(nil).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *S) TestGridFSOpenNext(c *C) {
	session, err := mgo.Dial("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	db := session.DB("mydb")

	gfs := db.GridFS("fs")

	file, err := gfs.Create("myfile1.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'1'})
	file.Close()

	file, err = gfs.Create("myfile2.txt")
	c.Assert(err, IsNil)
	file.Write([]byte{'2'})
	file.Close()

	var f *mgo.GridFile
	var b [1]byte

	iter := gfs.Find(nil).Sort("-filename").Iter()

	ok := gfs.OpenNext(iter, &f)
	c.Assert(ok, Equals, true)
	c.Check(f.Name(), Equals, "myfile2.txt")

	_, err = f.Read(b[:])
	c.Assert(err, IsNil)
	c.Assert(string(b[:]), Equals, "2")

	ok = gfs.OpenNext(iter, &f)
	c.Assert(ok, Equals, true)
	c.Check(f.Name(), Equals, "myfile1.txt")

	_, err = f.Read(b[:])
	c.Assert(err, IsNil)
	c.Assert(string(b[:]), Equals, "1")

	ok = gfs.OpenNext(iter, &f)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)
	c.Assert(f, IsNil)

	// Do it again with a more restrictive query to make sure
	// it's actually taken into account.
	iter = gfs.Find(bson.M{"filename": "myfile1.txt"}).Iter()

	ok = gfs.OpenNext(iter, &f)
	c.Assert(ok, Equals, true)
	c.Check(f.Name(), Equals, "myfile1.txt")

	ok = gfs.OpenNext(iter, &f)
	c.Assert(ok, Equals, false)
	c.Assert(iter.Close(), IsNil)
	c.Assert(f, IsNil)
}
