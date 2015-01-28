package database

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) Testing(c *C) {
	d := NewDatabase()
	d.GetDBHandle("/tmp/testdata")
	defer os.RemoveAll("/tmp/testdata")

	d.InitCollection("Matrix")

	data := map[string]interface{}{
		"version":  "1.4",
		"url":      "golang.org",
		"language": "Go",
	}

	docId, err1 := d.InsertToCollection("Matrix", data)
	c.Assert(err1, IsNil)

	retdata, err2 := d.GetCollectionData("Matrix", docId)

	c.Assert(err2, IsNil)
	c.Assert(data, DeepEquals, retdata)
}
