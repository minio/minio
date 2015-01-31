package tiedot

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) Testing(c *C) {
	d, err := NewDatabase("/tmp/testdata")
	defer os.RemoveAll("/tmp/testdata")
	c.Assert(err, IsNil)

	d.InitCollection("Matrix")

	data := map[string]interface{}{
		"version":  "1.4",
		"url":      "golang.org",
		"language": "Go",
	}

	_, err1 := d.InsertToCollection("Matrix", data)
	c.Assert(err1, IsNil)

	var indexes []string
	indexes = []string{"version", "url", "language"}
	err2 := d.InsertIndexToCollection("Matrix", indexes)
	c.Assert(err2, IsNil)
}
