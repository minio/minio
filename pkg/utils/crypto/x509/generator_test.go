package x509

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) Testing(c *C) {
	certObj := Certificates{}
	params := X509Params{
		Hostname:   "example.com",
		IsCA:       false,
		EcdsaCurve: "P224",
		ValidFrom:  "Jan 1 15:04:05 2015",
		ValidFor:   time.Duration(3600),
	}
	err := certObj.GenerateCertificates(params)
	c.Assert(err, IsNil)
}
