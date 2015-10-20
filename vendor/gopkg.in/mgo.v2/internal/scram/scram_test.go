package scram_test

import (
	"crypto/sha1"
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2/internal/scram"
	"strings"
)

var _ = Suite(&S{})

func Test(t *testing.T) { TestingT(t) }

type S struct{}

var tests = [][]string{{
	"U: user pencil",
	"N: fyko+d2lbbFgONRv9qkxdawL",
	"C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
	"S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
	"C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
	"S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=",
}, {
	"U: root fe8c89e308ec08763df36333cbf5d3a2",
	"N: OTcxNDk5NjM2MzE5",
	"C: n,,n=root,r=OTcxNDk5NjM2MzE5",
	"S: r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,s=XRDkVrFC9JuL7/F4tG0acQ==,i=10000",
	"C: c=biws,r=OTcxNDk5NjM2MzE581Ra3provgG0iDsMkDiIAlrh4532dDLp,p=6y1jp9R7ETyouTXS9fW9k5UHdBc=",
	"S: v=LBnd9dUJRxdqZiEq91NKP3z/bHA=",
}}

func (s *S) TestExamples(c *C) {
	for _, steps := range tests {
		if len(steps) < 2 || len(steps[0]) < 3 || !strings.HasPrefix(steps[0], "U: ") {
			c.Fatalf("Invalid test: %#v", steps)
		}
		auth := strings.Fields(steps[0][3:])
		client := scram.NewClient(sha1.New, auth[0], auth[1])
		first, done := true, false
		c.Logf("-----")
		c.Logf("%s", steps[0])
		for _, step := range steps[1:] {
			c.Logf("%s", step)
			switch step[:3] {
			case "N: ":
				client.SetNonce([]byte(step[3:]))
			case "C: ":
				if first {
					first = false
					done = client.Step(nil)
				}
				c.Assert(done, Equals, false)
				c.Assert(client.Err(), IsNil)
				c.Assert(string(client.Out()), Equals, step[3:])
			case "S: ":
				first = false
				done = client.Step([]byte(step[3:]))
			default:
				panic("invalid test line: " + step)
			}
		}
		c.Assert(done, Equals, true)
		c.Assert(client.Err(), IsNil)
	}
}
