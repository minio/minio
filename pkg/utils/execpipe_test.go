// !build linux,amd64

package utils

import (
	"io/ioutil"
	"os/exec"
	"testing"

	. "gopkg.in/check.v1"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (s *MySuite) TestPiping(c *C) {
	// Collect directories from the command-line
	dirs := []string{"."}

	// Run the command on each directory
	for _, dir := range dirs {
		// find $DIR -type f # Find all files
		ls := exec.Command("ls", "-l", dir)

		// | sort -t. -k2 # Sort by file extension
		sort := exec.Command("sort", "-t.", "-k2")

		// Run
		output, err := ExecPipe(ls, sort)
		c.Assert(err, IsNil)
		outputBytes, err := ioutil.ReadAll(output)
		c.Assert(err, IsNil)
		c.Assert(len(outputBytes), Not(Equals), 0)
	}
}
