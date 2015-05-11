package cli

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// IntFlag - a flag of integer type
type IntFlag struct {
	Name   string
	Value  int
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f IntFlag) String() string {
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name), f.Value, f.Usage))
}

// Apply -
func (f IntFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValInt, err := strconv.ParseInt(envVal, 0, 64)
				if err == nil {
					f.Value = int(envValInt)
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Int(name, f.Value, f.Usage)
	})
}

func (f IntFlag) getName() string {
	return f.Name
}

func (f IntFlag) isNotHidden() bool {
	return !f.Hide
}
