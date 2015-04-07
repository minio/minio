package cli

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// StringFlag - a string flag type
type StringFlag struct {
	Name   string
	Value  string
	Usage  string
	EnvVar string
	Hide   bool
}

func (f StringFlag) String() string {
	var fmtString string
	fmtString = "%s %v\t%v"

	if len(f.Value) > 0 {
		fmtString = "%s \"%v\"\t%v"
	} else {
		fmtString = "%s %v\t%v"
	}
	return withEnvHint(f.EnvVar, fmt.Sprintf(fmtString, prefixedNames(f.Name), f.Value, f.Usage))
}

// Apply -
func (f StringFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				f.Value = envVal
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.String(name, f.Value, f.Usage)
	})
}

func (f StringFlag) getName() string {
	return f.Name
}

func (f StringFlag) isNotHidden() bool {
	return !f.Hide
}
