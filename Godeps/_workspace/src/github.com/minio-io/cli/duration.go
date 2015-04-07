package cli

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// DurationFlag - a flag of time.Duration type
type DurationFlag struct {
	Name   string
	Value  time.Duration
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f DurationFlag) String() string {
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name), f.Value, f.Usage))
}

// Apply -
func (f DurationFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValDuration, err := time.ParseDuration(envVal)
				if err == nil {
					f.Value = envValDuration
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Duration(name, f.Value, f.Usage)
	})
}

func (f DurationFlag) getName() string {
	return f.Name
}

func (f DurationFlag) isNotHidden() bool {
	return !f.Hide
}
