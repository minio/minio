package cli

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// BoolFlag - a flag of bool type
type BoolFlag struct {
	Name   string
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f BoolFlag) String() string {
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s\t%v", prefixedNames(f.Name), f.Usage))
}

// Apply -
func (f BoolFlag) Apply(set *flag.FlagSet) {
	val := false
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValBool, err := strconv.ParseBool(envVal)
				if err == nil {
					val = envValBool
				}
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Bool(name, val, f.Usage)
	})
}

func (f BoolFlag) getName() string {
	return f.Name
}

func (f BoolFlag) isNotHidden() bool {
	return !f.Hide
}

// BoolTFlag - a flag of bool environment type
type BoolTFlag struct {
	Name   string
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f BoolTFlag) String() string {
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s\t%v", prefixedNames(f.Name), f.Usage))
}

// Apply -
func (f BoolTFlag) Apply(set *flag.FlagSet) {
	val := true
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValBool, err := strconv.ParseBool(envVal)
				if err == nil {
					val = envValBool
					break
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Bool(name, val, f.Usage)
	})
}

func (f BoolTFlag) getName() string {
	return f.Name
}

func (f BoolTFlag) isNotHidden() bool {
	return !f.Hide
}
