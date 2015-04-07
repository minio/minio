package cli

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Float64Flag - a flag of floating number type
type Float64Flag struct {
	Name   string
	Value  float64
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f Float64Flag) String() string {
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s \"%v\"\t%v", prefixedNames(f.Name), f.Value, f.Usage))
}

// Apply -
func (f Float64Flag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				envValFloat, err := strconv.ParseFloat(envVal, 10)
				if err == nil {
					f.Value = float64(envValFloat)
				}
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Float64(name, f.Value, f.Usage)
	})
}

func (f Float64Flag) getName() string {
	return f.Name
}

func (f Float64Flag) isNotHidden() bool {
	return !f.Hide
}
