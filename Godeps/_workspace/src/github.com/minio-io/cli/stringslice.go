package cli

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// StringSlice - type
type StringSlice []string

// Set -
func (f *StringSlice) Set(value string) error {
	*f = append(*f, value)
	return nil
}

// String -
func (f *StringSlice) String() string {
	return fmt.Sprintf("%s", *f)
}

// Value -
func (f *StringSlice) Value() []string {
	return *f
}

// StringSliceFlag - a string slice flag type
type StringSliceFlag struct {
	Name   string
	Value  *StringSlice
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f StringSliceFlag) String() string {
	firstName := strings.Trim(strings.Split(f.Name, ",")[0], " ")
	pref := prefixFor(firstName)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s [%v]\t%v", prefixedNames(f.Name), pref+firstName+" option "+pref+firstName+" option", f.Usage))
}

// Apply -
func (f StringSliceFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				newVal := &StringSlice{}
				for _, s := range strings.Split(envVal, ",") {
					s = strings.TrimSpace(s)
					newVal.Set(s)
				}
				f.Value = newVal
				break
			}
		}
	}

	eachName(f.Name, func(name string) {
		set.Var(f.Value, name, f.Usage)
	})
}

func (f StringSliceFlag) getName() string {
	return f.Name
}

func (f StringSliceFlag) isNotHidden() bool {
	return !f.Hide
}
