package cli

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// IntSlice - a type of integer slice
type IntSlice []int

// Set -
func (f *IntSlice) Set(value string) error {
	tmp, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	*f = append(*f, tmp)
	return nil
}

// String -
func (f *IntSlice) String() string {
	return fmt.Sprintf("%d", *f)
}

// Value -
func (f *IntSlice) Value() []int {
	return *f
}

// IntSliceFlag - a type of integer slice flag
type IntSliceFlag struct {
	Name   string
	Value  *IntSlice
	Usage  string
	EnvVar string
	Hide   bool
}

// String -
func (f IntSliceFlag) String() string {
	firstName := strings.Trim(strings.Split(f.Name, ",")[0], " ")
	pref := prefixFor(firstName)
	return withEnvHint(f.EnvVar, fmt.Sprintf("%s [%v]\t%v", prefixedNames(f.Name), pref+firstName+" option "+pref+firstName+" option", f.Usage))
}

// Apply -
func (f IntSliceFlag) Apply(set *flag.FlagSet) {
	if f.EnvVar != "" {
		for _, envVar := range strings.Split(f.EnvVar, ",") {
			envVar = strings.TrimSpace(envVar)
			if envVal := os.Getenv(envVar); envVal != "" {
				newVal := &IntSlice{}
				for _, s := range strings.Split(envVal, ",") {
					s = strings.TrimSpace(s)
					err := newVal.Set(s)
					if err != nil {
						fmt.Fprintf(os.Stderr, err.Error())
					}
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

func (f IntSliceFlag) getName() string {
	return f.Name
}

func (f IntSliceFlag) isNotHidden() bool {
	return !f.Hide
}
