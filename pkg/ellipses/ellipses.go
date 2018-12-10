/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ellipses

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	// Regex to extract ellipses syntax inputs.
	regexpEllipses = regexp.MustCompile(`(.*)({[0-9a-z]*\.\.\.[0-9a-z]*})(.*)`)

	// Ellipses constants
	openBraces  = "{"
	closeBraces = "}"
	ellipses    = "..."
)

// Parses an ellipses range pattern of following style
// `{1...64}`
// `{33...64}`
func parseEllipsesRange(pattern string) (seq []string, err error) {
	if strings.Index(pattern, openBraces) == -1 {
		return nil, errors.New("Invalid argument")
	}
	if strings.Index(pattern, closeBraces) == -1 {
		return nil, errors.New("Invalid argument")
	}

	pattern = strings.TrimPrefix(pattern, openBraces)
	pattern = strings.TrimSuffix(pattern, closeBraces)

	ellipsesRange := strings.Split(pattern, ellipses)
	if len(ellipsesRange) != 2 {
		return nil, errors.New("Invalid argument")
	}

	var hexadecimal bool
	var start, end uint64
	if start, err = strconv.ParseUint(ellipsesRange[0], 10, 64); err != nil {
		// Look for hexadecimal conversions if any.
		start, err = strconv.ParseUint(ellipsesRange[0], 16, 64)
		if err != nil {
			return nil, err
		}
		hexadecimal = true
	}

	if end, err = strconv.ParseUint(ellipsesRange[1], 10, 64); err != nil {
		// Look for hexadecimal conversions if any.
		end, err = strconv.ParseUint(ellipsesRange[1], 16, 64)
		if err != nil {
			return nil, err
		}
		hexadecimal = true
	}

	if start > end {
		return nil, fmt.Errorf("Incorrect range start %d cannot be bigger than end %d", start, end)
	}

	for i := start; i <= end; i++ {
		if strings.HasPrefix(ellipsesRange[0], "0") && len(ellipsesRange[0]) > 1 || strings.HasPrefix(ellipsesRange[1], "0") {
			if hexadecimal {
				seq = append(seq, fmt.Sprintf(fmt.Sprintf("%%0%dx", len(ellipsesRange[1])), i))
			} else {
				seq = append(seq, fmt.Sprintf(fmt.Sprintf("%%0%dd", len(ellipsesRange[1])), i))
			}
		} else {
			if hexadecimal {
				seq = append(seq, fmt.Sprintf("%x", i))
			} else {
				seq = append(seq, fmt.Sprintf("%d", i))
			}
		}
	}

	return seq, nil
}

// Pattern - ellipses pattern, describes the range and also the
// associated prefix and suffixes.
type Pattern struct {
	Prefix string
	Suffix string
	Seq    []string
}

// argExpander - recursively expands labels into its respective forms.
func argExpander(labels [][]string) (out [][]string) {
	if len(labels) == 1 {
		for _, v := range labels[0] {
			out = append(out, []string{v})
		}
		return out
	}
	for _, lbl := range labels[0] {
		rs := argExpander(labels[1:])
		for _, rlbls := range rs {
			r := append(rlbls, []string{lbl}...)
			out = append(out, r)
		}
	}
	return out
}

// ArgPattern contains a list of patterns provided in the input.
type ArgPattern []Pattern

// Expand - expands all the ellipses patterns in
// the given argument.
func (a ArgPattern) Expand() [][]string {
	labels := make([][]string, len(a))
	for i := range labels {
		labels[i] = a[i].Expand()
	}
	return argExpander(labels)
}

// Expand - expands a ellipses pattern.
func (p Pattern) Expand() []string {
	var labels []string
	for i := range p.Seq {
		switch {
		case p.Prefix != "" && p.Suffix == "":
			labels = append(labels, fmt.Sprintf("%s%s", p.Prefix, p.Seq[i]))
		case p.Suffix != "" && p.Prefix == "":
			labels = append(labels, fmt.Sprintf("%s%s", p.Seq[i], p.Suffix))
		case p.Suffix == "" && p.Prefix == "":
			labels = append(labels, fmt.Sprintf("%s", p.Seq[i]))
		default:
			labels = append(labels, fmt.Sprintf("%s%s%s", p.Prefix, p.Seq[i], p.Suffix))
		}
	}
	return labels
}

// HasEllipses - returns true if input arg has ellipses type pattern.
func HasEllipses(args ...string) bool {
	var ok = true
	for _, arg := range args {
		ok = ok && (strings.Count(arg, ellipses) > 0 || (strings.Count(arg, openBraces) > 0 && strings.Count(arg, closeBraces) > 0))
	}
	return ok
}

// ErrInvalidEllipsesFormatFn error returned when invalid ellipses format is detected.
var ErrInvalidEllipsesFormatFn = func(arg string) error {
	return fmt.Errorf("Invalid ellipsis format in (%s), Ellipsis range must be provided in format {N...M} where N and M are positive integers, M must be greater than N,  with an allowed minimum range of 4", arg)
}

// FindEllipsesPatterns - finds all ellipses patterns, recursively and parses the ranges numerically.
func FindEllipsesPatterns(arg string) (ArgPattern, error) {
	var patterns []Pattern
	parts := regexpEllipses.FindStringSubmatch(arg)
	if len(parts) == 0 {
		// We throw an error if arg doesn't have any recognizable ellipses pattern.
		return nil, ErrInvalidEllipsesFormatFn(arg)
	}

	parts = parts[1:]
	patternFound := regexpEllipses.MatchString(parts[0])
	for patternFound {
		seq, err := parseEllipsesRange(parts[1])
		if err != nil {
			return patterns, err
		}
		patterns = append(patterns, Pattern{
			Prefix: "",
			Suffix: parts[2],
			Seq:    seq,
		})
		parts = regexpEllipses.FindStringSubmatch(parts[0])
		if len(parts) > 0 {
			parts = parts[1:]
			patternFound = HasEllipses(parts[0])
			continue
		}
		break
	}

	if len(parts) > 0 {
		seq, err := parseEllipsesRange(parts[1])
		if err != nil {
			return patterns, err
		}

		patterns = append(patterns, Pattern{
			Prefix: parts[0],
			Suffix: parts[2],
			Seq:    seq,
		})
	}

	// Check if any of the prefix or suffixes now have flower braces
	// left over, in such a case we generally think that there is
	// perhaps a typo in users input and error out accordingly.
	for _, pattern := range patterns {
		if strings.Count(pattern.Prefix, openBraces) > 0 || strings.Count(pattern.Prefix, closeBraces) > 0 {
			return nil, ErrInvalidEllipsesFormatFn(arg)
		}
		if strings.Count(pattern.Suffix, openBraces) > 0 || strings.Count(pattern.Suffix, closeBraces) > 0 {
			return nil, ErrInvalidEllipsesFormatFn(arg)
		}
	}

	return patterns, nil
}
