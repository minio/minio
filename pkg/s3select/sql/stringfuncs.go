// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package sql

import (
	"errors"
	"strings"
)

var (
	errMalformedEscapeSequence  = errors.New("Malformed escape sequence in LIKE clause")
	errInvalidTrimArg           = errors.New("Trim argument is invalid - this should not happen")
	errInvalidSubstringIndexLen = errors.New("Substring start index or length falls outside the string")
)

const (
	percent    rune = '%'
	underscore rune = '_'
	runeZero   rune = 0
)

func evalSQLLike(text, pattern string, escape rune) (match bool, err error) {
	s := []rune{}
	prev := runeZero
	hasLeadingPercent := false
	patLen := len([]rune(pattern))
	for i, r := range pattern {
		if i > 0 && prev == escape {
			switch r {
			case percent, escape, underscore:
				s = append(s, r)
				prev = r
				if r == escape {
					prev = runeZero
				}
			default:
				return false, errMalformedEscapeSequence
			}
			continue
		}

		prev = r

		var ok bool
		switch r {
		case percent:
			if len(s) == 0 {
				hasLeadingPercent = true
				continue
			}

			text, ok = matcher(text, string(s), hasLeadingPercent)
			if !ok {
				return false, nil
			}
			hasLeadingPercent = true
			s = []rune{}

			if i == patLen-1 {
				// Last pattern character is a %, so
				// we are done.
				return true, nil
			}

		case underscore:
			if len(s) == 0 {
				text, ok = dropRune(text)
				if !ok {
					return false, nil
				}
				continue
			}

			text, ok = matcher(text, string(s), hasLeadingPercent)
			if !ok {
				return false, nil
			}
			hasLeadingPercent = false

			text, ok = dropRune(text)
			if !ok {
				return false, nil
			}
			s = []rune{}

		case escape:
			if i == patLen-1 {
				return false, errMalformedEscapeSequence
			}
			// Otherwise do nothing.

		default:
			s = append(s, r)
		}

	}
	if hasLeadingPercent {
		return strings.HasSuffix(text, string(s)), nil
	}
	return string(s) == text, nil
}

// matcher - Finds `pat` in `text`, and returns the part remainder of
// `text`, after the match. If leadingPercent is false, `pat` must be
// the prefix of `text`, otherwise it must be a substring.
func matcher(text, pat string, leadingPercent bool) (res string, match bool) {
	if !leadingPercent {
		res = strings.TrimPrefix(text, pat)
		if len(text) == len(res) {
			return "", false
		}
	} else {
		parts := strings.SplitN(text, pat, 2)
		if len(parts) == 1 {
			return "", false
		}
		res = parts[1]
	}
	return res, true
}

func dropRune(text string) (res string, ok bool) {
	r := []rune(text)
	if len(r) == 0 {
		return "", false
	}
	return string(r[1:]), true
}

func evalSQLSubstring(s string, startIdx, length int) (res string, err error) {
	rs := []rune(s)

	// According to s3 document, if startIdx < 1, it is set to 1.
	if startIdx < 1 {
		startIdx = 1
	}

	if startIdx > len(rs) {
		startIdx = len(rs) + 1
	}

	// StartIdx is 1-based in the input
	startIdx--
	endIdx := len(rs)
	if length != -1 {
		if length < 0 {
			return "", errInvalidSubstringIndexLen
		}

		if length > (endIdx - startIdx) {
			length = endIdx - startIdx
		}

		endIdx = startIdx + length
	}

	return string(rs[startIdx:endIdx]), nil
}

const (
	trimLeading  = "LEADING"
	trimTrailing = "TRAILING"
	trimBoth     = "BOTH"
)

func evalSQLTrim(where *string, trimChars, text string) (result string, err error) {
	cutSet := " "
	if trimChars != "" {
		cutSet = trimChars
	}

	trimFunc := strings.Trim
	switch {
	case where == nil:
	case *where == trimBoth:
	case *where == trimLeading:
		trimFunc = strings.TrimLeft
	case *where == trimTrailing:
		trimFunc = strings.TrimRight
	default:
		return "", errInvalidTrimArg
	}

	return trimFunc(text, cutSet), nil
}
