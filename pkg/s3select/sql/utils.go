/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package sql

import (
	"fmt"
	"strings"
)

// String functions

// String - returns the JSONPath representation
func (e *JSONPath) String() string {
	if len(e.pathString) == 0 {
		parts := make([]string, len(e.PathExpr)+1)
		parts[0] = e.BaseKey.String()
		for i, pe := range e.PathExpr {
			parts[i+1] = pe.String()
		}
		e.pathString = strings.Join(parts, "")
	}
	return e.pathString
}

func (e *JSONPathElement) String() string {
	switch {
	case e.Key != nil:
		return e.Key.String()
	case e.Index != nil:
		return fmt.Sprintf("[%d]", *e.Index)
	case e.ObjectWildcard:
		return ".*"
	case e.ArrayWildcard:
		return "[*]"
	}
	return ""
}

// String removes double quotes in quoted identifiers
func (i *Identifier) String() string {
	if i.Unquoted != nil {
		return *i.Unquoted
	}
	return string(*i.Quoted)
}

func (o *ObjectKey) String() string {
	if o.Lit != nil {
		return fmt.Sprintf("['%s']", string(*o.Lit))
	}
	return fmt.Sprintf(".%s", o.ID.String())
}

func (o *ObjectKey) keyString() string {
	if o.Lit != nil {
		return string(*o.Lit)
	}
	return o.ID.String()
}

// getLastKeypathComponent checks if the given expression is a path
// expression, and if so extracts the last dot separated component of
// the path. Otherwise it returns false.
func getLastKeypathComponent(e *Expression) (string, bool) {
	if len(e.And) > 1 ||
		len(e.And[0].Condition) > 1 ||
		e.And[0].Condition[0].Not != nil ||
		e.And[0].Condition[0].Operand.ConditionRHS != nil {
		return "", false
	}

	operand := e.And[0].Condition[0].Operand.Operand
	if operand.Right != nil ||
		operand.Left.Right != nil ||
		operand.Left.Left.Negated != nil ||
		operand.Left.Left.Primary.JPathExpr == nil {
		return "", false
	}

	// Check if path expression ends in a key
	jpath := operand.Left.Left.Primary.JPathExpr
	n := len(jpath.PathExpr)
	if n > 0 && jpath.PathExpr[n-1].Key == nil {
		return "", false
	}
	ps := jpath.String()
	if idx := strings.LastIndex(ps, "."); idx >= 0 {
		// Get last part of path string.
		ps = ps[idx+1:]
	}
	return ps, true
}

// HasKeypath returns if the from clause has a key path -
// e.g. S3object[*].id
func (from *TableExpression) HasKeypath() bool {
	return len(from.Table.PathExpr) > 1
}
