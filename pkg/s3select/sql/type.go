/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

// Type - value type.
type Type string

const (
	// Null - represents NULL value type.
	Null Type = "null"

	// Bool - represents boolean value type.
	Bool Type = "bool"

	// Int - represents integer value type.
	Int Type = "int"

	// Float - represents floating point value type.
	Float Type = "float"

	// String - represents string value type.
	String Type = "string"

	// Timestamp - represents time value type.
	Timestamp Type = "timestamp"

	// Array - represents array of values where each value type is one of above.
	Array Type = "array"

	column             Type = "column"
	record             Type = "record"
	function           Type = "function"
	aggregateFunction  Type = "aggregatefunction"
	arithmeticFunction Type = "arithmeticfunction"
	comparisonFunction Type = "comparisonfunction"
	logicalFunction    Type = "logicalfunction"

	// Integer            Type = "integer" // Same as Int
	// Decimal            Type = "decimal" // Same as Float
	// Numeric            Type = "numeric" // Same as Float
)

func (t Type) isBase() bool {
	switch t {
	case Null, Bool, Int, Float, String, Timestamp:
		return true
	}

	return false
}

func (t Type) isBaseKind() bool {
	switch t {
	case Null, Bool, Int, Float, String, Timestamp, column:
		return true
	}

	return false
}

func (t Type) isNumber() bool {
	switch t {
	case Int, Float:
		return true
	}

	return false
}

func (t Type) isNumberKind() bool {
	switch t {
	case Int, Float, column:
		return true
	}

	return false
}

func (t Type) isIntKind() bool {
	switch t {
	case Int, column:
		return true
	}

	return false
}

func (t Type) isBoolKind() bool {
	switch t {
	case Bool, column:
		return true
	}

	return false
}

func (t Type) isStringKind() bool {
	switch t {
	case String, column:
		return true
	}

	return false
}
