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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/minio/minio/internal/s3select/jstream"
	"github.com/minio/simdjson-go"
)

var (
	errInvalidASTNode    = errors.New("invalid AST Node")
	errExpectedBool      = errors.New("expected bool")
	errLikeNonStrArg     = errors.New("LIKE clause requires string arguments")
	errLikeInvalidEscape = errors.New("LIKE clause has invalid ESCAPE character")
	errNotImplemented    = errors.New("not implemented")
)

// AST Node Evaluation functions
//
// During evaluation, the query is known to be valid, as analysis is
// complete. The only errors possible are due to value type
// mismatches, etc.
//
// If an aggregation node is present as a descendant (when
// e.prop.isAggregation is true), we call evalNode on all child nodes,
// check for errors, but do not perform any combining of the results
// of child nodes. The final result row is returned after all rows are
// processed, and the `getAggregate` function is called.

func (e *AliasedExpression) evalNode(r Record, tableAlias string) (*Value, error) {
	return e.Expression.evalNode(r, tableAlias)
}

func (e *Expression) evalNode(r Record, tableAlias string) (*Value, error) {
	if len(e.And) == 1 {
		// In this case, result is not required to be boolean
		// type.
		return e.And[0].evalNode(r, tableAlias)
	}

	// Compute OR of conditions
	result := false
	for _, ex := range e.And {
		res, err := ex.evalNode(r, tableAlias)
		if err != nil {
			return nil, err
		}
		b, ok := res.ToBool()
		if !ok {
			return nil, errExpectedBool
		}
		if b {
			return FromBool(true), nil
		}
		result = result || b
	}
	return FromBool(result), nil
}

func (e *AndCondition) evalNode(r Record, tableAlias string) (*Value, error) {
	if len(e.Condition) == 1 {
		// In this case, result does not have to be boolean
		return e.Condition[0].evalNode(r, tableAlias)
	}

	// Compute AND of conditions
	result := true
	for _, ex := range e.Condition {
		res, err := ex.evalNode(r, tableAlias)
		if err != nil {
			return nil, err
		}
		b, ok := res.ToBool()
		if !ok {
			return nil, errExpectedBool
		}
		if !b {
			return FromBool(false), nil
		}
		result = result && b
	}
	return FromBool(result), nil
}

func (e *Condition) evalNode(r Record, tableAlias string) (*Value, error) {
	if e.Operand != nil {
		// In this case, result does not have to be boolean
		return e.Operand.evalNode(r, tableAlias)
	}

	// Compute NOT of condition
	res, err := e.Not.evalNode(r, tableAlias)
	if err != nil {
		return nil, err
	}
	b, ok := res.ToBool()
	if !ok {
		return nil, errExpectedBool
	}
	return FromBool(!b), nil
}

func (e *ConditionOperand) evalNode(r Record, tableAlias string) (*Value, error) {
	opVal, opErr := e.Operand.evalNode(r, tableAlias)
	if opErr != nil || e.ConditionRHS == nil {
		return opVal, opErr
	}

	// Need to evaluate the ConditionRHS
	switch {
	case e.ConditionRHS.Compare != nil:
		cmpRight, cmpRErr := e.ConditionRHS.Compare.Operand.evalNode(r, tableAlias)
		if cmpRErr != nil {
			return nil, cmpRErr
		}

		b, err := opVal.compareOp(strings.ToUpper(e.ConditionRHS.Compare.Operator), cmpRight)
		return FromBool(b), err

	case e.ConditionRHS.Between != nil:
		return e.ConditionRHS.Between.evalBetweenNode(r, opVal, tableAlias)

	case e.ConditionRHS.Like != nil:
		return e.ConditionRHS.Like.evalLikeNode(r, opVal, tableAlias)

	case e.ConditionRHS.In != nil:
		return e.ConditionRHS.In.evalInNode(r, opVal, tableAlias)

	default:
		return nil, errInvalidASTNode
	}
}

func (e *Between) evalBetweenNode(r Record, arg *Value, tableAlias string) (*Value, error) {
	stVal, stErr := e.Start.evalNode(r, tableAlias)
	if stErr != nil {
		return nil, stErr
	}

	endVal, endErr := e.End.evalNode(r, tableAlias)
	if endErr != nil {
		return nil, endErr
	}

	part1, err1 := stVal.compareOp(opLte, arg)
	if err1 != nil {
		return nil, err1
	}

	part2, err2 := arg.compareOp(opLte, endVal)
	if err2 != nil {
		return nil, err2
	}

	result := part1 && part2
	if e.Not {
		result = !result
	}

	return FromBool(result), nil
}

func (e *Like) evalLikeNode(r Record, arg *Value, tableAlias string) (*Value, error) {
	inferTypeAsString(arg)

	s, ok := arg.ToString()
	if !ok {
		err := errLikeNonStrArg
		return nil, errLikeInvalidInputs(err)
	}

	pattern, err1 := e.Pattern.evalNode(r, tableAlias)
	if err1 != nil {
		return nil, err1
	}

	// Infer pattern as string (in case it is untyped)
	inferTypeAsString(pattern)

	patternStr, ok := pattern.ToString()
	if !ok {
		err := errLikeNonStrArg
		return nil, errLikeInvalidInputs(err)
	}

	escape := runeZero
	if e.EscapeChar != nil {
		escapeVal, err2 := e.EscapeChar.evalNode(r, tableAlias)
		if err2 != nil {
			return nil, err2
		}

		inferTypeAsString(escapeVal)

		escapeStr, ok := escapeVal.ToString()
		if !ok {
			err := errLikeNonStrArg
			return nil, errLikeInvalidInputs(err)
		}

		if len([]rune(escapeStr)) > 1 {
			err := errLikeInvalidEscape
			return nil, errLikeInvalidInputs(err)
		}
	}

	matchResult, err := evalSQLLike(s, patternStr, escape)
	if err != nil {
		return nil, err
	}

	if e.Not {
		matchResult = !matchResult
	}

	return FromBool(matchResult), nil
}

func (e *ListExpr) evalNode(r Record, tableAlias string) (*Value, error) {
	res := make([]Value, len(e.Elements))
	if len(e.Elements) == 1 {
		// If length 1, treat as single value.
		return e.Elements[0].evalNode(r, tableAlias)
	}
	for i, elt := range e.Elements {
		v, err := elt.evalNode(r, tableAlias)
		if err != nil {
			return nil, err
		}
		res[i] = *v
	}
	return FromArray(res), nil
}

const floatCmpTolerance = 0.000001

func (e *In) evalInNode(r Record, lhs *Value, tableAlias string) (*Value, error) {
	// Compare two values in terms of in-ness.
	var cmp func(a, b Value) bool
	cmp = func(a, b Value) bool {
		// Convert if needed.
		inferTypesForCmp(&a, &b)

		if a.Equals(b) {
			return true
		}

		// If elements, compare each.
		aA, aOK := a.ToArray()
		bA, bOK := b.ToArray()
		if aOK && bOK {
			if len(aA) != len(bA) {
				return false
			}
			for i := range aA {
				if !cmp(aA[i], bA[i]) {
					return false
				}
			}
			return true
		}
		// Try as numbers
		aF, aOK := a.ToFloat()
		bF, bOK := b.ToFloat()

		diff := math.Abs(aF - bF)
		return aOK && bOK && diff < floatCmpTolerance
	}

	var rhs Value
	var err error
	var eltVal *Value
	switch {
	case e.JPathExpr != nil:
		eltVal, err = e.JPathExpr.evalNode(r, tableAlias)
		if err != nil {
			return nil, err
		}
	case e.ListExpr != nil:
		eltVal, err = e.ListExpr.evalNode(r, tableAlias)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errInvalidASTNode
	}
	rhs = *eltVal

	// If RHS is array compare each element.
	if arr, ok := rhs.ToArray(); ok {
		for _, element := range arr {
			// If we have an array we are on the wrong level.
			if cmp(element, *lhs) {
				return FromBool(true), nil
			}
		}
		return FromBool(false), nil
	}

	return FromBool(cmp(rhs, *lhs)), nil
}

func (e *Operand) evalNode(r Record, tableAlias string) (*Value, error) {
	lval, lerr := e.Left.evalNode(r, tableAlias)
	if lerr != nil || len(e.Right) == 0 {
		return lval, lerr
	}

	// Process remaining child nodes - result must be
	// numeric. This AST node is for terms separated by + or -
	// symbols.
	for _, rightTerm := range e.Right {
		op := rightTerm.Op
		rval, rerr := rightTerm.Right.evalNode(r, tableAlias)
		if rerr != nil {
			return nil, rerr
		}
		err := lval.arithOp(op, rval)
		if err != nil {
			return nil, err
		}
	}
	return lval, nil
}

func (e *MultOp) evalNode(r Record, tableAlias string) (*Value, error) {
	lval, lerr := e.Left.evalNode(r, tableAlias)
	if lerr != nil || len(e.Right) == 0 {
		return lval, lerr
	}

	// Process other child nodes - result must be numeric. This
	// AST node is for terms separated by *, / or % symbols.
	for _, rightTerm := range e.Right {
		op := rightTerm.Op
		rval, rerr := rightTerm.Right.evalNode(r, tableAlias)
		if rerr != nil {
			return nil, rerr
		}

		err := lval.arithOp(op, rval)
		if err != nil {
			return nil, err
		}
	}
	return lval, nil
}

func (e *UnaryTerm) evalNode(r Record, tableAlias string) (*Value, error) {
	if e.Negated == nil {
		return e.Primary.evalNode(r, tableAlias)
	}

	v, err := e.Negated.Term.evalNode(r, tableAlias)
	if err != nil {
		return nil, err
	}

	inferTypeForArithOp(v)
	v.negate()
	if v.isNumeric() {
		return v, nil
	}
	return nil, errArithMismatchedTypes
}

func (e *JSONPath) evalNode(r Record, tableAlias string) (*Value, error) {
	alias := tableAlias
	if tableAlias == "" {
		alias = baseTableName
	}
	pathExpr := e.StripTableAlias(alias)
	_, rawVal := r.Raw()
	switch rowVal := rawVal.(type) {
	case jstream.KVS, simdjson.Object:
		if len(pathExpr) == 0 {
			pathExpr = []*JSONPathElement{{Key: &ObjectKey{ID: e.BaseKey}}}
		}

		result, _, err := jsonpathEval(pathExpr, rowVal)
		if err != nil {
			return nil, err
		}

		return jsonToValue(result)
	default:
		if pathExpr[len(pathExpr)-1].Key == nil {
			return nil, errInvalidKeypath
		}
		return r.Get(pathExpr[len(pathExpr)-1].Key.keyString())
	}
}

// jsonToValue will convert the json value to an internal value.
func jsonToValue(result any) (*Value, error) {
	switch rval := result.(type) {
	case string:
		return FromString(rval), nil
	case float64:
		return FromFloat(rval), nil
	case int64:
		return FromInt(rval), nil
	case uint64:
		if rval <= math.MaxInt64 {
			return FromInt(int64(rval)), nil
		}
		return FromFloat(float64(rval)), nil
	case bool:
		return FromBool(rval), nil
	case jstream.KVS:
		bs, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		return FromBytes(bs), nil
	case []any:
		dst := make([]Value, len(rval))
		for i := range rval {
			v, err := jsonToValue(rval[i])
			if err != nil {
				return nil, err
			}
			dst[i] = *v
		}
		return FromArray(dst), nil
	case simdjson.Object:
		o := rval
		elems, err := o.Parse(nil)
		if err != nil {
			return nil, err
		}
		bs, err := elems.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return FromBytes(bs), nil
	case []Value:
		return FromArray(rval), nil
	case nil:
		return FromNull(), nil
	case Missing:
		return FromMissing(), nil
	}
	return nil, fmt.Errorf("Unhandled value type: %T", result)
}

func (e *PrimaryTerm) evalNode(r Record, tableAlias string) (res *Value, err error) {
	switch {
	case e.Value != nil:
		return e.Value.evalNode(r)
	case e.JPathExpr != nil:
		return e.JPathExpr.evalNode(r, tableAlias)
	case e.ListExpr != nil:
		return e.ListExpr.evalNode(r, tableAlias)
	case e.SubExpression != nil:
		return e.SubExpression.evalNode(r, tableAlias)
	case e.FuncCall != nil:
		return e.FuncCall.evalNode(r, tableAlias)
	}
	return nil, errInvalidASTNode
}

func (e *FuncExpr) evalNode(r Record, tableAlias string) (res *Value, err error) {
	switch e.getFunctionName() {
	case aggFnCount, aggFnAvg, aggFnMax, aggFnMin, aggFnSum:
		return e.getAggregate()
	default:
		return e.evalSQLFnNode(r, tableAlias)
	}
}

// evalNode on a literal value is independent of the node being an
// aggregation or a row function - it always returns a value.
func (e *LitValue) evalNode(_ Record) (res *Value, err error) {
	switch {
	case e.Int != nil:
		if *e.Int < math.MaxInt64 && *e.Int > math.MinInt64 {
			return FromInt(int64(*e.Int)), nil
		}
		return FromFloat(*e.Int), nil
	case e.Float != nil:
		return FromFloat(*e.Float), nil
	case e.String != nil:
		return FromString(string(*e.String)), nil
	case e.Boolean != nil:
		return FromBool(bool(*e.Boolean)), nil
	case e.Missing:
		return FromMissing(), nil
	}
	return FromNull(), nil
}
