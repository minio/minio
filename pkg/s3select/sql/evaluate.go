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
	"encoding/json"
	"errors"
	"strings"

	"github.com/bcicen/jstream"
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

func (e *AliasedExpression) evalNode(r Record) (*Value, error) {
	return e.Expression.evalNode(r)
}

func (e *Expression) evalNode(r Record) (*Value, error) {
	if len(e.And) == 1 {
		// In this case, result is not required to be boolean
		// type.
		return e.And[0].evalNode(r)
	}

	// Compute OR of conditions
	result := false
	for _, ex := range e.And {
		res, err := ex.evalNode(r)
		if err != nil {
			return nil, err
		}
		b, ok := res.ToBool()
		if !ok {
			return nil, errExpectedBool
		}
		result = result || b
	}
	return FromBool(result), nil
}

func (e *AndCondition) evalNode(r Record) (*Value, error) {
	if len(e.Condition) == 1 {
		// In this case, result does not have to be boolean
		return e.Condition[0].evalNode(r)
	}

	// Compute AND of conditions
	result := true
	for _, ex := range e.Condition {
		res, err := ex.evalNode(r)
		if err != nil {
			return nil, err
		}
		b, ok := res.ToBool()
		if !ok {
			return nil, errExpectedBool
		}
		result = result && b
	}
	return FromBool(result), nil
}

func (e *Condition) evalNode(r Record) (*Value, error) {
	if e.Operand != nil {
		// In this case, result does not have to be boolean
		return e.Operand.evalNode(r)
	}

	// Compute NOT of condition
	res, err := e.Not.evalNode(r)
	if err != nil {
		return nil, err
	}
	b, ok := res.ToBool()
	if !ok {
		return nil, errExpectedBool
	}
	return FromBool(!b), nil
}

func (e *ConditionOperand) evalNode(r Record) (*Value, error) {
	opVal, opErr := e.Operand.evalNode(r)
	if opErr != nil || e.ConditionRHS == nil {
		return opVal, opErr
	}

	// Need to evaluate the ConditionRHS
	switch {
	case e.ConditionRHS.Compare != nil:
		cmpRight, cmpRErr := e.ConditionRHS.Compare.Operand.evalNode(r)
		if cmpRErr != nil {
			return nil, cmpRErr
		}

		b, err := opVal.compareOp(e.ConditionRHS.Compare.Operator, cmpRight)
		return FromBool(b), err

	case e.ConditionRHS.Between != nil:
		return e.ConditionRHS.Between.evalBetweenNode(r, opVal)

	case e.ConditionRHS.Like != nil:
		return e.ConditionRHS.Like.evalLikeNode(r, opVal)

	case e.ConditionRHS.In != nil:
		return e.ConditionRHS.In.evalInNode(r, opVal)

	default:
		return nil, errInvalidASTNode
	}
}

func (e *Between) evalBetweenNode(r Record, arg *Value) (*Value, error) {
	stVal, stErr := e.Start.evalNode(r)
	if stErr != nil {
		return nil, stErr
	}

	endVal, endErr := e.End.evalNode(r)
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

func (e *Like) evalLikeNode(r Record, arg *Value) (*Value, error) {
	inferTypeAsString(arg)

	s, ok := arg.ToString()
	if !ok {
		err := errLikeNonStrArg
		return nil, errLikeInvalidInputs(err)
	}

	pattern, err1 := e.Pattern.evalNode(r)
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
		escapeVal, err2 := e.EscapeChar.evalNode(r)
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

func (e *In) evalInNode(r Record, arg *Value) (*Value, error) {
	result := false
	for _, elt := range e.Expressions {
		eltVal, err := elt.evalNode(r)
		if err != nil {
			return nil, err
		}

		// FIXME: type inference?

		// Types must match.
		if arg.vType != eltVal.vType {
			// match failed.
			continue
		}

		if arg.value == eltVal.value {
			result = true
			break
		}
	}
	return FromBool(result), nil
}

func (e *Operand) evalNode(r Record) (*Value, error) {
	lval, lerr := e.Left.evalNode(r)
	if lerr != nil || len(e.Right) == 0 {
		return lval, lerr
	}

	// Process remaining child nodes - result must be
	// numeric. This AST node is for terms separated by + or -
	// symbols.
	for _, rightTerm := range e.Right {
		op := rightTerm.Op
		rval, rerr := rightTerm.Right.evalNode(r)
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

func (e *MultOp) evalNode(r Record) (*Value, error) {
	lval, lerr := e.Left.evalNode(r)
	if lerr != nil || len(e.Right) == 0 {
		return lval, lerr
	}

	// Process other child nodes - result must be numeric. This
	// AST node is for terms separated by *, / or % symbols.
	for _, rightTerm := range e.Right {
		op := rightTerm.Op
		rval, rerr := rightTerm.Right.evalNode(r)
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

func (e *UnaryTerm) evalNode(r Record) (*Value, error) {
	if e.Negated == nil {
		return e.Primary.evalNode(r)
	}

	v, err := e.Negated.Term.evalNode(r)
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

func (e *JSONPath) evalNode(r Record) (*Value, error) {
	// Strip the table name from the keypath.
	keypath := e.String()
	ps := strings.SplitN(keypath, ".", 2)
	if len(ps) == 2 {
		keypath = ps[1]
	}
	objFmt, rawVal := r.Raw()
	switch objFmt {
	case SelectFmtJSON, SelectFmtParquet:
		rowVal := rawVal.(jstream.KVS)

		pathExpr := e.PathExpr
		if len(pathExpr) == 0 {
			pathExpr = []*JSONPathElement{{Key: &ObjectKey{ID: e.BaseKey}}}
		}

		result, err := jsonpathEval(pathExpr, rowVal)
		if err != nil {
			return nil, err
		}

		switch rval := result.(type) {
		case string:
			return FromString(rval), nil
		case float64:
			return FromFloat(rval), nil
		case int64:
			return FromInt(rval), nil
		case bool:
			return FromBool(rval), nil
		case jstream.KVS, []interface{}:
			bs, err := json.Marshal(result)
			if err != nil {
				return nil, err
			}
			return FromBytes(bs), nil
		case nil:
			return FromNull(), nil
		default:
			return nil, errors.New("Unhandled value type")
		}
	default:
		return r.Get(keypath)
	}
}

func (e *PrimaryTerm) evalNode(r Record) (res *Value, err error) {
	switch {
	case e.Value != nil:
		return e.Value.evalNode(r)
	case e.JPathExpr != nil:
		return e.JPathExpr.evalNode(r)
	case e.SubExpression != nil:
		return e.SubExpression.evalNode(r)
	case e.FuncCall != nil:
		return e.FuncCall.evalNode(r)
	}
	return nil, errInvalidASTNode
}

func (e *FuncExpr) evalNode(r Record) (res *Value, err error) {
	switch e.getFunctionName() {
	case aggFnCount, aggFnAvg, aggFnMax, aggFnMin, aggFnSum:
		return e.getAggregate()
	default:
		return e.evalSQLFnNode(r)
	}
}

// evalNode on a literal value is independent of the node being an
// aggregation or a row function - it always returns a value.
func (e *LitValue) evalNode(_ Record) (res *Value, err error) {
	switch {
	case e.Number != nil:
		return floatToValue(*e.Number), nil
	case e.String != nil:
		return FromString(string(*e.String)), nil
	case e.Boolean != nil:
		return FromBool(bool(*e.Boolean)), nil
	}
	return FromNull(), nil
}
