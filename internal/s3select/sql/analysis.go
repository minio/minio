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
	"fmt"
	"strings"
)

// Query analysis - The query is analyzed to determine if it involves
// aggregation.
//
// Aggregation functions - An expression that involves aggregation of
// rows in some manner. Requires all input rows to be processed,
// before a result is returned.
//
// Row function - An expression that depends on a value in the
// row. They have an output for each input row.
//
// Some types of a queries are not valid. For example, an aggregation
// function combined with a row function is meaningless ("AVG(s.Age) +
// s.Salary"). Analysis determines if such a scenario exists so an
// error can be returned.

var (
	// Fatal error for query processing.
	errNestedAggregation      = errors.New("Cannot nest aggregations")
	errFunctionNotImplemented = errors.New("Function is not yet implemented")
	errUnexpectedInvalidNode  = errors.New("Unexpected node value")
	errInvalidKeypath         = errors.New("A provided keypath is invalid")
)

// qProp contains analysis info about an SQL term.
type qProp struct {
	isAggregation, isRowFunc bool

	err error
}

// `combine` combines a pair of `qProp`s, so that errors are
// propagated correctly, and checks that an aggregation is not being
// combined with a row-function term.
func (p *qProp) combine(q qProp) {
	switch {
	case p.err != nil:
		// Do nothing
	case q.err != nil:
		p.err = q.err
	default:
		p.isAggregation = p.isAggregation || q.isAggregation
		p.isRowFunc = p.isRowFunc || q.isRowFunc
		if p.isAggregation && p.isRowFunc {
			p.err = errNestedAggregation
		}
	}
}

func (e *SelectExpression) analyze(s *Select) (result qProp) {
	if e.All {
		return qProp{isRowFunc: true}
	}

	for _, ex := range e.Expressions {
		result.combine(ex.analyze(s))
	}
	return result
}

func (e *AliasedExpression) analyze(s *Select) qProp {
	return e.Expression.analyze(s)
}

func (e *Expression) analyze(s *Select) (result qProp) {
	for _, ac := range e.And {
		result.combine(ac.analyze(s))
	}
	return result
}

func (e *AndCondition) analyze(s *Select) (result qProp) {
	for _, ac := range e.Condition {
		result.combine(ac.analyze(s))
	}
	return result
}

func (e *Condition) analyze(s *Select) (result qProp) {
	if e.Operand != nil {
		result = e.Operand.analyze(s)
	} else {
		result = e.Not.analyze(s)
	}
	return result
}

func (e *ListExpr) analyze(s *Select) (result qProp) {
	for _, ac := range e.Elements {
		result.combine(ac.analyze(s))
	}
	return result
}

func (e *ConditionOperand) analyze(s *Select) (result qProp) {
	if e.ConditionRHS == nil {
		result = e.Operand.analyze(s)
	} else {
		result.combine(e.Operand.analyze(s))
		result.combine(e.ConditionRHS.analyze(s))
	}
	return result
}

func (e *ConditionRHS) analyze(s *Select) (result qProp) {
	switch {
	case e.Compare != nil:
		result = e.Compare.Operand.analyze(s)
	case e.Between != nil:
		result.combine(e.Between.Start.analyze(s))
		result.combine(e.Between.End.analyze(s))
	case e.In != nil:
		result.combine(e.In.analyze(s))
	case e.Like != nil:
		result.combine(e.Like.Pattern.analyze(s))
		if e.Like.EscapeChar != nil {
			result.combine(e.Like.EscapeChar.analyze(s))
		}
	default:
		result = qProp{err: errUnexpectedInvalidNode}
	}
	return result
}

func (e *In) analyze(s *Select) (result qProp) {
	switch {
	case e.JPathExpr != nil:
		// Check if the path expression is valid
		if len(e.JPathExpr.PathExpr) > 0 {
			if e.JPathExpr.BaseKey.String() != s.From.As && !strings.EqualFold(e.JPathExpr.BaseKey.String(), baseTableName) {
				result = qProp{err: errInvalidKeypath}
				return result
			}
		}
		result = qProp{isRowFunc: true}
	case e.ListExpr != nil:
		result = e.ListExpr.analyze(s)
	default:
		result = qProp{err: errUnexpectedInvalidNode}
	}
	return result
}

func (e *Operand) analyze(s *Select) (result qProp) {
	result.combine(e.Left.analyze(s))
	for _, r := range e.Right {
		result.combine(r.Right.analyze(s))
	}
	return result
}

func (e *MultOp) analyze(s *Select) (result qProp) {
	result.combine(e.Left.analyze(s))
	for _, r := range e.Right {
		result.combine(r.Right.analyze(s))
	}
	return result
}

func (e *UnaryTerm) analyze(s *Select) (result qProp) {
	if e.Negated != nil {
		result = e.Negated.Term.analyze(s)
	} else {
		result = e.Primary.analyze(s)
	}
	return result
}

func (e *PrimaryTerm) analyze(s *Select) (result qProp) {
	switch {
	case e.Value != nil:
		result = qProp{}

	case e.JPathExpr != nil:
		// Check if the path expression is valid
		if len(e.JPathExpr.PathExpr) > 0 {
			if e.JPathExpr.BaseKey.String() != s.From.As && !strings.EqualFold(e.JPathExpr.BaseKey.String(), baseTableName) {
				result = qProp{err: errInvalidKeypath}
				return result
			}
		}
		result = qProp{isRowFunc: true}

	case e.ListExpr != nil:
		result = e.ListExpr.analyze(s)

	case e.SubExpression != nil:
		result = e.SubExpression.analyze(s)

	case e.FuncCall != nil:
		result = e.FuncCall.analyze(s)

	default:
		result = qProp{err: errUnexpectedInvalidNode}
	}
	return result
}

func (e *FuncExpr) analyze(s *Select) (result qProp) {
	funcName := e.getFunctionName()

	switch funcName {
	case sqlFnCast:
		return e.Cast.Expr.analyze(s)

	case sqlFnExtract:
		return e.Extract.From.analyze(s)

	case sqlFnDateAdd:
		result.combine(e.DateAdd.Quantity.analyze(s))
		result.combine(e.DateAdd.Timestamp.analyze(s))
		return result

	case sqlFnDateDiff:
		result.combine(e.DateDiff.Timestamp1.analyze(s))
		result.combine(e.DateDiff.Timestamp2.analyze(s))
		return result

	// Handle aggregation function calls
	case aggFnAvg, aggFnMax, aggFnMin, aggFnSum, aggFnCount:
		// Initialize accumulator
		e.aggregate = newAggVal(funcName)

		var exprA qProp
		if funcName == aggFnCount {
			if e.Count.StarArg {
				return qProp{isAggregation: true}
			}

			exprA = e.Count.ExprArg.analyze(s)
		} else {
			if len(e.SFunc.ArgsList) != 1 {
				return qProp{err: fmt.Errorf("%s takes exactly one argument", funcName)}
			}
			exprA = e.SFunc.ArgsList[0].analyze(s)
		}

		if exprA.err != nil {
			return exprA
		}
		if exprA.isAggregation {
			return qProp{err: errNestedAggregation}
		}
		return qProp{isAggregation: true}

	case sqlFnCoalesce:
		if len(e.SFunc.ArgsList) == 0 {
			return qProp{err: fmt.Errorf("%s needs at least one argument", string(funcName))}
		}
		for _, arg := range e.SFunc.ArgsList {
			result.combine(arg.analyze(s))
		}
		return result

	case sqlFnNullIf:
		if len(e.SFunc.ArgsList) != 2 {
			return qProp{err: fmt.Errorf("%s needs exactly 2 arguments", string(funcName))}
		}
		for _, arg := range e.SFunc.ArgsList {
			result.combine(arg.analyze(s))
		}
		return result

	case sqlFnCharLength, sqlFnCharacterLength:
		if len(e.SFunc.ArgsList) != 1 {
			return qProp{err: fmt.Errorf("%s needs exactly 2 arguments", string(funcName))}
		}
		for _, arg := range e.SFunc.ArgsList {
			result.combine(arg.analyze(s))
		}
		return result

	case sqlFnLower, sqlFnUpper:
		if len(e.SFunc.ArgsList) != 1 {
			return qProp{err: fmt.Errorf("%s needs exactly 2 arguments", string(funcName))}
		}
		for _, arg := range e.SFunc.ArgsList {
			result.combine(arg.analyze(s))
		}
		return result

	case sqlFnTrim:
		if e.Trim.TrimChars != nil {
			result.combine(e.Trim.TrimChars.analyze(s))
		}
		if e.Trim.TrimFrom != nil {
			result.combine(e.Trim.TrimFrom.analyze(s))
		}
		return result

	case sqlFnSubstring:
		errVal := fmt.Errorf("Invalid argument(s) to %s", string(funcName))
		result.combine(e.Substring.Expr.analyze(s))
		switch {
		case e.Substring.From != nil:
			result.combine(e.Substring.From.analyze(s))
			if e.Substring.For != nil {
				result.combine(e.Substring.Expr.analyze(s))
			}
		case e.Substring.Arg2 != nil:
			result.combine(e.Substring.Arg2.analyze(s))
			if e.Substring.Arg3 != nil {
				result.combine(e.Substring.Arg3.analyze(s))
			}
		default:
			result.err = errVal
		}
		return result

	case sqlFnUTCNow:
		if len(e.SFunc.ArgsList) != 0 {
			result.err = fmt.Errorf("%s() takes no arguments", string(funcName))
		}
		return result
	}

	// TODO: implement other functions
	return qProp{err: errFunctionNotImplemented}
}
