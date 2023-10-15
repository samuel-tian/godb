package godb

import (
    "golang.org/x/exp/constraints"
    _ "fmt"
)

type Filter[T constraints.Ordered] struct {
	op     BoolOp
	left   Expr
	right  Expr
	child  Operator
	getter func(DBValue) T
}

func intFilterGetter(v DBValue) int64 {
	intV := v.(IntField)
	return intV.Value
}

func stringFilterGetter(v DBValue) string {
	stringV := v.(StringField)
	return stringV.Value
}

// Constructor for a filter operator on ints
func NewIntFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[int64], error) {
	if constExpr.GetExprType().Ftype != IntType || field.GetExprType().Ftype != IntType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply int filter to non int-types"}
	}
	f, err := newFilter[int64](constExpr, op, field, child, intFilterGetter)
	return f, err
}

// Constructor for a filter operator on strings
func NewStringFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[string], error) {
	if constExpr.GetExprType().Ftype != StringType || field.GetExprType().Ftype != StringType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply string filter to non string-types"}
	}
	f, err := newFilter[string](constExpr, op, field, child, stringFilterGetter)
	return f, err
}

// Getter is a function that reads a value of the desired type
// from a field of a tuple
// This allows us to have a generic interface for filters that work
// with any ordered type
func newFilter[T constraints.Ordered](constExpr Expr, op BoolOp, field Expr, child Operator, getter func(DBValue) T) (*Filter[T], error) {
	return &Filter[T]{op, field, constExpr, child, getter}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter[T]) Descriptor() *TupleDesc {
	// TODO: some code goes here
    return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over
// the results of the child iterator and return a tuple if it satisfies
// the predicate.
// HINT: you can use the evalPred function defined in types.go to compare two values
func (f *Filter[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := f.child.Iterator(tid)
    if err != nil {
        return nil, err
    }

    return func() (*Tuple, error) {
        for {
            t, err := childIter()
            if err != nil {
                return nil, err
            }
            if t == nil {
                return nil, nil
            }

            left_eval, err := f.left.EvalExpr(t)
            if err != nil {
                return nil, err
            }
            right_eval, err := f.right.EvalExpr(t)
            if err != nil {
                return nil, err
            }

            left_val := f.getter(left_eval)
            right_val := f.getter(right_eval)
            if evalPred(left_val, right_val, f.op) {
                return t, nil
            }
        }
        return nil, nil
    }, nil
}
