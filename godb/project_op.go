package godb

import (
    _ "fmt"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	//add additional fields here
    distinct bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
    if len(outputNames) != len(selectFields) {
        return nil, GoDBError{MalformedDataError, "field lengths not equal"}
    }
    return &Project{selectFields, outputNames, child, distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
    fts := []FieldType{}
    for i, expr := range p.selectFields {
        field := expr.GetExprType()
        field.Fname = p.outputNames[i]
        fts = append(fts, field)
    }
    td := TupleDesc{}
    td.Fields = fts
    return &td
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := p.child.Iterator(tid)
    if err != nil {
        return nil, err
    }

    return func() (*Tuple, error) {
        for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
            if err != nil {
                return nil, err
            }
            var ret Tuple
            ret.Desc = *p.Descriptor()
            var fields []DBValue
            for _, expr := range p.selectFields {
                eval, err := expr.EvalExpr(t)
                if err != nil {
                    return nil, err
                }
                fields = append(fields, eval)
            }
            ret.Fields = fields
            return &ret, nil
        }
        return nil, nil
    }, nil
}
