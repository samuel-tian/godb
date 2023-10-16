package godb

import (
    "sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
    ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
    if len(orderByFields) != len(ascending) {
        return nil, GoDBError{MalformedDataError, "field lengths not equal"}
    }
    return &OrderBy{orderByFields, child, ascending}, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
    return o.child.Descriptor()
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := o.child.Iterator(tid)
    if err != nil {
        return nil, err
    }
    var tuples []*Tuple
    for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
        if err != nil {
            return nil, err
        }
        tuples = append(tuples, t)
    }

    sort.Slice(tuples, func(i, j int) bool {
        for k, expr := range o.orderBy {
            eval_i, _ := expr.EvalExpr(tuples[i])
            eval_j, _ := expr.EvalExpr(tuples[j])
            switch expr.GetExprType().Ftype {
            case IntType:
                val_i := eval_i.(IntField).Value
                val_j := eval_j.(IntField).Value
                if val_i == val_j {continue}
                if o.ascending[k] {
                    return val_i < val_j
                } else {
                    return val_i > val_j
                }
            case StringType:
                val_i := eval_i.(StringField).Value
                val_j := eval_j.(StringField).Value
                if val_i == val_j {continue}
                if o.ascending[k] {
                    return val_i < val_j
                } else {
                    return val_i > val_j
                }
            }
        }
        return true
    })
    ind := 0
    return func() (*Tuple, error) {
        if ind == len(tuples) {
            return nil, err
        }
        ind++
        return tuples[ind-1], nil
    }, nil
}
