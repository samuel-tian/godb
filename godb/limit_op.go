package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// TODO: some code goes here
    return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
    return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := l.child.Iterator(tid)
    if err != nil {
        return nil, err
    }

    var count int64 = 0
    return func() (*Tuple, error) {
        for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
            limit, _ := l.limitTups.EvalExpr(t)
            if count == limit.(IntField).Value {
                break
            }
            count++
            return t, nil
        }
        return nil, nil
    }, nil
}
