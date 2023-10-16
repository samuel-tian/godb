package godb

type DeleteOp struct {
	// TODO: some code goes here
    file DBFile
    child Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
    return &DeleteOp{deleteFile, child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
    ft := FieldType{"count", "", IntType}
    fts := []FieldType{ft}
    td := TupleDesc{}
    td.Fields = fts
    return &td
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := dop.child.Iterator(tid)
    if err != nil {
        return nil, err
    }
    count := 0;
    for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
        if err != nil {
            return nil, err
        }
        dop.file.deleteTuple(t, tid)
        count++
    }

    return func() (*Tuple, error) {
        ret := Tuple{}
        ret.Desc = *dop.Descriptor()
        ret.Fields = []DBValue{}
        ret.Fields = append(ret.Fields, IntField{int64(count)})
        return &ret, nil
    }, nil

}
