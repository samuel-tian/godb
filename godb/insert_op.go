package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
    file DBFile
    child Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
    return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
    ft := FieldType{"count", "", IntType}
    fts := []FieldType{ft}
    td := TupleDesc{}
    td.Fields = fts
    return &td
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
    childIter, err := iop.child.Iterator(tid)
    if err != nil {
        return nil, err
    }
    count := 0
    for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
        if err != nil {
            return nil, err
        }
        iop.file.insertTuple(t, tid)
        count++
    }

    return func() (*Tuple, error) {
        ret := Tuple{}
        ret.Desc = *iop.Descriptor()
        ret.Fields = []DBValue{}
        ret.Fields = append(ret.Fields, IntField{int64(count)})
        return &ret, nil
    }, nil

}
