package godb

import (
	"os"
	"testing"
  "fmt"
)

const TestingFileC1 string = "testName.dat"
const TestingFileC2 string = "testAge.dat"

func makeTestVars() (TupleDesc, Tuple, Tuple, *ColumnFile, *BufferPool, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{999},
		}}

	bp := NewBufferPool(10)
	os.Remove(TestingFileC1)
  os.Remove(TestingFileC2)
	cf, err := NewColumnFile([]string{TestingFileC1, TestingFileC2}, td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, t1, t2, cf, bp, tid

}

func TestInsertColumnFile(t *testing.T) {
	_, t1, t2, cf, _, tid := makeTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
  iter, _ := cf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("HeapFile iterator expected 2 tuples, got %d", i)
	}
}

func TestCreateColumnFile(t *testing.T) {
  makeTestVars()
  fmt.Sprintf("using fmt")
}

