package godb

import (
	"fmt"
	"testing"
  "os"
)

func TestIntFilter(t *testing.T) {
  fmt.Sprintf("use fmt")
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewIntFilter(&ConstExpr{IntField{25}, IntType}, OpGt, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

func TestStringFilter(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"name", "", StringType}
	filt, err := NewStringFilter(&ConstExpr{StringField{"sam"}, StringType}, OpEq, &FieldExpr{f}, hf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

const JoinTestFile string = "JoinTestFile.dat"
const JoinTestFileCol2 string = "JoinTestFile2.dat"

func TestJoin(t *testing.T) {
	td, t1, t2, hf, bp, tid := makeTestVars()
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t2, tid)

	os.Remove(JoinTestFile)
  os.Remove(JoinTestFileCol2)
	hf2, err := NewColumnFile([]string{JoinTestFile, JoinTestFileCol2}, td, bp)
	hf2.insertTuple(&t1, tid)
	hf2.insertTuple(&t2, tid)
	hf2.insertTuple(&t2, tid)

	outT1 := joinTuples(&t1, &t1)
	outT2 := joinTuples(&t2, &t2)

	leftField := FieldExpr{td.Fields[1]}
	join, err := NewIntJoin(hf, &leftField, hf2, &leftField, 100)
	if err != nil {
		t.Errorf("unexpected error initializing join")
		return
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	cntOut1 := 0
	cntOut2 := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		if t.equals(outT1) {
			cntOut1++
		} else if t.equals(outT2) {
			cntOut2++
		}
		cnt++
	}
	if cnt != 5 {
		t.Errorf("unexpected number of join results (%d, expected 5)", cnt)
	}
	if cntOut1 != 1 {
		t.Errorf("unexpected number of t1 results (%d, expected 1)", cntOut1)
	}
	if cntOut2 != 4 {
		t.Errorf("unexpected number of t2 results (%d, expected 4)", cntOut2)
	}

}
