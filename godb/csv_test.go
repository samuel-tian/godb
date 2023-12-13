package godb

import (
	"os"
	"testing"
  "fmt"
  "time"
)

func TestLoadCSVColumn(t *testing.T) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: StringType},
		{Fname: "b", Ftype: StringType},
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: StringType},
		{Fname: "e", Ftype: StringType},
		{Fname: "f", Ftype: StringType},
		{Fname: "g", Ftype: StringType},
		{Fname: "h", Ftype: StringType},
		{Fname: "i", Ftype: StringType},
		{Fname: "j", Ftype: StringType},
		{Fname: "k", Ftype: StringType},
		{Fname: "l", Ftype: StringType},
	}}
	bp := NewBufferPool(100)
  files := make([]string, 13)
  for i := 0; i < 13; i++ {
    files[i] = fmt.Sprintf("%d.dat", i)
    os.Remove(files[i])
  }
	cf, err := NewColumnFile(files, td, bp)
	tid := NewTID()
	bp.BeginTransaction(tid)

	f, err := os.Open("test_column_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_column_file.csv")
		return
	}
	err = cf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}
  start := time.Now()
	iter, _ := cf.IteratorColumns([]int{5}, tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
  cur := time.Now()
  elapsed := cur.Sub(start)
  fmt.Printf("column took %v seconds\n", elapsed)
}

func TestLoadCSVHeap(t *testing.T) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: StringType},
		{Fname: "b", Ftype: StringType},
		{Fname: "c", Ftype: StringType},
		{Fname: "d", Ftype: StringType},
		{Fname: "e", Ftype: StringType},
		{Fname: "f", Ftype: StringType},
		{Fname: "g", Ftype: StringType},
		{Fname: "h", Ftype: StringType},
		{Fname: "i", Ftype: StringType},
		{Fname: "j", Ftype: StringType},
		{Fname: "k", Ftype: StringType},
		{Fname: "l", Ftype: StringType},
	}}
	bp := NewBufferPool(100)
  file := "heap_csv.dat"
  os.Remove(file)
	cf, err := NewHeapFile(file, &td, bp)
	tid := NewTID()
	bp.BeginTransaction(tid)

	f, err := os.Open("test_column_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_column_file.csv")
		return
	}
	err = cf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}
  start := time.Now()
  iter, _ := cf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
  cur := time.Now()
  elapsed := cur.Sub(start)
  fmt.Printf("heap took %v seconds\n", elapsed)
}
