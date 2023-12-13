package godb

import (
  "testing"
  "fmt"
  _ "os"
)

func TestInsertDeleteColumnPage(t *testing.T) {
  td, t1, t2, cf, _, _ := makeTestVars()
  pgName := newColumnPage(&td, 0, 0, cf)
  pgAge := newColumnPage(&td, 1, 0, cf)

  var expectedNameSlots = ((4096 - 8) / (StringLength))
  var expectedAgeSlots = ((4096 - 8) / 8)

  if pgName.getNumSlots() != expectedNameSlots {
    t.Fatalf("incorrect number of slots")
  }
  if pgAge.getNumSlots() != expectedAgeSlots {
    t.Fatalf("incorrect number of slots")
  }

  pgName.insertTuple(&t1)
  rid, _ := pgName.insertTuple(&t2)

  iter := pgName.tupleIter()
  cnt := 0
  for {
    tup, _ := iter()
    if tup == nil {
      break
    }
    cnt++
  }
  if cnt != 2 {
    t.Errorf("Expected 2 tuples in iterator, got %d", cnt)
  }

  pgName.deleteTuple(rid)
  iter = pgName.tupleIter()
  cnt = 0
  for {
    tup, _ := iter()
    if tup == nil {
      break
    }
    cnt++
  }
  if cnt != 1 {
    t.Errorf("Expected 2 tuples in iterator, got %d", cnt)
  }

}

func TestColumnPageInsertTuple(t *testing.T) {
  td, t1, _, cf, _, _ := makeTestVars()
  page := newColumnPage(&td, 0, 0, cf)
  free := page.getNumSlots()

  for i := 0; i < free; i++ {
    var addition = Tuple {
      Desc: td,
      Fields: []DBValue{
        StringField{"sam"},
        IntField{int64(i)},
      },
    }
    page.insertTuple(&addition)

		iter := page.tupleIter()
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		cnt, found := 0, false
		for {

			tup, _ := iter()
      fields := []FieldType{td.Fields[0]}
      additionProjected, _ := addition.project(fields)
			found = found || additionProjected.equals(tup)
			if tup == nil {
				break
			}

			cnt += 1
		}
		if cnt != i+1 {
			t.Errorf("Expected %d tuple in interator, got %d", i+1, cnt)
		}
		if !found {
			t.Errorf("Expected inserted tuple to be FOUND, got NOT FOUND")
		}
  }
	_, err := page.insertTuple(&t1)

	if err == nil {
		t.Errorf("Expected error due to full page")
	}
}

func TestColumnPageDeleteTuple(t *testing.T) {
	td, _, _, cf, _, _ := makeTestVars()
	page := newColumnPage(&td, 0, 0, cf)
	free := page.getNumSlots()

	list := make([]recordID, free)
	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		list[i], _ = page.insertTuple(&addition)
	}
	if len(list) == 0 {
		t.Fatalf("Rid list is empty.")
	}

	for _, rid := range list {
		err := page.deleteTuple(rid)
		if err != nil {
			t.Errorf("Found error %s", err.Error())
		}
	}

	err := page.deleteTuple(list[0])
	if err == nil {
		t.Errorf("page should be empty; expected error")
	}
}

func TestColumnPageSerialization(t *testing.T) {

	td, _, _, cf, _, _ := makeTestVars()
	page := newColumnPage(&td, 0, 0, cf)
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()
	page2 := newColumnPage(&td, 0, 0, cf)
	err := page2.initFromBuffer(buf)
	if err != nil {
		t.Fatalf("Error loading heap page from buffer.")
	}

	iter, iter2 := page.tupleIter(), page2.tupleIter()
	if iter == nil {
		t.Fatalf("iter was nil.")
	}
	if iter2 == nil {
		t.Fatalf("iter2 was nil.")
	}

	findEqCount := func(t0 *Tuple, iter3 func() (*Tuple, error)) int {
		cnt := 0
		for tup, _ := iter3(); tup != nil; tup, _ = iter3() {
			if t0.equals(tup) {
				cnt += 1
			}
		}
		return cnt
	}

	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if findEqCount(tup, page.tupleIter()) != findEqCount(tup, page2.tupleIter()) {
			t.Errorf("Serialization / deserialization doesn't result in identical heap page.")
		}
	}
}

func use_fmt() {
  fmt.Sprintf("using fmt")
}
