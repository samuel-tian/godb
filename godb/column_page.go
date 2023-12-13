package godb

import (
  "unsafe"
  "errors"
  "fmt"
  "bytes"
  "encoding/binary"
)

type columnPage struct {
  columnFile *ColumnFile
  desc *TupleDesc // contains 1 descriptor just for this column
  dirty bool
  columnNo int32
  pageNo int32

  // header
  numSlots int32
  numUsedSlots int32
  // in total, header is 8 bytes

  tuples [](*Tuple)
}

func (c *columnPage) getNumSlots() (int) {
  return (int)(c.numSlots)
}

func (c *columnPage) isDirty() bool {
  return c.dirty
}

func (c *columnPage) setDirty(dirty bool) {
  c.dirty = dirty
}

func (c *columnPage) getFile() *DBFile {
  var f DBFile = c.columnFile
  return &f
}

func newColumnPage(desc *TupleDesc, columnNo int, pageNo int, f *ColumnFile) *columnPage {
  ret := new(columnPage)

  ret.columnFile = f
  ret.desc = &TupleDesc{Fields : []FieldType{desc.Fields[columnNo]}}
  ret.dirty = false

  var tupleSize int32 = 0
  field := desc.Fields[columnNo]
  switch field.Ftype {
  case IntType:
    tupleSize = (int32) (unsafe.Sizeof(int64(0)))
  case StringType:
    tupleSize = (int32) (unsafe.Sizeof(byte('a'))) *
                (int32) (StringLength)
  }

  headerSize := 8
  var numSlots int32 = (int32) (PageSize - headerSize) / tupleSize
  ret.numSlots = numSlots
  ret.numUsedSlots = 0
  ret.columnNo = (int32) (columnNo)
  ret.pageNo = (int32) (pageNo)

  tuples := make([](*Tuple), numSlots)
  for i, _ := range tuples {
    tuples[i] = nil
  }
  ret.tuples = tuples

  return ret
}

func (c *columnPage) insertTuple(t *Tuple) (recordID, error) {
  if c.numUsedSlots == c.numSlots {
    return nil, errors.New("page is full")
  }
  tupleToInsert, _ := t.project(c.desc.Fields)

  for i, tup := range c.tuples {
    if tup == nil {
      c.tuples[i] = tupleToInsert
      c.numUsedSlots++
      c.setDirty(true)
      return i, nil
    }
  }

  return nil, errors.New(fmt.Sprintf("should not reach here %d %d",
                                      c.numUsedSlots, c.numSlots))
}

func (c *columnPage) deleteTuple(rid recordID) error {
  switch rid := rid.(type) {
  case int:
    if c.tuples[rid] == nil {
      return errors.New("tuple to delete does not exist in page")
    }
    c.tuples[rid] = nil
    c.numUsedSlots--
    c.setDirty(true)
    return nil
  default:
    return errors.New("invalid record ID")
  }
}

func (c *columnPage) tupleIter() func() (*Tuple, error) {
  rid := 0
  return func() (*Tuple, error) {
    for rid < (int)(c.numSlots) && c.tuples[rid] == nil {
      rid++
    }
    if rid == (int)(c.numSlots) {
      return nil, nil
    } else {
      ret := c.tuples[rid]
      rid++
      return ret, nil
    }
  }
}

func (c *columnPage) toBuffer() (*bytes.Buffer, error) {
  buf := new(bytes.Buffer)
  err := binary.Write(buf, binary.LittleEndian, c.numSlots)
  if err != nil {
    return nil, err
  }
  err = binary.Write(buf, binary.LittleEndian, c.numUsedSlots)
  if err != nil {
    return nil, err
  }

  for _, t := range c.tuples {
    if t == nil {
      continue
    }
    err = t.writeTo(buf)
    if err != nil {
      return nil, err
    }
  }
  return buf, nil
}

func (c *columnPage) initFromBuffer(buf *bytes.Buffer) error {
  err := binary.Read(buf, binary.LittleEndian, &c.numSlots)
  if err != nil {
    return nil
  }
  var numUsedSlots int32
  err = binary.Read(buf, binary.LittleEndian, &numUsedSlots)
  if err != nil {
    return nil
  }
  c.tuples = make([](*Tuple), c.numSlots)
  for i, _ := range c.tuples {
    c.tuples[i] = nil
  }
  for i := 0; i < (int)(numUsedSlots); i++ {
    tup, err := readTupleFrom(buf, c.desc)
    if err != nil {
      return nil
    }
    c.insertTuple(tup)
  }
  return nil
}
