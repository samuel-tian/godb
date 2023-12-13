package godb

import (
  "fmt"
  "os"
  "errors"
  "bytes"
  "sync"

  "bufio"
  "strings"
  "strconv"
)

type ColumnFile struct {
  bufPool *BufferPool
  filenames []string
  td TupleDesc

  numPagesPerColumn int
  numColumns int

  columnFileLock sync.Mutex
}

func NewColumnFile(fromFiles []string, td TupleDesc, bp *BufferPool) (*ColumnFile, error) {
  ret := new(ColumnFile)
  ret.bufPool = bp
  ret.td = td
  ret.filenames = fromFiles

  ret.numColumns = len(td.Fields)
  if ret.numColumns != len(fromFiles) {
    return nil, errors.New("number of files and number of columns is incompatible")
  }

  for _, filename := range ret.filenames {
    file, err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR, 0755)
    if err != nil {
      return nil, err
    }
    defer file.Close()
    fi, err := file.Stat()
    if err != nil {
      return nil, err
    }

    var size int = (int)(fi.Size())
    totalPages := (size + PageSize - 1) / PageSize
    ret.numPagesPerColumn = totalPages / ret.numColumns

    break
  }

  return ret, nil
}

func (f *ColumnFile) NumPages() int {
  return f.numPagesPerColumn * f.numColumns
}

func (f *ColumnFile) insertTuple(t *Tuple, tid TransactionID) error {
  j := 0

  // inserting tuple into first column
  pageInserted := false
  for i := 0; i < f.numPagesPerColumn; i++ {
    pageNo := i * f.numColumns + j
    // _, ok := f.bufPool.pages[f.pageKey(pageNo)]
    // if !ok { // first search through pages in buffer pool
    //   continue
    // }
    page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
    if err != nil {
      return err
    }
    cp := (*page).(*columnPage)
    slot, err := cp.insertTuple(t)
    if err == nil {
      pageInserted = true
      t.Rid = RecordID{pageNo : pageNo, slotNo : slot.(int)}

      for k := 1; k < f.numColumns; k++ { // insert into rest of columns
        pageNo = i * f.numColumns + k
        page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
        if err != nil {
          return err
        }
        cp = (*page).(*columnPage)
        cp.insertTuple(t)
      }

      break
    }
  }


  if !pageInserted {
    // append new page to each column
    f.columnFileLock.Lock()
    defer f.columnFileLock.Unlock()

    newPageNo := f.numPagesPerColumn * f.numColumns + j
    page := newColumnPage(&f.td, j, newPageNo, f)
    var p Page = page
    f.numPagesPerColumn++
    f.flushPage(&p)

    bufPoolPage, err := f.bufPool.GetPage(f, newPageNo, tid, WritePerm)
    if err != nil {
      return err
    }

    slot, err := ((*bufPoolPage).(*columnPage)).insertTuple(t)
    t.Rid = RecordID{pageNo : newPageNo, slotNo : slot.(int)}
    if err != nil {
      return err
    }

    for k := 1; k < f.numColumns; k++ {
      newPageNo = (f.numPagesPerColumn - 1) * f.numColumns + k
      page = newColumnPage(&f.td, k, newPageNo, f)
      p = page
      f.flushPage(&p)

      bufPoolPage, err = f.bufPool.GetPage(f, newPageNo, tid, WritePerm)
      if err != nil {
        return err
      }

      slot, err = ((*bufPoolPage).(*columnPage)).insertTuple(t)
      if err != nil {
        return err
      }
    }
  }
  return nil
}

func (f *ColumnFile) deleteTuple(t *Tuple, tid TransactionID) error {
  rid := t.Rid.(RecordID)
  startPageNo := rid.pageNo
  for i := 0; i < f.numColumns; i++ {
    page, err := f.bufPool.GetPage(f, startPageNo + i, tid, WritePerm)
    if err != nil {
      return err
    }
    cp := (*page).(*columnPage)
    err = cp.deleteTuple(rid.slotNo)
    if err != nil {
      return err
    }
  }
  return nil
}

func (f *ColumnFile) readPage(pageNo int) (*Page, error) {
  column := pageNo % f.numColumns
  slotInColumn := pageNo / f.numColumns
  filename := f.filenames[column]
  file, err := os.Open(filename)
  if err != nil {
    return nil, err
  }

  _, err = file.Seek((int64)(PageSize * slotInColumn), 0)
  if err != nil {
    return nil, err
  }
  b := make([]byte, PageSize)
  _, err = file.Read(b)
  if err != nil {
    return nil, err
  }
  buf := bytes.NewBuffer(b)

  cp := newColumnPage(&f.td, column, pageNo, f)
  err = cp.initFromBuffer(buf)
  if err != nil {
    return nil, err
  }
  cp.setDirty(false)

  var p Page = cp
  return &p, nil
}

func (f *ColumnFile) flushPage(page *Page) error {
  cp := (*page).(*columnPage)
  buf, err := cp.toBuffer()
  if err != nil {
    return err
  }
  column := (int)(cp.columnNo)
  pageNo := (int)(cp.pageNo)
  slotInColumn := pageNo / f.numColumns
  filename := f.filenames[column]
  file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0755)
  if err != nil {
    return err
  }
  defer file.Close()
  _, err = file.Seek((int64)(PageSize * slotInColumn), 0)
  if err != nil {
    return err
  }
  _, err = file.Write(buf.Bytes())
  if err != nil {
    return err
  }
  (*page).setDirty(false)
  return nil
}

func (f *ColumnFile) Descriptor() *TupleDesc {
  // TODO
  return &(f.td)
}

func (f *ColumnFile) IteratorColumns(columns []int, tid TransactionID) (func() (*Tuple, error), error) {
  pageInColumn := 0
  numColumns := len(columns)
  pages := make([]*columnPage, numColumns)
  for local_idx, i := range columns {
    p, err := f.bufPool.GetPage(f, pageInColumn * f.numColumns + i, tid, ReadPerm)
    if err != nil {
      return func() (*Tuple, error) {
        return nil, nil
      }, err
    }
    pages[local_idx] = (*p).(*columnPage)
  }
  iters := make([](func() (*Tuple, error)), numColumns)
  for local_idx, _ := range columns {
    iters[local_idx] = pages[local_idx].tupleIter()
  }

  fmt.Sprintf("use fmt")
  return func() (*Tuple, error) {
    tuples := make([](*Tuple), numColumns)
    for local_idx, _ := range columns {
      t, _ := iters[local_idx]()
      tuples[local_idx] = t
    }
    for tuples[0] == nil {
      pageInColumn++
      if pageInColumn >= f.numPagesPerColumn {
        return nil, nil
      }
      for local_idx, i := range columns {
        p, err := f.bufPool.GetPage(f, pageInColumn * f.numColumns + i, tid, ReadPerm)
        if err != nil {
          return nil, err
        }
        pages[local_idx] = (*p).(*columnPage)
        iters[local_idx] = pages[local_idx].tupleIter()
        t, _ := iters[local_idx]()
        tuples[local_idx] = t
      }
    }

    var ret *Tuple = nil
    for _, tup := range tuples {
      ret = joinTuples(ret, tup)
    }
    return ret, nil
  }, nil
}

func (f *ColumnFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
  // TODO
  columns := make([]int, f.numColumns)
  for i := 0; i < f.numColumns; i++ {
    columns[i] = i
  }
  return f.IteratorColumns(columns, tid)
}

type columnHash struct {
  filename string
  pageNo int
}

func (f *ColumnFile) pageKey(pgNo int) any {
  column := pgNo % f.numColumns
  return columnHash{filename : f.filenames[column],
                    pageNo : pgNo}
}

func (f *ColumnFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}
