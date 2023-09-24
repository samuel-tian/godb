package godb

import (
    "encoding/binary"
	"bytes"
    "unsafe"
    "errors"
    "fmt"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
    numSlots int32
    numUsedSlots int32
    dirty bool
    heapFile *HeapFile
    desc *TupleDesc
    tuples [](*Tuple)
    pageNo int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
    var tupleSize int32 = 0
    for _, f := range desc.Fields {
        switch f.Ftype {
        case IntType:
            tupleSize += (int32)(unsafe.Sizeof(int64(0)))
        case StringType:
            tupleSize += ((int32)(unsafe.Sizeof(byte('a')))) * (int32)(StringLength)
        }
    }
    var numSlots int32 = (int32)(PageSize - 8) / tupleSize
    tuples := make([](*Tuple), numSlots)
    for i, _ := range tuples {
        tuples[i] = nil
    }
    return &heapPage{numSlots: numSlots,
                     numUsedSlots: 0,
                     dirty: false,
                     heapFile: f,
                     desc: desc,
                     pageNo: pageNo,
                     tuples: tuples} //replace me
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
    return (int)(h.numSlots - h.numUsedSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
    if h.numUsedSlots == h.numSlots {
        return nil, errors.New("page is full")
    }
    for i, tup := range h.tuples {
        if tup == nil {
            h.tuples[i] = t
            h.numUsedSlots++
            h.setDirty(true)
            return i, nil
        }
    }
    return nil, errors.New(fmt.Sprintf("should not reach here %d %d", h.numUsedSlots, h.numSlots))
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
    switch rid := rid.(type) {
    case int:
        if h.tuples[rid] == nil {
            return errors.New("tuple to delete does not exist in page")
        }
        h.tuples[rid] = nil
        h.numUsedSlots--
        h.setDirty(true)
        return nil
    default:
        return errors.New("invalid record ID")
    }
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
    return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
    h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (h *heapPage) getFile() *DBFile {
	// TODO: some code goes here
    var f DBFile = h.heapFile
    return &f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, h.numSlots)
    if err != nil {
        return nil, err
    }
    err = binary.Write(buf, binary.LittleEndian, h.numUsedSlots)
    if err != nil {
        return nil, err
    }
    for _, t := range h.tuples {
        if t == nil {
            continue
        }
        err = t.writeTo(buf)
        if err != nil {
            return nil, err
        }
    }
	return buf, nil //replace me
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
    var numSlots int32
    err := binary.Read(buf, binary.LittleEndian, &numSlots)
    if err != nil {
        return err
    }
    h.numSlots = numSlots
    h.tuples = make([](*Tuple), numSlots)
    for i, _ := range h.tuples {
        h.tuples[i] = nil
    }
    var numUsedSlots int32
    err = binary.Read(buf, binary.LittleEndian, &numUsedSlots)
    if err != nil {
        return err
    }
    h.numUsedSlots = 0
    for i := 0; i < (int)(numUsedSlots); i++ {
        tup, err := readTupleFrom(buf, h.desc)
        if err != nil {
            return nil
        }
        h.insertTuple(tup)
    }
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
    rid := 0
    return func() (*Tuple, error) {
        for rid < (int)(p.numSlots) && p.tuples[rid] == nil {
            rid++
        }
        if rid == (int)(p.numSlots) {
            return nil, nil
        } else {
            ret := p.tuples[rid]
            rid++
            return ret, nil
        }
    }
}
