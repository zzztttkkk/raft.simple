package api

import (
	"context"
	"errors"
	"sync"
)

type Option[T any] struct {
	V     T
	Valid bool
}

type OpKind int

const (
	OpKindRead OpKind = iota
	OpKindSet
	OpKindDel
)

type OpResult struct {
	idx    int
	err    error
	val    []byte
	hasval bool
}

var (
	ErrNilResultBytes = errors.New("nil result bytes")
)

func (result *OpResult) Index() int {
	return result.idx
}

func (result *OpResult) Error() error {
	return result.err
}

func (result *OpResult) Bytes() ([]byte, error) {
	err := result.Error()
	if err != nil {
		return nil, err
	}
	if !result.hasval {
		return nil, ErrNilResultBytes
	}
	return result.val, nil
}

type Op struct {
	kind   OpKind
	bucket string
	key    string
	val    Option[[]byte]
	local  bool
}

var (
	oppool = sync.Pool{
		New: func() any {
			return &Op{
				val: Option[[]byte]{
					V: make([]byte, 0, 4096),
				},
			}
		},
	}

	resultpool = sync.Pool{
		New: func() any {
			return &OpResult{
				val: make([]byte, 0, 4096),
			}
		},
	}
)

type ReadOptions struct {
	Local bool
}

func NewReadOp(bucket, key string, opts *ReadOptions) *Op {
	op := oppool.Get().(*Op)
	op.kind = OpKindRead
	op.bucket = bucket
	op.key = key
	op.local = opts != nil && opts.Local
	return op
}

func NewDelOp(bucket, key string) *Op {
	op := oppool.Get().(*Op)
	op.kind = OpKindDel
	op.bucket = bucket
	op.key = key
	return op
}

func NewSetOp(bucket, key string, val []byte) *Op {
	op := oppool.Get().(*Op)
	op.kind = OpKindSet
	op.bucket = bucket
	op.key = key
	op.val.V = append(op.val.V, val...)
	op.val.Valid = true
	return op
}

func ReleaseOps(ops ...*Op) {
	for _, op := range ops {
		op.bucket = ""
		op.key = ""
		op.local = false

		if op.val.Valid {
			op.val.Valid = false
			if cap(op.val.V) > 1024*1024 {
				op.val.V = make([]byte, 0, 4096)
			} else {
				op.val.V = op.val.V[:0]
			}
		}
		oppool.Put(op)
	}
}

func NewValResult(idx int, val []byte) *OpResult {
	obj := resultpool.Get().(*OpResult)
	obj.idx = idx
	obj.err = nil
	obj.hasval = true
	obj.val = append(obj.val, val...)
	return obj
}

func NewErrResult(idx int, err error) *OpResult {
	obj := resultpool.Get().(*OpResult)
	obj.idx = idx
	obj.err = err
	obj.hasval = false
	return obj
}

func ReleaseResults(results ...*OpResult) {
	for _, v := range results {
		v.err = nil
		v.hasval = false
		if cap(v.val) > 1024*1024 {
			v.val = make([]byte, 0, 4096)
		} else {
			v.val = v.val[:0]
		}
		resultpool.Put(v)
	}
}

type LogEntry struct {
	Term    int64
	Version int64
	Ops     []*Op
}

type IAppendOnlyStore interface {
	Version() int64
	Has(ctx context.Context, term, version int64) (bool, error)

	View(ctx context.Context, ops ...*Op) ([]*OpResult, error)
	Export(ctx context.Context, begin int64, count int) ([]*LogEntry, error)

	TruncateAfter(v int64) error
	Update(ctx context.Context, term int64, ops ...*Op) (int64, []*OpResult, error)
}
