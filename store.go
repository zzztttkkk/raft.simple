package main

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

type OpKind int

const (
	OpKindRead OpKind = iota
	OpKindSet
	OpKindDel
)

type OpResult sql.Null[struct {
	err    error
	val    []byte
	hasval bool
}]

var (
	ErrPendingOp      = errors.New("pending op")
	ErrNilResultBytes = errors.New("nil result bytes")
)

func (result *OpResult) Error() error {
	if !result.Valid {
		return ErrPendingOp
	}
	return result.V.err
}

func (result *OpResult) Bytes() ([]byte, error) {
	err := result.Error()
	if err != nil {
		return nil, err
	}
	if !result.V.hasval {
		return nil, ErrNilResultBytes
	}
	return result.V.val, nil
}

type Op struct {
	kind   OpKind
	bucket string
	key    string
	val    sql.Null[[]byte]
	local  bool

	Result OpResult
}

var (
	oppool = sync.Pool{
		New: func() any {
			return &Op{
				val: sql.Null[[]byte]{
					V: make([]byte, 0, 4096),
				},
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
		if op.Result.Valid {
			op.Result.Valid = false
			op.Result.V.err = nil
			op.Result.V.hasval = false
			if cap(op.Result.V.val) > 1024*1024 {
				op.Result.V.val = make([]byte, 0, 4096)
			} else {
				op.Result.V.val = op.Result.V.val[:0]
			}
		}
		oppool.Put(op)
	}
}

type IAppendOnlyStore interface {
	Version() int64
	View(ctx context.Context, ops ...*Op) error
	Update(ctx context.Context, ops ...*Op) (int64, error)
	ApplyWithVersion(ctx context.Context, version int64, ops ...*Op) error
}
