package main

import (
	"context"

	"go.etcd.io/bbolt"
)

type _CtxKeyType int

const (
	_TxKey _CtxKeyType = iota
)

func withTx(ctx context.Context, tx *bbolt.Tx) context.Context {
	return context.WithValue(ctx, _TxKey, tx)
}

func peekTx(ctx context.Context) *bbolt.Tx {
	va := ctx.Value(_TxKey)
	if va == nil {
		return nil
	}
	return va.((*bbolt.Tx))
}
