package main

import "context"

type NotifyKind int

const (
	NotifyKindLeaderPing NotifyKind = iota
	NotifyKindFollowerPong
	NotifyKindCandidateVoteRequest
	NotifyKindVoteResponse
)

type Notifty struct {
	Kind    NotifyKind
	Term    int64
	Version int64

	// NotifyKindVoteResponse
	VoteAgreed bool

	// NotifyKindFollowerPong
	PingOK bool
}

type IConn interface {
	Info() string
	Notify(ctx context.Context, notify *Notifty) error
}
