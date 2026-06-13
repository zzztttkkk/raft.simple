package main

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

type Server struct {
	lock sync.Mutex

	machine_count int32
	conns         []IConn

	role  Role
	term  int64
	store IAppendOnlyStore

	// for candidate
	vote_state map[int]struct{}

	// follower
	last_leader_notify  atomic.Int64
	last_voted_term     int64
	last_voted_for_conn IConn
}

func (server *Server) on_new_term(conn IConn, new_term int64) {
	prev := server.role
	server.role = RoleFollower
	prev_term := server.term
	server.term = new_term
	slog.Info("got a new term, change to follower 🫨",
		slog.Int("prev_role", int(prev)),
		slog.String("conn", conn.Info()),
		slog.Int("new_term", int(new_term)),
		slog.Int("local_term", int(prev_term)),
	)
}
