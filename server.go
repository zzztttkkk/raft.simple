package main

import (
	"log/slog"
	"sync"
	"time"
)

type Server struct {
	lock sync.Mutex

	cfg           *Config
	machine_count int32
	conns         []IConn

	role  Role
	term  int64
	store IAppendOnlyStore

	// candidate
	vote_state map[int]struct{}

	// follower
	last_leader_ping *_LastLeaderPing

	last_voted_term     int64
	last_voted_for_conn IConn

	// leader
	current_pong_term  int64
	current_pong_state map[int]struct{}
}

type _LastLeaderPing struct {
	At   int64
	Term int64
}

func (llp *_LastLeaderPing) ok(server *Server) bool {
	diff := time.Now().UnixMilli() - llp.At
	if diff > int64(server.cfg.ElectionMaxStep) {
		return false
	}
	return llp.Term == server.term
}

func (server *Server) on_new_term(conn IConn, new_term int64) {
	prev := server.role
	server.role = RoleFollower
	prev_term := server.term
	server.term = new_term
	slog.Info("got a new term, change to follower 🫨",
		slog.String("prev_role", prev.String()),
		slog.String("conn", conn.Info()),
		slog.Int("new_term", int(new_term)),
		slog.Int("local_term", int(prev_term)),
	)
}
