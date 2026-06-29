package main

import (
	"log/slog"
	"sync"
	"time"

	"github.com/zzztttkkk/raft.simple/api"
)

type Server struct {
	lock sync.Mutex

	cfg           *Config
	machine_count int32
	conns         []IConn
	connmap       map[IConn]string

	_role Role
	term  int64
	store api.IAppendOnlyStore

	// candidate
	vote_began_at     time.Time
	vote_wait_timeout time.Duration
	vote_state        map[string]struct{}

	// follower
	consistency_checking bool
	last_leader_ping     *_LastLeaderPing
	last_voted_term      int64
	last_voted_for_conn  IConn

	// leader
	current_pong_term   int64
	current_pong_state  map[string]struct{}
	current_sync_states map[string]*_FollowerSyncState
}

type _FollowerSyncState struct {
	conn                 IConn
	consistency_checking bool
	max_same_version     int64
	next_send_version    int64
}

func (server *Server) _conns_copy_in_lock() []IConn {
	cs := make([]IConn, len(server.conns))
	copy(cs, server.conns)
	return cs
}

type _LastLeaderPing struct {
	At   time.Time
	Term int64
}

func (llp *_LastLeaderPing) ok(server *Server) bool {
	if time.Since(llp.At) > time.Duration(server.cfg.ElectionMaxStep)*time.Millisecond {
		return false
	}
	return llp.Term == server.term
}

func (server *Server) _change_role_in_lock(newrole Role, reason string) {
	if newrole == server._role {
		return
	}

	// reset props
	server.vote_began_at = time.Time{}
	server.vote_wait_timeout = time.Duration(0)
	clear(server.vote_state)

	server.last_voted_term = -1
	server.last_voted_for_conn = nil

	server.current_pong_term = -1
	clear(server.current_pong_state)
	clear(server.current_sync_states)

	// change role
	prev := server._role
	server._role = newrole
	slog.Info("role changed",
		slog.String("from", prev.String()),
		slog.String("to", newrole.String()),
		slog.String("reason", reason),
	)

	if server._role == RoleFollower {
		server.consistency_checking = true
		// todo do check
	}
}

func (server *Server) on_new_term_in_lock(conn IConn, new_term int64) {
	prev := server._role
	server._change_role_in_lock(RoleFollower, "on new term")
	prev_term := server.term
	server.term = new_term
	slog.Info("got a new term, change to follower 🫨",
		slog.String("prev_role", prev.String()),
		slog.String("conn", conn.Info()),
		slog.Int("new_term", int(new_term)),
		slog.Int("local_term", int(prev_term)),
	)
}
