package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

func (server *Server) do_election(timeouts time.Duration) {
	server.lock.Lock()
	if server._role == RoleLeader {
		server.lock.Unlock()
		return
	}
	// prevent election if leader is alive
	if server.last_leader_ping != nil && server.last_leader_ping.ok(server) {
		server.last_leader_ping = nil
		server.lock.Unlock()
		return
	}
	server._change_role_in_lock(RoleCandidate, "do election")
	server.term++
	term := server.term
	version := server.store.Version()
	server.vote_began_at = time.Now()
	server.vote_wait_timeout = timeouts
	server.vote_state = map[string]struct{}{}
	// prevent vote another
	server.last_voted_term = term
	server.last_voted_for_conn = nil

	conns := server._conns_copy_in_lock()

	server.lock.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeouts)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(len(conns))

		for _, conn := range conns {
			go func() {
				defer wg.Done()
				err := conn.Notify(ctx, &Notify{Kind: NotifyKindCandidateVoteRequest, Term: term, Version: version})
				if err != nil {
					slog.Error("send vote request failed", slog.Any("err", err), slog.String("conn", conn.Info()))
				}
			}()
		}

		wg.Wait()
	}()
}

func (server *Server) _send_vote_response(conn IConn, term int64, version int64, agreed bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		err := conn.Notify(ctx, &Notify{
			Kind: NotifyKindVoteResponse, Term: term, Version: version, VoteAgreed: agreed,
		})
		if err != nil {
			slog.Error("send vote response failed", slog.Any("err", err), slog.String("conn", conn.Info()))
		}
	}()
}

func (server *Server) on_vote_request(conn IConn, notify *Notify) {
	server.lock.Lock()
	defer server.lock.Unlock()

	term := server.term
	version := server.store.Version()
	if notify.Term < term || (notify.Term == term && notify.Version < version) {
		slog.Info("got a vote request from a old server 🤣", slog.String("conn", conn.Info()))
		server._send_vote_response(conn, term, version, false)
		return
	}
	if notify.Term > term {
		server.on_new_term_in_lock(conn, notify.Term)
		term = notify.Term
	}
	if notify.Term == server.last_voted_term && conn != server.last_voted_for_conn {
		slog.Info(
			"got a vote request, but already voted 🙄",
			slog.String("request conn", conn.Info()),
			slog.String("voted conn", server.last_voted_for_conn.Info()),
		)
		server._send_vote_response(conn, term, version, false)
		return
	}
	server.last_voted_term = notify.Term
	server.last_voted_for_conn = conn

	server._send_vote_response(conn, term, version, true)
}

func (server *Server) on_vote_response(conn IConn, notify *Notify) {
	server.lock.Lock()
	defer server.lock.Unlock()

	if !notify.VoteAgreed {
		if notify.Term > server.term {
			server.on_new_term_in_lock(conn, notify.Term)
		}
		return
	}

	if server._role != RoleCandidate {
		slog.Info(
			"got a vote response, but i'm not a candidate 😭",
			slog.String("conn", conn.Info()),
			slog.Int("resp_term", int(notify.Term)),
		)
		return
	}
	if notify.Term != server.term {
		if notify.Term > server.term {
			server.on_new_term_in_lock(conn, notify.Term)
			return
		}
		slog.Info(
			"got a old vote response 😓",
			slog.String("conn", conn.Info()),
			slog.Int("resp_term", int(notify.Term)),
			slog.Int("local_term", int(server.term)),
		)
		return
	}

	if time.Since(server.vote_began_at) > server.vote_wait_timeout {
		slog.Info(
			"got a timeouted vote response",
			slog.String("conn", conn.Info()),
		)
		return
	}

	idx, ok := server.connmap[conn]
	if !ok {
		return
	}
	server.vote_state[idx] = struct{}{}
	if len(server.vote_state)+1 < (int(server.machine_count)/2 + 1) {
		return
	}
	server._change_role_in_lock(RoleLeader, "on vote response")
	server.do_leader_ping_internal(server._conns_copy_in_lock(), server.term, server.store.Version())
}

func (server *Server) election_loop() {
	n := server.cfg.ElectionMaxStep - server.cfg.ElectionMinStep
	for {
		timeouts := time.Millisecond * time.Duration(rand.IntN(int(n))+int(server.cfg.ElectionMinStep))
		server.do_election(timeouts)
		time.Sleep(timeouts + time.Millisecond*time.Duration(rand.IntN(50)))
	}
}
