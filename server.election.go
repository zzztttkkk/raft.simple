package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"
)

func (server *Server) do_election() {
	server.lock.Lock()
	if server.role == RoleLeader {
		server.lock.Unlock()
		return
	}
	// prevent election if leader is alive
	if server.last_leader_ping != nil && server.last_leader_ping.ok(server) {
		server.last_leader_ping = nil
		server.lock.Unlock()
		return
	}
	server.role = RoleCandidate
	server.term++
	term := server.term
	version := server.store.Version()
	server.vote_state = map[int]struct{}{}
	// prevent vote another
	server.last_voted_term = term
	server.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*150)
	defer cancel()

	for _, conn := range server.conns {
		go func() {
			err := conn.Notify(ctx, &Notifty{Kind: NotifyKindCandidateVoteRequest, Term: term, Version: version})
			if err != nil {
				slog.Error("send vote request failed", slog.Any("err", err), slog.String("conn", conn.Info()))
			}
		}()
	}
}

func (server *Server) _send_vote_response(conn IConn, term int64, version int64, agreed bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		err := conn.Notify(ctx, &Notifty{
			Kind: NotifyKindVoteResponse, Term: term, Version: version, VoteAgreed: agreed,
		})
		if err != nil {
			slog.Error("send vote response failed", slog.Any("err", err), slog.String("conn", conn.Info()))
		}
	}()
}

func (server *Server) on_vote_request(conn IConn, notify *Notifty) {
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
		server.on_new_term(conn, notify.Term)
		term = notify.Term
	}
	if notify.Term == server.last_voted_term && conn != server.last_voted_for_conn {
		slog.Info(
			"got a vote request, but already voted 🙄",
			slog.String("request conn", conn.Info()),
			slog.String("voted conn", server.last_voted_for_conn.Info()),
		)
		return
	}
	server.last_voted_term = notify.Term
	server.last_voted_for_conn = conn

	server._send_vote_response(conn, term, version, true)
}

func (server *Server) on_vote_response(conn IConn, notify *Notifty) {
	server.lock.Lock()
	defer server.lock.Unlock()

	if !notify.VoteAgreed {
		if notify.Term > server.term {
			server.on_new_term(conn, notify.Term)
		}
		return
	}

	if server.role != RoleCandidate {
		slog.Info(
			"got a vote response, but i'm not a candidate 😭",
			slog.String("conn", conn.Info()),
			slog.Int("resp_term", int(notify.Term)),
		)
		return
	}
	if notify.Term != server.term {
		if notify.Term > server.term {
			server.on_new_term(conn, notify.Term)
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

	idx := slices.Index(server.conns, conn)
	if idx < 0 {
		return
	}

	server.vote_state[idx] = struct{}{}
	if len(server.vote_state)+1 < (int(server.machine_count)/2 + 1) {
		return
	}
	server.role = RoleLeader
	server.do_leader_ping_internal(server.term, server.store.Version())
}

func (server *Server) election_loop() {
	n := server.cfg.ElectionMaxStep - server.cfg.ElectionMinStep
	for {
		time.Sleep(time.Millisecond * time.Duration(rand.IntN(int(n))+int(server.cfg.ElectionMinStep)))
		server.do_election()
	}
}
