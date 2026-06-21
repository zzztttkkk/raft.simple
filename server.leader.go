package main

import (
	"context"
	"log/slog"
	"slices"
	"time"
)

func (server *Server) do_leader_ping_internal(term int64, version int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(server.cfg.LeaderPingTimeoput))
	defer cancel()

	for _, conn := range server.conns {
		go func() {
			err := conn.Notify(ctx, &Notifty{Kind: NotifyKindLeaderPing, Term: term, Version: version})
			if err != nil {
				slog.Error("send leader ping failed", slog.Any("err", err), slog.String("conn", conn.Info()))
			}
		}()
	}
}

func (server *Server) do_leader_ping() {
	server.lock.Lock()
	defer server.lock.Unlock()

	if server.role != RoleLeader {
		return
	}

	// check last ping
	if server.current_pong_term > 0 {
		if len(server.current_pong_state)+1 < (int(server.machine_count)/2)+1 {
			server.current_pong_term = -1
			server.current_pong_state = nil
			server.role = RoleFollower
			slog.Info("too few follower pong, change to follower")
			return
		}
	}
	term, version := server.term, server.store.Version()
	server.current_pong_term = term
	server.current_pong_state = make(map[int]struct{}, server.machine_count)

	server.do_leader_ping_internal(term, version)
}

func (server *Server) leader_loop() {
	for {
		time.Sleep(time.Millisecond * time.Duration(server.cfg.LeaderPingStep))
		server.do_leader_ping()
	}
}

func (server *Server) on_leader_ping(conn IConn, notify *Notifty) {
	server.lock.Lock()
	defer server.lock.Unlock()

	term, version := server.term, server.store.Version()

	sendpong := func(ok bool) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(server.cfg.LeaderPingTimeoput*3/4))
			defer cancel()
			err := conn.Notify(ctx, &Notifty{Kind: NotifyKindFollowerPong, Term: term, Version: version, PingOK: ok})
			if err != nil {
				slog.Error("send follower pong failed", slog.Any("err", err), slog.String("conn", conn.Info()))
			}
		}()
	}

	if notify.Term != server.term {
		if notify.Term > server.term {
			server.on_new_term(conn, notify.Term)
		} else {
			// let the old leader change to follower
			sendpong(false)
		}
		return
	}

	server.last_leader_ping = &_LastLeaderPing{
		At:   time.Now().UnixMilli(),
		Term: notify.Term,
	}
	sendpong(true)
}

func (server *Server) on_follower_pong(conn IConn, notify *Notifty) {
	server.lock.Lock()
	defer server.lock.Unlock()

	if server.role != RoleLeader {
		return
	}

	if notify.Term < server.term {
		return
	}

	if notify.Term > server.term {
		server.on_new_term(conn, notify.Term)
		return
	}

	if !notify.PingOK {
		return
	}

	if notify.Term != server.current_pong_term {
		return
	}

	idx := slices.Index(server.conns, conn)
	server.current_pong_state[idx] = struct{}{}
}
