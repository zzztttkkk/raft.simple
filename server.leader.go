package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

func (server *Server) _check_cfg_ping_pong() error {
	if server.cfg.LeaderPingTimeout >= server.cfg.LeaderPingStep {
		return fmt.Errorf("check cfg failed: LeaderPingTimeout must less than LeaderPingStep")
	}
	return nil
}

func (server *Server) do_leader_ping_internal(conns []IConn, term int64, version int64) {
	go func() {
		timeout := time.Millisecond * time.Duration(server.cfg.LeaderPingTimeout)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(len(conns))

		for _, conn := range conns {
			go func() {
				defer wg.Done()

				err := conn.Notify(ctx, &Notify{Kind: NotifyKindLeaderPing, Term: term, Version: version})
				if err != nil {
					slog.Error("send leader ping failed", slog.Any("err", err), slog.String("conn", conn.Info()))
				}
			}()
		}

		wg.Wait()
	}()
}

func (server *Server) do_leader_ping() {
	server.lock.Lock()
	defer server.lock.Unlock()

	if server._role != RoleLeader {
		return
	}

	// check last ping
	if server.current_pong_term > 0 {
		if len(server.current_pong_state)+1 < (int(server.machine_count)/2)+1 {
			server.current_pong_term = -1
			server.current_pong_state = nil
			server._change_role_in_lock(RoleFollower, "last leader ping failed")
			slog.Info("too few follower pong, change to follower")
			return
		}
	}
	term, version := server.term, server.store.Version()
	server.current_pong_term = term
	server.current_pong_state = make(map[string]struct{}, server.machine_count)

	server.do_leader_ping_internal(server._conns_copy_in_lock(), term, version)
}

func (server *Server) leader_loop() {
	for {
		time.Sleep(time.Millisecond * time.Duration(server.cfg.LeaderPingStep))
		server.do_leader_ping()
	}
}

func (server *Server) on_leader_ping(conn IConn, notify *Notify) {
	server.lock.Lock()
	defer server.lock.Unlock()

	term, version := server.term, server.store.Version()

	sendpong := func(ok bool) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(server.cfg.LeaderPingTimeout*3/4))
			defer cancel()
			err := conn.Notify(ctx, &Notify{Kind: NotifyKindFollowerPong, Term: term, Version: version, PingOK: ok})
			if err != nil {
				slog.Error("send follower pong failed", slog.Any("err", err), slog.String("conn", conn.Info()))
			}
		}()
	}

	if notify.Term != server.term {
		if notify.Term > server.term {
			server.on_new_term_in_lock(conn, notify.Term)
			sendpong(true)
		} else {
			// let the old leader change to follower
			sendpong(false)
		}
		return
	}

	server.last_leader_ping = &_LastLeaderPing{
		At:   time.Now(),
		Term: notify.Term,
	}
	sendpong(true)
}

func (server *Server) on_follower_pong(conn IConn, notify *Notify) {
	server.lock.Lock()
	defer server.lock.Unlock()

	if server._role != RoleLeader {
		return
	}

	if notify.Term < server.term {
		return
	}

	if notify.Term > server.term {
		server.on_new_term_in_lock(conn, notify.Term)
		return
	}

	if !notify.PingOK {
		slog.Warn("follower reject ping", slog.String("conn", conn.Info()))
		return
	}

	if notify.Term != server.current_pong_term {
		return
	}

	idx, ok := server.connmap[conn]
	if !ok {
		return
	}
	server.current_pong_state[idx] = struct{}{}
}
