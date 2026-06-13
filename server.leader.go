package main

import (
	"context"
	"log/slog"
	"time"
)

func (server *Server) do_leader_ping_internal(term int64, version int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*150)
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

func (serer *Server) leader_loop() {
	for {
		time.Sleep(time.Millisecond * 50)
	}
}
