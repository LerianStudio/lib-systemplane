//go:build unit

// The LISTEN stage failing on a connection that came up fine: pgbouncer in
// transaction pooling refuses LISTEN. openListen (first open) and dialAndListen
// (reconnect) must each report that stage and close the connection they opened.
package postgres

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/jackc/pgx/v5/pgproto3"
)

// listenRefusingServer answers the startup handshake and the identity query
// like Postgres, refuses LISTEN, and closes the returned channel once the
// client has dropped its connection.
func listenRefusingServer(t *testing.T) (dsn string, clientGone <-chan struct{}) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = ln.Close() })

	gone := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}

		defer conn.Close()
		defer close(gone)

		be := pgproto3.NewBackend(conn, conn)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}

		be.Send(&pgproto3.AuthenticationOk{})
		be.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

		for be.Flush() == nil {
			msg, err := be.Receive()
			if err != nil {
				return
			}

			q, ok := msg.(*pgproto3.Query)
			if !ok {
				return // Terminate, or anything this fake does not speak
			}

			if strings.HasPrefix(q.String, "LISTEN") {
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: "LISTEN is not supported"})
			} else {
				be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
					{Name: []byte("db"), DataTypeOID: 25}, {Name: []byte("addr"), DataTypeOID: 25},
					{Name: []byte("port"), DataTypeOID: 23}, {Name: []byte("started"), DataTypeOID: 25},
				}})
				be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("fake"), []byte("127.0.0.1"), []byte("5432"), []byte("1")}})
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			}

			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}()

	dsn = "postgres://u@" + ln.Addr().String() + "/fake?sslmode=disable&default_query_exec_mode=simple_protocol"

	return dsn, gone
}

func TestPostgresReconnectRefusedListenIsReportedAndClosed(t *testing.T) {
	dsn, gone := listenRefusingServer(t)

	s := &Store{}

	conn, err := s.dialAndListen(newFeed(store.Scope{Tenant: "t1"}, dsn))
	if conn != nil {
		_ = conn.Close(context.Background())

		t.Fatal("a refused LISTEN returned a connection: the feed would read from a socket no notification ever reaches")
	}

	assertListenStage(t, err)
	assertClientGone(t, gone)
}

func TestPostgresOpenListenRefusedListenIsReportedAndClosed(t *testing.T) {
	dsn, gone := listenRefusingServer(t)

	s := &Store{feeds: map[string]*feed{}}

	conn, _, err := s.openListen(context.Background(), newFeed(store.Scope{Tenant: "t1"}, dsn))
	if conn != nil {
		_ = conn.Close(context.Background())

		t.Fatal("a refused LISTEN returned a connection: the feed would be published with a socket no notification ever reaches")
	}

	assertListenStage(t, err)
	assertClientGone(t, gone)
}

// "listen connect" shares a prefix with the LISTEN stage, so it is excluded
// explicitly: a dial that never answered must not satisfy this assertion.
func assertListenStage(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("LISTEN succeeded; the refusal this test drives no longer happens")
	}

	if strings.Contains(err.Error(), "listen connect") {
		t.Fatalf("error %q names the connect stage; the connection came up, so the LISTEN is what failed", err)
	}

	if !strings.Contains(err.Error(), "systemplane/postgres: listen tenant t1") {
		t.Fatalf("error %q does not name the LISTEN stage and the feed it belongs to", err)
	}
}

func assertClientGone(t *testing.T, gone <-chan struct{}) {
	t.Helper()

	select {
	case <-gone:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection opened before the refused LISTEN was never closed, so every retry leaks one")
	}
}
