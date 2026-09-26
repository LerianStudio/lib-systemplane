// Package mongotest holds the live-MongoDB test helpers shared across packages:
// the wait for a fresh replica-set member to accept writes, and a severable
// proxy that holds a changefeed down from outside the library.
package mongotest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// AwaitWritablePrimary waits out the member's SECONDARY-to-PRIMARY step after
// the container reports ready, where a write fails with NotWritablePrimary.
func AwaitWritablePrimary(ctx context.Context, client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}

		err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		if err == nil && hello.IsWritablePrimary {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("mongo member never became writable primary: %w", errors.Join(err, ctx.Err()))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Proxy forwards a local port to target so a test can sever every connection
// through it and restore it on the same address. The outage lasts as long as
// the test wants, which a one-shot cursor kill cannot give.
type Proxy struct {
	target string
	// Addr is the proxy's listen address; a client dials it instead of target.
	Addr string

	mu      sync.Mutex
	ln      net.Listener
	conns   []net.Conn
	severed bool

	wg sync.WaitGroup
}

// NewProxy starts a Proxy for target, severed again when the test ends.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()

	ln, err := new(net.ListenConfig).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for proxy: %v", err)
	}

	p := &Proxy{target: target, Addr: ln.Addr().String()}
	p.serve(ln)

	t.Cleanup(p.Sever)

	return p
}

func (p *Proxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.severed = false
	p.mu.Unlock()

	p.wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			p.handle(conn)
		}
	})
}

// handle checks severed and appends in ONE hold, so a connection accepted
// while Sever runs is closed instead of outliving the outage.
func (p *Proxy) handle(client net.Conn) {
	upstream, err := new(net.Dialer).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = client.Close()

		return
	}

	p.mu.Lock()

	if p.severed {
		p.mu.Unlock()

		_ = client.Close()
		_ = upstream.Close()

		return
	}

	p.conns = append(p.conns, client, upstream)
	p.mu.Unlock()

	p.pipe(client, upstream)
	p.pipe(upstream, client)
}

func (p *Proxy) pipe(dst, src net.Conn) {
	p.wg.Go(func() {
		_, _ = io.Copy(dst, src)

		_ = dst.Close()
		_ = src.Close()
	})
}

// Sever refuses new connections, drops every live one and waits for the
// forwarding goroutines. It is safe to call twice.
func (p *Proxy) Sever() {
	p.mu.Lock()
	p.severed = true
	ln, conns := p.ln, p.conns
	p.ln, p.conns = nil, nil
	p.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	for _, conn := range conns {
		_ = conn.Close()
	}

	p.wg.Wait()
}

// Restore listens again on Addr, so a client reconnects without being told.
func (p *Proxy) Restore(t testing.TB) {
	t.Helper()

	ln, err := new(net.ListenConfig).Listen(context.Background(), "tcp", p.Addr)
	if err != nil {
		t.Fatalf("restore proxy on %s: %v", p.Addr, err)
	}

	p.serve(ln)
}
