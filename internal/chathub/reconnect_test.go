package chathub

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type reconnectWriteFailureConn struct {
	net.Conn
	writes int
}

func (c *reconnectWriteFailureConn) Write(p []byte) (int, error) {
	c.writes++
	if c.writes == 4 { // HTTP upgrade, SignalR handshake, ping, payload.
		return 0, errText("write tcp: i/o timeout")
	}
	return c.Conn.Write(p)
}

func TestChatAutomaticallyReconnectsOnlyBeforeOutput(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		writeTimeout, dialFail bool
		continuation, buffered bool
		alwaysFail             bool
		prefix                 string
		status                 int
		wantDials              int32
		wantSuccess            bool
	}{
		{name: "write timeout with one account", writeTimeout: true, wantDials: 2, wantSuccess: true},
		{name: "read disconnect before output", wantDials: 2, wantSuccess: true},
		{name: "continuation dial failure", dialFail: true, continuation: true, wantDials: 2, wantSuccess: true},
		{name: "continuation after payload", continuation: true, wantDials: 1},
		{name: "ambiguous continuation write", continuation: true, writeTimeout: true, wantDials: 1},
		{name: "partial text", prefix: `{"type":1,"target":"update","arguments":[{"messages":[{"author":"bot","text":"partial"}]}]}`, wantDials: 1},
		{name: "partial reasoning", prefix: `{"type":1,"target":"update","arguments":[{"messages":[{"author":"bot","messageType":"Progress","contentOrigin":"ChainOfThoughtSummary","text":"thinking"}]}]}`, wantDials: 1},
		{name: "buffered tool event", buffered: true, prefix: `{"type":1,"target":"update","arguments":[{"plugin":{"functionName":"list_files","functionArguments":{"path":"."}}}]}`, wantDials: 1},
		{name: "tool event", prefix: `{"type":1,"target":"update","arguments":[{"plugin":{"functionName":"list_files","functionArguments":{"path":"."}}}]}`, wantDials: 1},
		{name: "retry limit", alwaysFail: true, wantDials: 2},
		{name: "quota is not a transport failure", status: http.StatusTooManyRequests, wantDials: 1},
		{name: "auth is not a transport failure", status: http.StatusUnauthorized, wantDials: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dials, exchanges atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := exchanges.Add(1)
				if tc.status != 0 {
					w.WriteHeader(tc.status)
					return
				}
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`{}`+rs)); err != nil {
					return
				}
				for i := 0; i < 2; i++ { // Initial ping and payload.
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
				if (n == 1 && !tc.dialFail) || tc.alwaysFail {
					if tc.prefix != "" {
						_ = conn.WriteMessage(websocket.TextMessage, []byte(tc.prefix+rs))
					}
					return // Drop the transport without a completion frame.
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":2,"item":{"result":{"value":"Success","message":"done"}}}`+rs+`{"type":3}`+rs))
			}))
			defer server.Close()
			client := &Client{Dialer: &websocket.Dialer{
				NetDialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					n := dials.Add(1)
					if n == 1 && tc.dialFail {
						return nil, errText("connection reset by peer")
					}
					conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "http://"))
					if err == nil && n == 1 && tc.writeTimeout {
						return &reconnectWriteFailureConn{Conn: conn}, nil
					}
					return conn, err
				},
			}}
			req := Request{Text: "hello"}
			if tc.continuation {
				req.ConversationID, req.SessionID = "existing-conversation", "existing-session"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			account := Account{AccessToken: "test", OID: "only-account", TID: "test"}
			var result Result
			var err error
			if tc.buffered {
				result, err = client.Chat(ctx, account, req)
			} else {
				result, err = client.ChatWithEvents(ctx, account, req, func(StreamEvent) error { return nil })
			}
			if tc.wantSuccess {
				if err != nil || result.Text != "done" {
					t.Fatalf("result=%q err=%v, want successful automatic reconnect", result.Text, err)
				}
			} else if err == nil {
				t.Fatal("interrupted request unexpectedly succeeded")
			}
			if got := dials.Load(); got != tc.wantDials {
				t.Fatalf("dials=%d, want %d", got, tc.wantDials)
			}
			var internalErr *reconnectableChatError
			if errors.As(err, &internalErr) {
				t.Fatal("internal retry marker escaped the public entry point")
			}
		})
	}
}

func TestChatReconnectBackoffHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var dials atomic.Int32
	firstDial := make(chan struct{})
	client := &Client{Dialer: &websocket.Dialer{
		NetDialTLSContext: func(context.Context, string, string) (net.Conn, error) {
			if dials.Add(1) == 1 {
				close(firstDial)
			}
			return nil, errText("connection reset by peer")
		},
	}}
	done := make(chan error, 1)
	go func() {
		_, err := client.Chat(ctx, Account{AccessToken: "test", OID: "only-account", TID: "test"}, Request{Text: "hello"})
		done <- err
	}()
	<-firstDial
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request did not stop reconnecting")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("canceled request dialed %d times", got)
	}
}
