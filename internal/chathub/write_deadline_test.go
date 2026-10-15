package chathub

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestChatRepliesToHeartbeatAfterInitialWriteDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serverDone := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverDone <- func() error {
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return err
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(25 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil { // SignalR handshake
				return err
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{}`+rs)); err != nil {
				return err
			}
			for i := 0; i < 2; i++ { // Initial ping and chat payload
				if _, _, err := conn.ReadMessage(); err != nil {
					return err
				}
			}
			// A long response must still answer pings after the original 15s
			// write deadline. Use the real transport to exercise deadline expiry.
			timer := time.NewTimer(16 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
			ping := `{"type":6}` + rs
			if err := conn.WriteMessage(websocket.TextMessage, []byte(ping)); err != nil {
				return err
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, reply, err := conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("read heartbeat reply: %w", err)
			}
			if string(reply) != ping {
				return fmt.Errorf("heartbeat reply = %q, want %q", reply, ping)
			}
			return conn.WriteMessage(websocket.TextMessage, []byte(`{"type":2,"item":{"result":{"value":"Success","message":"done"}}}`+rs+`{"type":3}`+rs))
		}()
	}))
	defer server.Close()

	client := &Client{Dialer: &websocket.Dialer{
		// Route the production wss URL to this local fixture without TLS or
		// external network access; all WebSocket/SignalR framing is real.
		NetDialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "http://"))
		},
	}}
	result, err := client.Chat(ctx, Account{AccessToken: "test", OID: "test", TID: "test"}, Request{Text: "hello"})
	select {
	case serverErr := <-serverDone:
		if serverErr != nil {
			t.Fatalf("upstream fixture: %v (chat error: %v)", serverErr, err)
		}
	case <-ctx.Done():
		t.Fatalf("upstream fixture did not finish: %v (chat error: %v)", ctx.Err(), err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "done" {
		t.Fatalf("text = %q, want done", result.Text)
	}
}
