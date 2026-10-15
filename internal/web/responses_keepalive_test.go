package web

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Notifications are sent after Flush, so the test observes frames already
// delivered to the client. The recorder deliberately has no write lock: the
// adapter must serialize events and heartbeats, including under -race.
type responsesKeepaliveRecorder struct {
	*httptest.ResponseRecorder
	frames        chan string
	lastFrame     string
	failHeartbeat bool
}

func (w *responsesKeepaliveRecorder) Write(p []byte) (int, error) {
	if w.failHeartbeat && string(p) == ": keepalive\n\n" {
		return 0, io.ErrClosedPipe
	}
	w.lastFrame = string(p)
	return w.ResponseRecorder.Write(p)
}

func (w *responsesKeepaliveRecorder) Flush() {
	w.ResponseRecorder.Flush()
	select {
	case w.frames <- w.lastFrame:
	default:
	}
}

func waitResponsesFrame(t *testing.T, ctx context.Context, frames <-chan string, want string) {
	t.Helper()
	for {
		select {
		case frame := <-frames:
			if strings.HasPrefix(frame, want) {
				return
			}
		case <-ctx.Done():
			t.Fatalf("did not receive %q: %v", want, ctx.Err())
		}
	}
}

func TestResponsesKeepaliveBeforeTextAndDuringPause(t *testing.T) {
	const interval = 10 * time.Millisecond
	for _, terminal := range []string{"response.completed", "response.failed"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			w := &responsesKeepaliveRecorder{ResponseRecorder: httptest.NewRecorder(), frames: make(chan string, 64)}
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			allowText, allowFinish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				streamResponsesFromChat(w, r, oaiReq{}, "test-model", func(w http.ResponseWriter, r *http.Request) {
					select {
					case <-allowText:
					case <-r.Context().Done():
						return
					}
					_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n")
					select {
					case <-allowFinish:
					case <-r.Context().Done():
						return
					}
					if terminal == "response.failed" {
						_, _ = fmt.Fprint(w, "data: {\"error\":{\"code\":\"upstream_timeout\",\"message\":\"timed out\"}}\n\n")
					}
					_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
				}, interval)
			}()
			waitResponsesFrame(t, ctx, w.frames, "event: response.created\n")
			waitResponsesFrame(t, ctx, w.frames, ": keepalive\n\n")
			close(allowText)
			waitResponsesFrame(t, ctx, w.frames, "event: response.output_text.delta\n")
			waitResponsesFrame(t, ctx, w.frames, ": keepalive\n\n")
			close(allowFinish)
			waitResponsesFrame(t, ctx, w.frames, "event: "+terminal+"\n")
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("adapter did not stop after its terminal event")
			}
			select {
			case frame := <-w.frames:
				t.Fatalf("frame after terminal event: %q", frame)
			case <-time.After(3 * interval):
			}
			if strings.Count(w.Body.String(), ": keepalive\n\n") < 2 {
				t.Fatal("heartbeats were not flushed during both idle periods")
			}
		})
	}
}

func TestResponsesKeepaliveStopsOnDisconnect(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("write_failure=%t", failWrite), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			w := &responsesKeepaliveRecorder{ResponseRecorder: httptest.NewRecorder(), frames: make(chan string, 64), failHeartbeat: failWrite}
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			producerDone, adapterDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(adapterDone)
				streamResponsesFromChat(w, r, oaiReq{}, "test-model", func(w http.ResponseWriter, r *http.Request) {
					defer close(producerDone)
					<-r.Context().Done()
				}, 10*time.Millisecond)
			}()
			waitResponsesFrame(t, ctx, w.frames, "event: response.created\n")
			if !failWrite {
				cancel()
			}
			for _, done := range []<-chan struct{}{adapterDone, producerDone} {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("disconnect left the adapter or producer running")
				}
			}
			if strings.Contains(w.Body.String(), "event: response.completed\n") {
				t.Fatal("disconnected request was marked completed")
			}
		})
	}
}
