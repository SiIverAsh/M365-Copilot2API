package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestResponsesStreamPreservesUpstreamError(t *testing.T) {
	textChunk := `data: {"choices":[{"delta":{"content":"partial answer"}}]}` + "\n\n"
	toolChunk := `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"weather","arguments":"{"}}]}}]}` + "\n\n"
	errorFrame := `data: {"error":{"code":"upstream_timeout","type":"upstream_error","message":"upstream timed out waiting for the response stream (WS_READ_TIMEOUT)"}}` + "\n\n"
	for _, tc := range []struct {
		name, prefix, failure, wantCode, wantType, wantMessage string
		status                                                 int
	}{
		{"before text", "", errorFrame, "upstream_timeout", "upstream_error", "upstream timed out waiting for the response stream (WS_READ_TIMEOUT)", 200},
		{"after partial text", textChunk, errorFrame, "upstream_timeout", "upstream_error", "upstream timed out waiting for the response stream (WS_READ_TIMEOUT)", 200},
		{"after partial tool", toolChunk, errorFrame, "upstream_timeout", "upstream_error", "upstream timed out waiting for the response stream (WS_READ_TIMEOUT)", 200},
		{"http JSON error", "", `{"error":{"type":"invalid_request_error","message":"messages required"}}` + "\n", "invalid_request_error", "invalid_request_error", "messages required", 400},
		{"missing code", textChunk, `data: {"error":{"message":"generation interrupted"}}` + "\n\n", "upstream_error", "", "generation interrupted", 200},
		{"string error", textChunk, `data: {"error":"generation interrupted"}` + "\n\n", "upstream_error", "", "generation interrupted", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			w := httptest.NewRecorder()
			streamResponsesFromChat(w, r, oaiReq{}, "test-model", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				// Even malformed upstreams that continue writing after an error
				// must not turn the failed response back into a successful one.
				_, _ = fmt.Fprint(w, tc.prefix+tc.failure+errorFrame+textChunk+"data: [DONE]\n\n")
			}, responsesKeepaliveInterval)
			body := w.Body.String()
			if strings.Count(body, "event: response.failed\n") != 1 {
				t.Fatalf("expected exactly one failure: %s", body)
			}
			for _, forbidden := range []string{"event: response.completed\n", "event: response.output_text.done\n", "event: response.output_item.done\n", "event: response.function_call_arguments.done\n"} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("unexpected success event %q: %s", forbidden, body)
				}
			}
			var createdID string
			for _, line := range strings.Split(body, "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				var event struct {
					Type     string `json:"type"`
					Response struct {
						ID, Status string
						Error      struct{ Code, Type, Message string }
					} `json:"response"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
					t.Fatal(err)
				}
				if event.Type == "response.created" {
					createdID = event.Response.ID
				}
				if event.Type == "response.failed" {
					got := event.Response
					if got.Status != "failed" || got.ID == "" || got.ID != createdID || got.Error.Code != tc.wantCode || got.Error.Type != tc.wantType || got.Error.Message != tc.wantMessage {
						t.Fatalf("unexpected failure: %+v", got)
					}
				}
			}
			wantTextDeltas := 0
			if tc.prefix == textChunk {
				wantTextDeltas = 1
			}
			if got := strings.Count(body, "event: response.output_text.delta\n"); got != wantTextDeltas {
				t.Fatalf("text deltas = %d, want %d: %s", got, wantTextDeltas, body)
			}
		})
	}
}

func TestResponsesStreamStillCompletesWithoutError(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	w := httptest.NewRecorder()
	streamResponsesFromChat(w, r, oaiReq{}, "test-model", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\ndata: [DONE]\n\n")
	}, responsesKeepaliveInterval)
	if body := w.Body.String(); strings.Contains(body, "event: response.failed\n") || strings.Count(body, "event: response.completed\n") != 1 {
		t.Fatalf("unexpected completion: %s", body)
	}
}

func TestResponsesStreamFailureStopsProducer(t *testing.T) {
	for _, tc := range []struct{ name, payload, code string }{
		{"error frame", "data: {\"error\":{\"code\":\"upstream_timeout\",\"message\":\"timed out\"}}\n\n", "upstream_timeout"},
		{"oversized frame", "data: " + strings.Repeat("x", 2<<20), "upstream_stream_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			producerDone := make(chan error, 1)
			streamResponsesFromChat(w, r, oaiReq{}, "test-model", func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, tc.payload)
				_, err := fmt.Fprint(w, strings.Repeat("x", 8<<10))
				<-r.Context().Done()
				producerDone <- err
			}, responsesKeepaliveInterval)
			select {
			case err := <-producerDone:
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("producer write error = %v, want closed pipe", err)
				}
			case <-ctx.Done():
				t.Fatal("failed stream did not stop its producer")
			}
			body := w.Body.String()
			if strings.Count(body, "event: response.failed\n") != 1 || !strings.Contains(body, `"code":"`+tc.code+`"`) || strings.Contains(body, "event: response.completed\n") {
				t.Fatalf("unexpected failure stream: %s", body)
			}
		})
	}
}
