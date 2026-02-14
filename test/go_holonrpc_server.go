package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcMessage struct {
	JSONRPC string                 `json:"jsonrpc,omitempty"`
	ID      interface{}            `json:"id,omitempty"`
	Method  string                 `json:"method,omitempty"`
	Params  map[string]interface{} `json:"params,omitempty"`
	Result  map[string]interface{} `json:"result,omitempty"`
	Error   *rpcError              `json:"error,omitempty"`
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen failed: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	handled := make(chan struct{})
	var once sync.Once
	markHandled := func() {
		once.Do(func() {
			close(handled)
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"holon-rpc"}})
		if acceptErr != nil {
			markHandled()
			return
		}
		defer markHandled()
		defer conn.Close(websocket.StatusNormalClosure, "done")

		ctx := r.Context()
		for {
			_, data, readErr := conn.Read(ctx)
			if readErr != nil {
				return
			}

			var msg rpcMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				_ = writeError(ctx, conn, nil, -32700, "parse error")
				continue
			}
			if msg.JSONRPC != "2.0" {
				_ = writeError(ctx, conn, msg.ID, -32600, "invalid request")
				continue
			}

			switch msg.Method {
			case "rpc.heartbeat":
				_ = writeResult(ctx, conn, msg.ID, map[string]interface{}{})
			case "echo.v1.Echo/Ping":
				params := msg.Params
				if params == nil {
					params = map[string]interface{}{}
				}
				_ = writeResult(ctx, conn, msg.ID, map[string]interface{}{
					"message": params["message"],
					"sdk":     "go-holons",
					"version": "0.3.0",
				})
			default:
				_ = writeError(ctx, conn, msg.ID, -32601, fmt.Sprintf("method %q not found", msg.Method))
			}
		}
	})

	server := &http.Server{Handler: mux}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "serve failed: %v\n", serveErr)
		}
	}()

	fmt.Printf("ws://%s/rpc\n", listener.Addr().String())

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func writeResult(ctx context.Context, conn *websocket.Conn, id interface{}, result map[string]interface{}) error {
	payload, err := json.Marshal(rpcMessage{JSONRPC: "2.0", ID: id, Result: result})
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func writeError(ctx context.Context, conn *websocket.Conn, id interface{}, code int, message string) error {
	payload, err := json.Marshal(rpcMessage{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}})
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}
