package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

const (
	defaultSDK       = "c-holons"
	defaultServerSDK = "go-holons"
	defaultMethod    = "echo.v1.Echo/Ping"
	defaultMessage   = "cert"
	defaultTimeoutMS = 5000
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

type options struct {
	url              string
	sdk              string
	serverSDK        string
	method           string
	params           map[string]interface{}
	expectedErrorIDs []int
	timeoutMS        int
	connectOnly      bool
}

func main() {
	args, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	timeout := time.Duration(args.timeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultTimeoutMS * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startedAt := time.Now()

	conn, _, err := websocket.Dial(ctx, args.url, &websocket.DialOptions{Subprotocols: []string{"holon-rpc"}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	if conn.Subprotocol() != "holon-rpc" {
		_ = conn.Close(websocket.StatusProtocolError, "missing holon-rpc subprotocol")
		fmt.Fprintln(os.Stderr, "server did not negotiate holon-rpc")
		os.Exit(1)
	}

	if args.connectOnly {
		mustWriteJSON(map[string]interface{}{
			"status":     "pass",
			"sdk":        args.sdk,
			"server_sdk": args.serverSDK,
			"latency_ms": time.Since(startedAt).Milliseconds(),
			"check":      "connect",
		})
		return
	}

	req := rpcMessage{
		JSONRPC: "2.0",
		ID:      "c1",
		Method:  args.method,
		Params:  args.params,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode request failed: %v\n", err)
		os.Exit(1)
	}

	if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
		fmt.Fprintf(os.Stderr, "write failed: %v\n", err)
		os.Exit(1)
	}

	_, respData, err := conn.Read(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read failed: %v\n", err)
		os.Exit(1)
	}

	var resp rpcMessage
	if err := json.Unmarshal(respData, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "decode response failed: %v\n", err)
		os.Exit(1)
	}

	if resp.JSONRPC != "2.0" {
		fmt.Fprintf(os.Stderr, "unexpected jsonrpc version: %q\n", resp.JSONRPC)
		os.Exit(1)
	}
	if fmt.Sprint(resp.ID) != fmt.Sprint(req.ID) {
		fmt.Fprintf(os.Stderr, "unexpected response id: %v\n", resp.ID)
		os.Exit(1)
	}

	if resp.Error != nil {
		if containsInt(args.expectedErrorIDs, resp.Error.Code) {
			mustWriteJSON(map[string]interface{}{
				"status":     "pass",
				"sdk":        args.sdk,
				"server_sdk": args.serverSDK,
				"latency_ms": time.Since(startedAt).Milliseconds(),
				"method":     args.method,
				"error_code": resp.Error.Code,
			})
			return
		}

		fmt.Fprintf(os.Stderr, "rpc error: %d %s\n", resp.Error.Code, resp.Error.Message)
		os.Exit(1)
	}

	if len(args.expectedErrorIDs) > 0 {
		fmt.Fprintf(os.Stderr, "expected one of error codes %v, but call succeeded\n", args.expectedErrorIDs)
		os.Exit(1)
	}

	if args.method == defaultMethod {
		expected := fmt.Sprint(args.params["message"])
		actual := fmt.Sprint(resp.Result["message"])
		if actual != expected {
			fmt.Fprintf(os.Stderr, "unexpected echo response: %s\n", string(respData))
			os.Exit(1)
		}
	}

	mustWriteJSON(map[string]interface{}{
		"status":     "pass",
		"sdk":        args.sdk,
		"server_sdk": args.serverSDK,
		"latency_ms": time.Since(startedAt).Milliseconds(),
		"method":     args.method,
	})
}

func parseFlags() (options, error) {
	var out options
	var paramsJSON string
	var expectError string
	out.sdk = defaultSDK
	out.serverSDK = defaultServerSDK
	out.method = defaultMethod
	out.timeoutMS = defaultTimeoutMS

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		token := args[i]

		if token == "--connect-only" {
			out.connectOnly = true
			continue
		}
		if token == "--sdk" {
			value, err := requireValue(args, i, "--sdk")
			if err != nil {
				return options{}, err
			}
			out.sdk = value
			i++
			continue
		}
		if token == "--server-sdk" {
			value, err := requireValue(args, i, "--server-sdk")
			if err != nil {
				return options{}, err
			}
			out.serverSDK = value
			i++
			continue
		}
		if token == "--method" {
			value, err := requireValue(args, i, "--method")
			if err != nil {
				return options{}, err
			}
			out.method = value
			i++
			continue
		}
		if token == "--message" {
			value, err := requireValue(args, i, "--message")
			if err != nil {
				return options{}, err
			}
			out.params = map[string]interface{}{"message": value}
			i++
			continue
		}
		if token == "--params-json" {
			value, err := requireValue(args, i, "--params-json")
			if err != nil {
				return options{}, err
			}
			paramsJSON = value
			i++
			continue
		}
		if token == "--expect-error" {
			value, err := requireValue(args, i, "--expect-error")
			if err != nil {
				return options{}, err
			}
			expectError = value
			i++
			continue
		}
		if token == "--timeout-ms" {
			value, err := requireValue(args, i, "--timeout-ms")
			if err != nil {
				return options{}, err
			}
			timeout, err := strconv.Atoi(value)
			if err != nil || timeout <= 0 {
				return options{}, fmt.Errorf("--timeout-ms must be a positive integer")
			}
			out.timeoutMS = timeout
			i++
			continue
		}
		if strings.HasPrefix(token, "--") {
			return options{}, fmt.Errorf("unknown flag: %s", token)
		}
		if out.url != "" {
			return options{}, fmt.Errorf("unexpected argument: %s", token)
		}
		out.url = token
	}

	if out.url == "" {
		return options{}, fmt.Errorf("usage: go_holonrpc_client.go <ws://host:port/rpc> [flags]")
	}

	if out.params == nil {
		out.params = map[string]interface{}{}
	}

	params, err := parseParams(paramsJSON, out.method, fmt.Sprint(out.params["message"]))
	if err != nil {
		return options{}, err
	}

	codes, err := parseExpectedCodes(expectError)
	if err != nil {
		return options{}, err
	}

	out.params = params
	out.expectedErrorIDs = codes
	return out, nil
}

func parseParams(raw string, method string, message string) (map[string]interface{}, error) {
	if strings.TrimSpace(raw) == "" {
		if method == defaultMethod {
			if message == "" || message == "<nil>" {
				message = defaultMessage
			}
			return map[string]interface{}{"message": message}, nil
		}
		return map[string]interface{}{}, nil
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("--params-json must be valid JSON object: %w", err)
	}
	if parsed == nil {
		return nil, fmt.Errorf("--params-json must decode to a JSON object")
	}
	return parsed, nil
}

func parseExpectedCodes(raw string) ([]int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	tokens := strings.Split(trimmed, ",")
	codes := make([]int, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		code, err := strconv.Atoi(token)
		if err != nil {
			return nil, fmt.Errorf("invalid error code in --expect-error: %s", token)
		}
		codes = append(codes, code)
	}

	if len(codes) == 0 {
		return nil, fmt.Errorf("--expect-error requires at least one numeric code")
	}
	return codes, nil
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func mustWriteJSON(value interface{}) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fmt.Fprintf(os.Stderr, "encode output failed: %v\n", err)
		os.Exit(1)
	}
}

func requireValue(args []string, index int, flagName string) (string, error) {
	if index+1 >= len(args) {
		return "", fmt.Errorf("missing value for %s", flagName)
	}
	value := strings.TrimSpace(args[index+1])
	if value == "" {
		return "", fmt.Errorf("missing value for %s", flagName)
	}
	return value, nil
}
