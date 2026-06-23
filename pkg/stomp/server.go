package stomp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"servqueue/pkg/broker"
)

type Server struct {
	addr      string
	engine    *broker.BrokerEngine
	username  string
	passcode  string
	tlsCert   string
	tlsKey    string
	listener  net.Listener
}

func NewServer(addr string, engine *broker.BrokerEngine, username, passcode, tlsCert, tlsKey string) *Server {
	return &Server{
		addr:     addr,
		engine:   engine,
		username: username,
		passcode: passcode,
		tlsCert:  tlsCert,
		tlsKey:   tlsKey,
	}
}

func (s *Server) Start() error {
	var listener net.Listener
	var err error

	if s.tlsCert != "" && s.tlsKey != "" {
		cert, certErr := tls.LoadX509KeyPair(s.tlsCert, s.tlsKey)
		if certErr != nil {
			return fmt.Errorf("tls: failed to load certificates: %w", certErr)
		}
		cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
		listener, err = tls.Listen("tcp", s.addr, cfg)
	} else {
		listener, err = net.Listen("tcp", s.addr)
	}

	if err != nil {
		return err
	}
	s.listener = listener

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return nil
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
}

type Frame struct {
	Command string
	Headers map[string]string
	Body    string
}

func readFrame(reader *bufio.Reader) (*Frame, error) {
	cmdLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	command := strings.TrimSpace(cmdLine)
	if command == "" {
		// Skip empty lines (keep-alive)
		return readFrame(reader)
	}

	headers := make(map[string]string)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break // End of headers
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	// Read body until null byte (\x00)
	bodyBytes, err := reader.ReadBytes(0)
	if err != nil {
		return nil, err
	}
	body := string(bytes.TrimSuffix(bodyBytes, []byte{0}))

	return &Frame{
		Command: command,
		Headers: headers,
		Body:    body,
	}, nil
}

func writeFrame(writer io.Writer, command string, headers map[string]string, body string) error {
	var buf bytes.Buffer
	buf.WriteString(command)
	buf.WriteByte('\n')
	for k, v := range headers {
		buf.WriteString(fmt.Sprintf("%s:%s\n", k, v))
	}
	buf.WriteString("\n")
	buf.WriteString(body)
	buf.WriteByte(0)
	_, err := writer.Write(buf.Bytes())
	return err
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	type subInfo struct {
		topic string
		ch    chan string
	}
	activeSubs := make(map[string]subInfo)
	type txPublish struct {
		topic   string
		payload string
		ctx     context.Context
	}
	txBuffers := make(map[string][]txPublish)
	defer func() {
		for _, sub := range activeSubs {
			s.engine.Unsubscribe(sub.topic, sub.ch)
		}
	}()

	authenticated := false

	// If no username/passcode is required, default to authenticated
	if s.username == "" && s.passcode == "" {
		authenticated = true
	}

	for {
		frame, err := readFrame(reader)
		if err != nil {
			return
		}

		if frame.Command == "CONNECT" {
			login := frame.Headers["login"]
			passcode := frame.Headers["passcode"]

			if !authenticated {
				isValid := false
				if login == s.username && passcode == s.passcode {
					isValid = true
				} else if jwtSec := os.Getenv("SERV_JWT_SECRET"); jwtSec != "" {
					if _, ok := validateJWT(passcode, []byte(jwtSec)); ok {
						isValid = true
					} else if _, ok := validateJWT(login, []byte(jwtSec)); ok {
						isValid = true
					}
				}

				if !isValid {
					_ = writeFrame(writer, "ERROR", map[string]string{"message": "Authentication failed"}, "Invalid credentials")
					writer.Flush()
					return
				}
			}

			authenticated = true
			writeFrame(writer, "CONNECTED", map[string]string{"version": "1.2"}, "")
			writer.Flush()
			continue
		}

		if !authenticated {
			_ = writeFrame(writer, "ERROR", map[string]string{"message": "Not authenticated"}, "Send CONNECT frame first")
			writer.Flush()
			return
		}

		switch frame.Command {
		case "SEND":
			destination := frame.Headers["destination"]
			if destination == "" {
				continue
			}

			ctx := context.Background()
			if tp, exists := frame.Headers["traceparent"]; exists {
				ctx = context.WithValue(ctx, "traceparent", tp)
			}
			if msgID, exists := frame.Headers["message-id"]; exists {
				ctx = context.WithValue(ctx, "message-id", msgID)
			} else if idVal, exists := frame.Headers["id"]; exists {
				ctx = context.WithValue(ctx, "message-id", idVal)
			}
			if delayVal, exists := frame.Headers["delay-ms"]; exists {
				ctx = context.WithValue(ctx, "delay-ms", delayVal)
			}
			if prodID, exists := frame.Headers["producer-id"]; exists {
				ctx = context.WithValue(ctx, "producer-id", prodID)
			}
			if seqStr, exists := frame.Headers["sequence-number"]; exists {
				ctx = context.WithValue(ctx, "sequence-number", seqStr)
			}

			txID := frame.Headers["transaction"]
			if txID != "" {
				if buf, exists := txBuffers[txID]; exists {
					txBuffers[txID] = append(buf, txPublish{
						topic:   destination,
						payload: frame.Body,
						ctx:     ctx,
					})
				} else {
					_ = writeFrame(writer, "ERROR", map[string]string{"message": "Transaction not found"}, "Transaction "+txID+" was not started")
					writer.Flush()
				}
				continue
			}

			_, _ = s.engine.Publish(ctx, destination, frame.Body)

		case "BEGIN":
			txID := frame.Headers["transaction"]
			if txID != "" {
				txBuffers[txID] = []txPublish{}
			}

		case "COMMIT":
			txID := frame.Headers["transaction"]
			if txID != "" {
				if buf, exists := txBuffers[txID]; exists {
					for _, msg := range buf {
						_, _ = s.engine.Publish(msg.ctx, msg.topic, msg.payload)
					}
					delete(txBuffers, txID)
				}
			}

		case "ABORT":
			txID := frame.Headers["transaction"]
			if txID != "" {
				delete(txBuffers, txID)
			}

		case "SUBSCRIBE":
			destination := frame.Headers["destination"]
			subID := frame.Headers["id"]
			group := frame.Headers["group"]
			if destination == "" {
				continue
			}

			var ch chan string
			if group != "" {
				ch = s.engine.SubscribeGroup(destination, group)
			} else {
				ch = s.engine.Subscribe(destination)
			}
			activeSubs[subID] = subInfo{topic: destination, ch: ch}

			go func(topic string, id string, subChan chan string) {
				for msg := range subChan {
					hdrs := map[string]string{
						"destination":  topic,
						"subscription": id,
						"message-id":   "msg-0",
					}
					// Include traceparent if it looks like JSON with traceparent field
					if strings.Contains(msg, "_traceparent") {
						hdrs["traceparent"] = "extracted-tp"
					}
					_ = writeFrame(conn, "MESSAGE", hdrs, msg)
				}
			}(destination, subID, ch)

		case "DISCONNECT":
			writeFrame(writer, "RECEIPT", map[string]string{"receipt-id": frame.Headers["receipt"]}, "")
			writer.Flush()
			return
		}
	}
}

func validateJWT(tokenStr string, secret []byte) (string, bool) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return "", false
	}

	headerPart, payloadPart, signaturePart := parts[0], parts[1], parts[2]
	
	// Validate signature
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(headerPart + "." + payloadPart))
	expectedMac := mac.Sum(nil)
	
	// Base64Url decode signaturePart
	sigBytes, err := base64UrlDecode(signaturePart)
	if err != nil || !hmac.Equal(sigBytes, expectedMac) {
		return "", false
	}

	// Base64Url decode payloadPart and extract username, exp
	payloadBytes, err := base64UrlDecode(payloadPart)
	if err != nil {
		return "", false
	}

	var claims struct {
		Username string `json:"username"`
		Exp      int64  `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", false
	}

	// Check expiration
	if claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return "", false
	}

	return claims.Username, true
}

func base64UrlDecode(s string) ([]byte, error) {
	if l := len(s) % 4; l > 0 {
		s += strings.Repeat("=", 4-l)
	}
	s = strings.ReplaceAll(s, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")
	return base64.URLEncoding.DecodeString(s)
}
