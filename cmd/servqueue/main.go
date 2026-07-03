package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type TopicInfo struct {
	Name         string `json:"name"`
	Subscribers  int    `json:"subscribers"`
	Partitions   int    `json:"partitions"`
	HasTransform bool   `json:"has_transform"`
	DLQTopic     string `json:"dlq_topic,omitempty"`
}

func main() {
	serverAddr := flag.String("server", "http://localhost:8082", "ServQueue HTTP server address")
	token := flag.String("token", "secret-token", "API Auth Token")
	tenant := flag.String("tenant", "", "Tenant ID for multi-tenant isolation")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	switch cmd {
	case "topics":
		if len(args) < 2 {
			fmt.Println("Error: topics command requires a subcommand (list, create)")
			os.Exit(1)
		}
		subCmd := args[1]
		switch subCmd {
		case "list":
			listTopics(*serverAddr, *token, *tenant)
		case "create":
			if len(args) < 3 {
				fmt.Println("Error: create requires a topic name")
				os.Exit(1)
			}
			createTopic(*serverAddr, *token, *tenant, args[2])
		default:
			fmt.Printf("Unknown topics subcommand: %s\n", subCmd)
			os.Exit(1)
		}
	case "publish":
		if len(args) < 3 {
			fmt.Println("Error: publish requires a topic and payload")
			os.Exit(1)
		}
		topic := args[1]
		payload := args[2]
		
		keyFlag := flag.NewFlagSet("publish", flag.ExitOnError)
		key := keyFlag.String("key", "", "Deduplication/Compaction key")
		priority := keyFlag.Int("priority", 0, "Priority level (higher first)")
		ttl := keyFlag.Duration("ttl", 0, "Time-to-live duration (e.g. 5s, 1m)")
		keyFlag.Parse(args[3:])

		publishMessage(*serverAddr, *token, *tenant, topic, payload, *key, *priority, *ttl)
	case "consume":
		if len(args) < 2 {
			fmt.Println("Error: consume requires a topic")
			os.Exit(1)
		}
		topic := args[1]
		
		consumeFlag := flag.NewFlagSet("consume", flag.ExitOnError)
		group := consumeFlag.String("group", "", "Consumer group name")
		consumeFlag.Parse(args[2:])

		consumeMessages(*serverAddr, *token, *tenant, topic, *group)
	case "tail":
		if len(args) < 2 {
			fmt.Println("Error: tail requires a topic")
			os.Exit(1)
		}
		topic := args[1]
		
		tailFlag := flag.NewFlagSet("tail", flag.ExitOnError)
		filter := tailFlag.String("filter", "", "Regex filter for message payload")
		tailFlag.Parse(args[2:])

		tailMessages(*serverAddr, *token, *tenant, topic, *filter)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: servqueue [options] <command> [args]")
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println("\nCommands:")
	fmt.Println("  topics list                     List all topics")
	fmt.Println("  topics create <name>            Create a new topic (by registering schema or empty)")
	fmt.Println("  publish <topic> <payload>       Publish a message to a topic")
	fmt.Println("    [--key <key>]                 Specify deduplication/compaction key")
	fmt.Println("    [--priority <num>]            Specify message priority")
	fmt.Println("    [--ttl <duration>]            Specify TTL (e.g., 10s)")
	fmt.Println("  consume <topic>                 Consume messages from a topic")
	fmt.Println("    [--group <group>]             Consume as part of a consumer group")
	fmt.Println("  tail <topic>                    Stream live messages from a topic")
	fmt.Println("    [--filter <regex>]            Filter message payloads by regex")
}

func doRequest(method, urlStr, token, tenant string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	return client.Do(req)
}

func listTopics(server, token, tenant string) {
	url := fmt.Sprintf("%s/api/v1/topics", server)
	resp, err := doRequest("GET", url, token, tenant, nil)
	if err != nil {
		fmt.Printf("HTTP request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Server returned error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var data struct {
		Topics []TopicInfo `json:"topics"`
		Count  int         `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		fmt.Printf("Failed to parse JSON response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d topics:\n", data.Count)
	for _, t := range data.Topics {
		fmt.Printf(" - %s (subscribers: %d, partitions: %d, transform: %t)\n", t.Name, t.Subscribers, t.Partitions, t.HasTransform)
	}
}

func createTopic(server, token, tenant, topic string) {
	// Creating topic is done via Schema registration (empty schema is fine to initialize it)
	url := fmt.Sprintf("%s/api/v1/topics/%s/schema", server, topic)
	schema := map[string]string{}
	bodyBytes, _ := json.Marshal(schema)

	resp, err := doRequest("POST", url, token, tenant, bytes.NewBuffer(bodyBytes))
	if err != nil {
		fmt.Printf("HTTP request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Server returned error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}
	fmt.Printf("Topic %q created successfully.\n", topic)
}

func publishMessage(server, token, tenant, topic, payload, key string, priority int, ttl time.Duration) {
	url := fmt.Sprintf("%s/api/v1/publish", server)
	
	reqBody := map[string]interface{}{
		"topic":   topic,
		"payload": payload,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		fmt.Printf("Failed to create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	if key != "" {
		req.Header.Set("Message-Key", key) // Custom header or processed in context?
	}
	if priority != 0 {
		req.Header.Set("Priority", fmt.Sprintf("%d", priority))
	}
	if ttl > 0 {
		req.Header.Set("TTL", ttl.String())
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("HTTP request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Server returned error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var resData map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&resData)
	fmt.Println("Message published successfully.")
	if dp, ok := resData["delivered_payload"]; ok {
		fmt.Printf("Delivered Payload: %v\n", dp)
	}
}

func consumeMessages(server, token, tenant, topic, group string) {
	url := fmt.Sprintf("%s/api/v1/replay", server)
	reqBody := map[string]interface{}{
		"topic":  topic,
		"offset": int64(0),
	}
	if group != "" {
		reqBody["group"] = group
	}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := doRequest("POST", url, token, tenant, bytes.NewBuffer(bodyBytes))
	if err != nil {
		fmt.Printf("HTTP request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Server returned error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	var res struct {
		Status  string   `json:"status"`
		Topic   string   `json:"topic"`
		Records []string `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		fmt.Printf("Failed to parse JSON response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Consumed %d messages from topic %s:\n", len(res.Records), res.Topic)
	for i, r := range res.Records {
		fmt.Printf("[%d] %s\n", i, r)
	}
}

func tailMessages(serverAddr, token, tenant, topic, filterStr string) {
	wsURL := strings.Replace(serverAddr, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = fmt.Sprintf("%s/api/v1/tail?topic=%s", wsURL, url.QueryEscape(topic))
	if filterStr != "" {
		wsURL = fmt.Sprintf("%s&filter=%s", wsURL, url.QueryEscape(filterStr))
	}

	dialer := websocket.DefaultDialer
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+token)
	if tenant != "" {
		headers.Set("X-Tenant-ID", tenant)
	}

	conn, _, err := dialer.Dial(wsURL, headers)
	if err != nil {
		fmt.Printf("Failed to connect to tail stream: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Streaming live messages from topic %s...\n", topic)
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("Connection closed: %v\n", err)
			break
		}

		var jsonObj interface{}
		if err := json.Unmarshal(message, &jsonObj); err == nil {
			pretty, _ := json.MarshalIndent(jsonObj, "", "  ")
			fmt.Println(string(pretty))
		} else {
			fmt.Println(string(message))
		}
	}
}
