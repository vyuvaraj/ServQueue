package ServShared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

type MailSendRequest struct {
	Channel  string                 `json:"channel"`
	Target   string                 `json:"target"`
	Template string                 `json:"template"`
	Version  string                 `json:"version,omitempty"`
	Category string                 `json:"category,omitempty"`
	Context  map[string]interface{} `json:"context"`
}

// MailSend forwards an email request to the ServMail microservice.
func MailSend(to string, template string, data map[string]interface{}) error {
	endpoint := os.Getenv("SERV_MAIL_URL")
	if endpoint == "" {
		endpoint = "http://localhost:8094"
	}
	endpoint = strings.TrimSuffix(endpoint, "/") + "/api/mail/send"

	reqPayload := MailSendRequest{
		Channel:  "email",
		Target:   to,
		Template: template,
		Context:  data,
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Set auth token if configured
	token := os.Getenv("SERV_STORE_TOKEN")
	if token == "" {
		token = "gateway-secret-token"
	}
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to send mail: status %d", resp.StatusCode)
	}
	return nil
}

// Notify forwards a generic channel notification (e.g. slack or sms) to the ServMail microservice.
func Notify(channel string, target string, message string) error {
	endpoint := os.Getenv("SERV_MAIL_URL")
	if endpoint == "" {
		endpoint = "http://localhost:8094"
	}
	endpoint = strings.TrimSuffix(endpoint, "/") + "/api/mail/send"

	reqPayload := MailSendRequest{
		Channel:  channel,
		Target:   target,
		Template: "{{.message}}",
		Context:  map[string]interface{}{"message": message},
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	token := os.Getenv("SERV_STORE_TOKEN")
	if token == "" {
		token = "gateway-secret-token"
	}
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to notify: status %d", resp.StatusCode)
	}
	return nil
}
