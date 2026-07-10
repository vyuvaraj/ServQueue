package ServShared

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

type AuditEvent struct {
	Timestamp time.Time              `json:"timestamp"`
	Service   string                 `json:"service"`
	Action    string                 `json:"action"`
	Actor     string                 `json:"actor"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

// EmitAuditEvent writes an audit event JSON payload to the system-audit-logs bucket in ServStore.
func EmitAuditEvent(service, action, actor string, details map[string]interface{}) error {
	event := AuditEvent{
		Timestamp: time.Now(),
		Service:   service,
		Action:    action,
		Actor:     actor,
		Details:   details,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	sc := NewStoreClient()
	key := fmt.Sprintf("audit-%d.json", event.Timestamp.UnixNano())
	putErr := sc.Put("system-audit-logs", key, data)
	if putErr != nil {
		return putErr
	}

	// Enforce storage retention options if configured
	retentionDaysStr := os.Getenv("SERV_AUDIT_RETENTION_DAYS")
	if retentionDaysStr != "" {
		if days, err := strconv.Atoi(retentionDaysStr); err == nil && days > 0 {
			go func() {
				urlStr := fmt.Sprintf("%s/system-audit-logs", sc.Endpoint)
				req, err := http.NewRequest("GET", urlStr, nil)
				if err != nil {
					return
				}
				if sc.AuthToken != "" {
					req.Header.Set("Authorization", "Bearer "+sc.AuthToken)
				}
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Do(req)
				if err != nil {
					return
				}
				defer resp.Body.Close()
				bodyBytes, _ := io.ReadAll(resp.Body)

				re := regexp.MustCompile(`<Key>(audit-\d+\.json)</Key>`)
				matches := re.FindAllStringSubmatch(string(bodyBytes), -1)
				retentionLimit := time.Duration(days) * 24 * time.Hour
				for _, m := range matches {
					k := m[1]
					var unixNano int64
					_, sscanfErr := fmt.Sscanf(k, "audit-%d.json", &unixNano)
					if sscanfErr == nil {
						t := time.Unix(0, unixNano)
						if time.Since(t) > retentionLimit {
							_ = sc.Delete("system-audit-logs", k)
						}
					}
				}
			}()
		}
	}

	return nil
}
