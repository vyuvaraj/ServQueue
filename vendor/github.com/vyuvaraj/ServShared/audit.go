package ServShared

import (
	"encoding/json"
	"fmt"
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
	return sc.Put("system-audit-logs", key, data)
}
