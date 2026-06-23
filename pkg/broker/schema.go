package broker

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ValidatePayload validates a JSON payload against a schema map.
func ValidatePayload(payload string, schema map[string]string) error {
	if len(schema) == 0 {
		return nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return fmt.Errorf("invalid payload: not a valid JSON object")
	}

	for field, rule := range schema {
		parts := strings.Split(rule, ",")
		required := false
		var expectedType string

		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "required" {
				required = true
			} else if p != "" {
				expectedType = p
			}
		}

		val, exists := data[field]
		if !exists || val == nil {
			if required {
				return fmt.Errorf("field '%s' is required", field)
			}
			continue
		}

		switch expectedType {
		case "string":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("field '%s' must be a string", field)
			}
		case "int", "integer":
			numVal, ok := val.(float64)
			if !ok {
				return fmt.Errorf("field '%s' must be an integer", field)
			}
			if numVal != float64(int(numVal)) {
				return fmt.Errorf("field '%s' must be an integer", field)
			}
		case "bool", "boolean":
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("field '%s' must be a boolean", field)
			}
		case "float", "number":
			if _, ok := val.(float64); !ok {
				return fmt.Errorf("field '%s' must be a number", field)
			}
		}
	}
	return nil
}
