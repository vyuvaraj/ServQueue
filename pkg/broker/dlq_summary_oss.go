//go:build !enterprise

package broker

import (
	"errors"
)

func (e *BrokerEngine) SummarizeDLQ(topic string) (map[string]interface{}, error) {
	return nil, errors.New("Enterprise Edition required for DLQ auto-summarization")
}

func (e *BrokerEngine) DetectMessageAnomalies(topic string) (map[string]interface{}, error) {
	return nil, errors.New("Enterprise Edition required for message pattern anomaly detection")
}
