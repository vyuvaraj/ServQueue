package middleware

import (
	"regexp"
)

var (
	// Redacts potential secrets / authorization strings.
	secretRegex = regexp.MustCompile(`(?i)(bearer\s+[a-zA-Z0-9\-\._~\+\/]+=*|password|secret|token|apikey)`)
)

// SanitizeLog matches logging parameters and filters potential secrets.
func SanitizeLog(input string) string {
	return secretRegex.ReplaceAllString(input, "[REDACTED]")
}
