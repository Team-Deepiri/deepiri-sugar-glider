package config

import (
	"os"
	"strconv"
	"strings"
)

// Preferred env namespace is SUGAR_GLIDER_*; SIDECAR_* remains as a compatibility alias.
func getEnvDual(sugarKey, sidecarKey, fallback string) string {
	if val := os.Getenv(sugarKey); val != "" {
		return val
	}
	if val := os.Getenv(sidecarKey); val != "" {
		return val
	}
	return fallback
}

func getEnvInt64Dual(sugarKey, sidecarKey string, fallback int64) int64 {
	if val := os.Getenv(sugarKey); val != "" {
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			return parsed
		}
	}
	if val := os.Getenv(sidecarKey); val != "" {
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getEnvBoolDual(sugarKey, sidecarKey string, fallback bool) bool {
	if val := strings.TrimSpace(strings.ToLower(os.Getenv(sugarKey))); val != "" {
		switch val {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	if val := strings.TrimSpace(strings.ToLower(os.Getenv(sidecarKey))); val != "" {
		switch val {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}
