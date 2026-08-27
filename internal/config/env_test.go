package config

import "testing"

func TestLoad_PrefersSugarGliderEnvOverSidecar(t *testing.T) {
	setBaselineEnv(t)
	t.Setenv("SUGAR_GLIDER_SERVICE_NAME", "sugar-glider")
	t.Setenv("SIDECAR_SERVICE_NAME", "legacy-sidecar")
	t.Setenv("SUGAR_GLIDER_WAL_MAX_BYTES", "1048576")
	t.Setenv("SUGAR_GLIDER_READY_MAX_WAL_DEPTH", "42")
	t.Setenv("SUGAR_GLIDER_READY_MAX_PUBLISH_QUEUE_DEPTH", "7")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceName != "sugar-glider" {
		t.Fatalf("ServiceName = %q, want sugar-glider", cfg.ServiceName)
	}
	if cfg.WALMaxBytes != 1048576 {
		t.Fatalf("WALMaxBytes = %d, want 1048576", cfg.WALMaxBytes)
	}
	if cfg.ReadyMaxWALDepth != 42 {
		t.Fatalf("ReadyMaxWALDepth = %d, want 42", cfg.ReadyMaxWALDepth)
	}
	if cfg.ReadyMaxPublishQueueDepth != 7 {
		t.Fatalf("ReadyMaxPublishQueueDepth = %d, want 7", cfg.ReadyMaxPublishQueueDepth)
	}
}

func TestLoad_FallsBackToSidecarEnv(t *testing.T) {
	setBaselineEnv(t)
	t.Setenv("SUGAR_GLIDER_SERVICE_NAME", "")
	t.Setenv("SIDECAR_SERVICE_NAME", "legacy-sidecar")
	t.Setenv("SIDECAR_WAL_MAX_BYTES", "2048")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceName != "legacy-sidecar" {
		t.Fatalf("ServiceName = %q, want legacy-sidecar", cfg.ServiceName)
	}
	if cfg.WALMaxBytes != 2048 {
		t.Fatalf("WALMaxBytes = %d, want 2048", cfg.WALMaxBytes)
	}
}
