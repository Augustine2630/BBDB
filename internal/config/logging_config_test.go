package config_test

import (
	"os"
	"testing"

	"BBDB/internal/config"
)

func TestLoad_LogDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("expected default level=info, got %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("expected default format=json, got %q", cfg.Log.Format)
	}
	if cfg.Log.File.Path != "" {
		t.Errorf("expected default file path empty, got %q", cfg.Log.File.Path)
	}
}

func TestLoad_LogFromYAML(t *testing.T) {
	yaml := `
log:
  level: debug
  format: text
  file:
    path: /tmp/bbdb-test.log
    max_size_mb: 50
    max_backups: 3
    max_age_days: 7
    compress: false
`
	f, err := os.CreateTemp(t.TempDir(), "bbdb*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(yaml); err != nil {
		t.Fatal(err)
	}
	f.Close()

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("expected level=debug, got %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("expected format=text, got %q", cfg.Log.Format)
	}
	if cfg.Log.File.Path != "/tmp/bbdb-test.log" {
		t.Errorf("expected file path, got %q", cfg.Log.File.Path)
	}
	if cfg.Log.File.MaxSizeMB != 50 {
		t.Errorf("expected max_size_mb=50, got %d", cfg.Log.File.MaxSizeMB)
	}
}
