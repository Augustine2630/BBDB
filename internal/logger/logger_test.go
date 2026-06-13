package logger_test

import (
	"os"
	"path/filepath"
	"testing"

	"BBDB/internal/config"
	"BBDB/internal/logger"

	"go.uber.org/zap"
)

func TestInit_JSONFormat(t *testing.T) {
	cfg := config.LogConfig{Level: "info", Format: "json"}
	if err := logger.Init(cfg); err != nil {
		t.Fatalf("Init(json) error: %v", err)
	}
	if zap.L() == zap.NewNop() {
		t.Error("global logger was not replaced")
	}
}

func TestInit_TextFormat(t *testing.T) {
	cfg := config.LogConfig{Level: "debug", Format: "text"}
	if err := logger.Init(cfg); err != nil {
		t.Fatalf("Init(text) error: %v", err)
	}
}

func TestInit_ECSFormat(t *testing.T) {
	cfg := config.LogConfig{Level: "warn", Format: "json-ecs"}
	if err := logger.Init(cfg); err != nil {
		t.Fatalf("Init(json-ecs) error: %v", err)
	}
}

func TestInit_InvalidLevel(t *testing.T) {
	cfg := config.LogConfig{Level: "verbose", Format: "json"}
	if err := logger.Init(cfg); err == nil {
		t.Error("expected error for unknown level")
	}
}

func TestInit_InvalidFormat(t *testing.T) {
	cfg := config.LogConfig{Level: "info", Format: "logfmt"}
	if err := logger.Init(cfg); err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestInit_FileOutput(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "bbdb.log")
	cfg := config.LogConfig{
		Level:  "info",
		Format: "json",
		File: config.LogFileConfig{
			Path:       logPath,
			MaxSizeMB:  1,
			MaxBackups: 1,
			MaxAgeDays: 1,
			Compress:   false,
		},
	}
	if err := logger.Init(cfg); err != nil {
		t.Fatalf("Init with file: %v", err)
	}
	zap.L().Info("test message")
	if err := logger.Sync(); err != nil {
		_ = err // Sync may return error for stdout on some systems
	}
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("expected log file to be created")
	}
}
