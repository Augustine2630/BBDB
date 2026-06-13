package logger

import (
	"fmt"
	"os"

	"BBDB/internal/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Init builds a zap logger from cfg and installs it as the global logger.
// All packages should call zap.L() after Init is called.
func Init(cfg config.LogConfig) error {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return err
	}

	enc, err := buildEncoder(cfg.Format)
	if err != nil {
		return err
	}

	cores := []zapcore.Core{
		zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level),
	}

	if cfg.File.Path != "" {
		lj := &lumberjack.Logger{
			Filename:   cfg.File.Path,
			MaxSize:    cfg.File.MaxSizeMB,
			MaxBackups: cfg.File.MaxBackups,
			MaxAge:     cfg.File.MaxAgeDays,
			Compress:   cfg.File.Compress,
		}
		cores = append(cores, zapcore.NewCore(enc, zapcore.AddSync(lj), level))
	}

	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	zap.ReplaceGlobals(logger)
	zap.RedirectStdLog(logger)
	return nil
}

// Sync flushes buffered log entries. Call on shutdown.
func Sync() error {
	return zap.L().Sync()
}

func parseLevel(s string) (zapcore.Level, error) {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("unknown log level %q: %w", s, err)
	}
	return l, nil
}

func buildEncoder(format string) (zapcore.Encoder, error) {
	switch format {
	case "text":
		return zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), nil
	case "json":
		return zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), nil
	case "json-ecs":
		encCfg := zap.NewProductionEncoderConfig()
		encCfg.TimeKey = "@timestamp"
		encCfg.MessageKey = "message"
		encCfg.LevelKey = "log.level"
		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		return zapcore.NewJSONEncoder(encCfg), nil
	default:
		return nil, fmt.Errorf("unknown log format %q: must be text, json, or json-ecs", format)
	}
}
