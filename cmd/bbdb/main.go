package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"BBDB/internal/config"
	"BBDB/internal/logger"
	"BBDB/internal/server"
)

var version = "dev"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var cfgFile string

	root := &cobra.Command{
		Use:               "bbdb",
		Short:             "BigBrotherDB — append-only telecom event storage engine",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}

	root.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "path to config YAML file")

	root.AddCommand(newStartCmd(&cfgFile))
	root.AddCommand(newVersionCmd())

	return root
}

func newStartCmd(cfgFile *string) *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the BBDB server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(*cfgFile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			if err := logger.Init(cfg.Log); err != nil {
				return fmt.Errorf("init logger: %w", err)
			}
			defer logger.Sync() //nolint:errcheck

			zap.L().Info("bbdb starting", zap.String("version", version))

			srv, err := server.New(cfg)
			if err != nil {
				return fmt.Errorf("init server: %w", err)
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()

			zap.L().Info("bbdb ready")
			if err := srv.Run(ctx); err != nil {
				zap.L().Error("fatal error", zap.Error(err))
				return err
			}
			zap.L().Info("bbdb stopped")
			return nil
		},
	}
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "bbdb %s\n", version)
		},
	}
}
