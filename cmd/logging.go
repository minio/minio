package cmd

import (
	"context"
	"errors"

	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/logger"
)

func proxyLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "proxy", err, errKind...)
}

func replLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "replication", err, errKind...)
}

func replLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "replication", err, id, errKind...)
}

func iamLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "iam", err, id, errKind...)
}

func iamLogIf(ctx context.Context, err error, errKind ...any) {
	if !errors.Is(err, grid.ErrDisconnected) {
		logger.LogIf(ctx, "iam", err, errKind...)
	}
}

func iamLogEvent(ctx context.Context, msg string, args ...any) {
	logger.Event(ctx, "iam", msg, args...)
}

func rebalanceLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "rebalance", err, errKind...)
}

func rebalanceLogEvent(ctx context.Context, msg string, args ...any) {
	logger.Event(ctx, "rebalance", msg, args...)
}

func adminLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "admin", err, errKind...)
}

func authNLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "authN", err, errKind...)
}

func authZLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "authZ", err, errKind...)
}

func peersLogIf(ctx context.Context, err error, errKind ...any) {
	if !errors.Is(err, grid.ErrDisconnected) {
		logger.LogIf(ctx, "peers", err, errKind...)
	}
}

func peersLogAlwaysIf(ctx context.Context, err error, errKind ...any) {
	if !errors.Is(err, grid.ErrDisconnected) {
		logger.LogAlwaysIf(ctx, "peers", err, errKind...)
	}
}

func peersLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	if !errors.Is(err, grid.ErrDisconnected) {
		logger.LogOnceIf(ctx, "peers", err, id, errKind...)
	}
}

func bugLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "internal", err, errKind...)
}

func healingLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "healing", err, errKind...)
}

func healingLogEvent(ctx context.Context, msg string, args ...any) {
	logger.Event(ctx, "healing", msg, args...)
}

func healingLogOnceIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "healing", err, errKind...)
}

func batchLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "batch", err, errKind...)
}

func batchLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "batch", err, id, errKind...)
}

func bootLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "bootstrap", err, errKind...)
}

func bootLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "bootstrap", err, id, errKind...)
}

func dnsLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "dns", err, errKind...)
}

func internalLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "internal", err, errKind...)
}

func internalLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "internal", err, id, errKind...)
}

func transitionLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "transition", err, errKind...)
}

func configLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "config", err, errKind...)
}

func configLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "config", err, id, errKind...)
}

func configLogOnceConsoleIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceConsoleIf(ctx, "config", err, id, errKind...)
}

func scannerLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "scanner", err, errKind...)
}

func scannerLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "scanner", err, id, errKind...)
}

func ilmLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "ilm", err, errKind...)
}

func ilmLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "ilm", err, id, errKind...)
}

func encLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "encryption", err, errKind...)
}

func encLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "encryption", err, id, errKind...)
}

func storageLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "storage", err, errKind...)
}

func storageLogAlwaysIf(ctx context.Context, err error, errKind ...any) {
	logger.LogAlwaysIf(ctx, "storage", err, errKind...)
}

func storageLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "storage", err, id, errKind...)
}

func decomLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "decom", err, errKind...)
}

func decomLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "decom", err, id, errKind...)
}

func decomLogEvent(ctx context.Context, msg string, args ...any) {
	logger.Event(ctx, "decom", msg, args...)
}

func etcdLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "etcd", err, errKind...)
}

func etcdLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "etcd", err, id, errKind...)
}

func metricsLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "metrics", err, errKind...)
}

func s3LogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "s3", err, errKind...)
}

func sftpLogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "sftp", err, id, errKind...)
}

func shutdownLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "shutdown", err, errKind...)
}

func stsLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "sts", err, errKind...)
}

func tierLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "tier", err, errKind...)
}

func kmsLogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "kms", err, errKind...)
}

// KMSLogger permits access to kms module specific logging
type KMSLogger struct{}

// LogOnceIf is the implementation of LogOnceIf, accessible using the Logger interface
func (l KMSLogger) LogOnceIf(ctx context.Context, err error, id string, errKind ...any) {
	logger.LogOnceIf(ctx, "kms", err, id, errKind...)
}

// LogIf is the implementation of LogIf, accessible using the Logger interface
func (l KMSLogger) LogIf(ctx context.Context, err error, errKind ...any) {
	logger.LogIf(ctx, "kms", err, errKind...)
}
