package logs

import (
	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
)

var Log *log.Logger

func InitLogger() {
	if Log != nil {
		return
	}

	Log = log.New()
	Log.SetLevel(log.InfoLevel)
	formatter := &log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		FullTimestamp:   true,
	}
	Log.SetReportCaller(true)
	Log.SetFormatter(formatter)
	pathMap := lfshook.PathMap{
		log.InfoLevel:  "./logs/ledger_info.log",
		log.WarnLevel:  "./logs/ledger_warn.log",
		log.ErrorLevel: "./logs/ledger_error.log",
		log.FatalLevel: "./logs/ledger_error.log",
		log.PanicLevel: "./logs/ledger_error.log",
	}
	Log.Hooks.Add(lfshook.NewHook(
		pathMap,
		formatter,
	))
	Log.WriterLevel(log.InfoLevel)
}

func GetLogger() *log.Logger {
	return Log
}
