package elog

import (
	"fmt"
	"log"
)

type Logger struct {
	*log.Logger
	debug *log.Logger
}

func New(prefix string, config *Config) *Logger {
	conf := initConfig(config)

	var logger Logger
	if *conf.DebugEnabled {
		logger.Logger = log.New(&writer{conf.Writer, conf.TimeFormat}, prefix, conf.DebugTrace)
		logger.debug = log.New(&writer{conf.Writer, conf.TimeFormat}, conf.DebugPrefix, conf.DebugTrace)
	} else {
		logger.Logger = log.New(&writer{conf.Writer, conf.TimeFormat}, prefix, conf.Trace)
	}
	return &logger
}

func (l *Logger) Errorf(format string, a ...interface{}) error {
	if l.debug != nil {
		l.debug.Output(2, fmt.Sprintf(format, a...))
	}
	return fmt.Errorf(format, a...)
}

func (l *Logger) Debug(a ...interface{}) {
	if l.debug != nil {
		l.debug.Output(2, fmt.Sprint(a...))
	}
}

func (l *Logger) Debugf(format string, a ...interface{}) {
	if l.debug != nil {
		l.debug.Output(2, fmt.Sprintf(format, a...))
	}
}

func (l *Logger) Debugln(a ...interface{}) {
	if l.debug != nil {
		l.debug.Output(2, fmt.Sprintln(a...))
	}
}

func (l *Logger) DebugOutput(calldepth int, s string) {
	if l.debug != nil {
		l.debug.Output(calldepth+1, s)
	}
}
