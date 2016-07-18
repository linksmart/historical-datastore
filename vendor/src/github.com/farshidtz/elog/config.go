package elog

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	NoTrace   = 1
	LongFile  = 1 << (iota + 2) // full file name and line number: /a/b/c/d.go:23
	ShortFile                   // final file name element and line number: d.go:23
)

type Config struct {
	Writer       io.Writer
	TimeFormat   string
	Prefix       string
	Trace        int
	DebugEnabled *bool
	DebugPrefix  string
	DebugTrace   int
	DebugEnvVar  string
}

func initConfig(config *Config) *Config {
	var conf = &Config{
		// default configurations
		Writer:     os.Stdout,
		TimeFormat: "2006/01/02 15:04:05",
		Trace:      0,
		// debugging config
		DebugEnvVar:  "DEBUG",
		DebugEnabled: nil,
		DebugPrefix:  "[debug] ",
		DebugTrace:   ShortFile,
	}

	if config != nil {
		if config.Writer != nil {
			conf.Writer = config.Writer
		}
		if config.TimeFormat != "" {
			conf.TimeFormat = config.TimeFormat
		}
		if config.Trace != 0 {
			conf.Trace = config.Trace
		}
		// debugging conf
		if config.DebugEnabled != nil {
			conf.DebugEnabled = config.DebugEnabled
		}
		if config.DebugEnvVar != "" {
			conf.DebugEnvVar = config.DebugEnvVar
		}
		if config.DebugPrefix != "" {
			conf.DebugPrefix = config.DebugPrefix
		}
		if config.DebugTrace != 0 {
			if config.DebugTrace == 8 || config.DebugTrace == 16 {
				conf.DebugTrace = config.DebugTrace
			} else {
				conf.DebugTrace = 0
			}
		}
	}

	conf.TimeFormat = fmt.Sprintf("%s ", strings.TrimSpace(conf.TimeFormat))
	if conf.DebugEnabled == nil {
		var debug bool
		// Enable debugging if environment variable is set
		v, err := strconv.Atoi(os.Getenv(conf.DebugEnvVar))
		if err == nil && v == 1 {
			debug = true
		}
		conf.DebugEnabled = &debug
	}

	return conf
}
