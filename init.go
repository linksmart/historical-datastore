// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"log"
	"os"

	"github.com/linksmart/historical-datastore/common"
)

const (
	EnvVerbose        = "VERBOSE"          // print extra information e.g. line number)
	EnvDisableLogTime = "DISABLE_LOG_TIME" // disable timestamp in logs
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)

	logFlags := log.LstdFlags
	if common.EvalEnv(EnvDisableLogTime) {
		logFlags = 0
	}
	if common.EvalEnv(EnvVerbose) {
		logFlags = logFlags | log.Lshortfile
	}
	log.SetFlags(logFlags)
}
