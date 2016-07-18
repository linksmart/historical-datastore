package main

import (
	"github.com/farshidtz/elog"
)

var logger *elog.Logger

func init() {
	logger = elog.New("[main] ", &elog.Config{
		DebugPrefix: "[main-debug] ",
	})
}