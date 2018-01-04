// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"github.com/farshidtz/elog"
)

var logger *elog.Logger

func init() {
	logger = elog.New("[hds] ", &elog.Config{
		DebugPrefix: "[hds-debug] ",
		DebugTrace:  elog.NoTrace,
	})
}
