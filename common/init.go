package common

import (
	"os"
)

const EnvDebug = "HDS_LOG_DEBUG"

func init() {
	DebugLogs = EvalEnv(EnvDebug)
}

// evalEnv returns the boolean value of the env variable with the given key
func EvalEnv(key string) bool {
	return os.Getenv(key) == "1" || os.Getenv(key) == "true" || os.Getenv(key) == "TRUE"
}
