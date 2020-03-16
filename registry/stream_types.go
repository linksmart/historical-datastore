package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// StreamType represents the type of a specific stream
type StreamType int

const (
	Float StreamType = iota
	String
	Bool
	Data
)

func (s StreamType) String() string {
	return toString[s]
}

var toString = map[StreamType]string{
	Float:  "float",
	String: "string",
	Bool:   "bool",
	Data:   "data",
}

var toID = map[string]StreamType{
	"float":  Float,
	"string": String,
	"bool":   Bool,
	"data":   Data,
}

// MarshalJSON marshals the enum as a quoted json string
func (s StreamType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (s *StreamType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}

	if s_val, ok := toID[j]; ok {
		*s = s_val
	} else {
		return fmt.Errorf("unsupported type:%s", j)
	}
	return nil
}
