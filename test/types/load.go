package types

import (
	"encoding/json"
	"fmt"
)

var typeMap = map[string]any{
	"User":         &User{},
	"Temperature":  &Temperature{},
	"Account":      &Account{},
	"Organization": &Organization{},
	"Transaction":  &Transaction{},
	"Message":      &Message{},
}

func TypeByName(name string) any {
	t, ok := typeMap[name]
	if !ok {
		return nil
	}

	return t
}

func UnmarshallData(name string, b []byte) (any, error) {
	switch name {
	case "User":
		var d []*User
		return d, json.Unmarshal(b, &d)
	case "Temperature":
		var d []*Temperature
		return d, json.Unmarshal(b, &d)
	case "Account":
		var d []*Account
		return d, json.Unmarshal(b, &d)
	case "Organization":
		var d []*Organization
		return d, json.Unmarshal(b, &d)
	case "Transaction":
		var d []*Transaction
		return d, json.Unmarshal(b, &d)
	case "Message":
		var d []*Message
		return d, json.Unmarshal(b, &d)
	}
	return nil, fmt.Errorf("no such data type %q", name)
}
