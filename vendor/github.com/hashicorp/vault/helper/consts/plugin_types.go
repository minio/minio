package consts

import "fmt"

var PluginTypes = []PluginType{
	PluginTypeUnknown,
	PluginTypeCredential,
	PluginTypeDatabase,
	PluginTypeSecrets,
}

type PluginType uint32

// This is a list of PluginTypes used by Vault.
// If we need to add any in the future, it would
// be best to add them to the _end_ of the list below
// because they resolve to incrementing numbers,
// which may be saved in state somewhere. Thus if
// the name for one of those numbers changed because
// a value were added to the middle, that could cause
// the wrong plugin types to be read from storage
// for a given underlying number. Example of the problem
// here: https://play.golang.org/p/YAaPw5ww3er
const (
	PluginTypeUnknown PluginType = iota
	PluginTypeCredential
	PluginTypeDatabase
	PluginTypeSecrets
)

func (p PluginType) String() string {
	switch p {
	case PluginTypeUnknown:
		return "unknown"
	case PluginTypeCredential:
		return "auth"
	case PluginTypeDatabase:
		return "database"
	case PluginTypeSecrets:
		return "secret"
	default:
		return "unsupported"
	}
}

func ParsePluginType(pluginType string) (PluginType, error) {
	switch pluginType {
	case "unknown":
		return PluginTypeUnknown, nil
	case "auth":
		return PluginTypeCredential, nil
	case "database":
		return PluginTypeDatabase, nil
	case "secret":
		return PluginTypeSecrets, nil
	default:
		return PluginTypeUnknown, fmt.Errorf("%q is not a supported plugin type", pluginType)
	}
}
