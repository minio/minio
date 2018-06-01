package authentication

import "path"

type KeyID struct {
	UserName    string
	AccountName string
	Fingerprint string
	IsManta     bool
}

func (input *KeyID) generate() string {
	var keyID string
	if input.UserName != "" {
		if input.IsManta {
			keyID = path.Join("/", input.AccountName, input.UserName, "keys", input.Fingerprint)
		} else {
			keyID = path.Join("/", input.AccountName, "users", input.UserName, "keys", input.Fingerprint)
		}
	} else {
		keyID = path.Join("/", input.AccountName, "keys", input.Fingerprint)
	}

	return keyID
}
