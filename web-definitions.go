package main

// AuthToken - auth token
type AuthToken struct {
	Token string `json:"token" form:"token"`
}

// User users.
type User struct {
	Username string `json:"username" form:"username"`
	Password string `json:"password" form:"password"`
}
