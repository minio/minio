package main

// ListBucketsArgs - list bucket args.
type ListBucketsArgs struct{}

// ListObjectsArgs - list object args.
type ListObjectsArgs struct {
	BucketName string `json:"bucketName"`
	Prefix     string `json:"prefix"`
}

// GetObjectURLArgs - get object url.
type GetObjectURLArgs struct {
	BucketName string `json:"bucketName"`
	ObjectName string `json:"objectName"`
}

// AuthToken - auth token reply
type AuthToken struct {
	Token string `json:"token" form:"token"`
}

// LoginArgs - login arguments.
type LoginArgs struct {
	Username string `json:"username" form:"username"`
	Password string `json:"password" form:"password"`
}
