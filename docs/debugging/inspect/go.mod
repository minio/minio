module github.com/minio/minio/docs/debugging/inspect

go 1.18

require (
	github.com/klauspost/compress v1.15.9
	github.com/minio/colorjson v1.0.2
	github.com/minio/madmin-go v1.3.5
	github.com/secure-io/sio-go v0.3.1
	github.com/tinylib/msgp v1.1.6
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/minio/pkg v1.1.20 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/sys v0.0.0-20220928140112-f11e5e49a4ec // indirect
)

replace github.com/minio/madmin-go => github.com/klauspost/madmin-go v1.0.15-0.20221018080536-5c0ac8af2ed3
