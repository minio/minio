SET MINIO_RELEASE=OFFICIAL
SET GO15VENDOREXPERIMENT=1
go run buildscripts/gen-ldflags.go > temp.txt
SET /p LDFLAGS=<temp.txt
go build -ldflags="%LDFLAGS%" -o %GOPATH%\bin\minio.exe
