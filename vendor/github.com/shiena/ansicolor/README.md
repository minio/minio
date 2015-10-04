[![GoDoc](https://godoc.org/github.com/shiena/ansicolor?status.svg)](https://godoc.org/github.com/shiena/ansicolor)

# ansicolor

Ansicolor library provides color console in Windows as ANSICON for Golang.

## Features

|Escape sequence|Text attributes|
|---------------|----|
|\x1b[0m|All attributes off(color at startup)|
|\x1b[1m|Bold on(enable foreground intensity)|
|\x1b[4m|Underline on|
|\x1b[5m|Blink on(enable background intensity)|
|\x1b[21m|Bold off(disable foreground intensity)|
|\x1b[24m|Underline off|
|\x1b[25m|Blink off(disable background intensity)|

|Escape sequence|Foreground colors|
|---------------|----|
|\x1b[30m|Black|
|\x1b[31m|Red|
|\x1b[32m|Green|
|\x1b[33m|Yellow|
|\x1b[34m|Blue|
|\x1b[35m|Magenta|
|\x1b[36m|Cyan|
|\x1b[37m|White|
|\x1b[39m|Default(foreground color at startup)|
|\x1b[90m|Light Gray|
|\x1b[91m|Light Red|
|\x1b[92m|Light Green|
|\x1b[93m|Light Yellow|
|\x1b[94m|Light Blue|
|\x1b[95m|Light Magenta|
|\x1b[96m|Light Cyan|
|\x1b[97m|Light White|

|Escape sequence|Background colors|
|---------------|----|
|\x1b[40m|Black|
|\x1b[41m|Red|
|\x1b[42m|Green|
|\x1b[43m|Yellow|
|\x1b[44m|Blue|
|\x1b[45m|Magenta|
|\x1b[46m|Cyan|
|\x1b[47m|White|
|\x1b[49m|Default(background color at startup)|
|\x1b[100m|Light Gray|
|\x1b[101m|Light Red|
|\x1b[102m|Light Green|
|\x1b[103m|Light Yellow|
|\x1b[104m|Light Blue|
|\x1b[105m|Light Magenta|
|\x1b[106m|Light Cyan|
|\x1b[107m|Light White|

## Example

```go
package main

import (
	"fmt"
	"os"

	"github.com/shiena/ansicolor"
)

func main() {
	w := ansicolor.NewAnsiColorWriter(os.Stdout)
	text := "%sforeground %sbold%s %sbackground%s\n"
	fmt.Fprintf(w, text, "\x1b[31m", "\x1b[1m", "\x1b[21m", "\x1b[41;32m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[32m", "\x1b[1m", "\x1b[21m", "\x1b[42;31m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[33m", "\x1b[1m", "\x1b[21m", "\x1b[43;34m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[34m", "\x1b[1m", "\x1b[21m", "\x1b[44;33m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[35m", "\x1b[1m", "\x1b[21m", "\x1b[45;36m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[36m", "\x1b[1m", "\x1b[21m", "\x1b[46;35m", "\x1b[0m")
	fmt.Fprintf(w, text, "\x1b[37m", "\x1b[1m", "\x1b[21m", "\x1b[47;30m", "\x1b[0m")
}
```

![screenshot](https://gist.githubusercontent.com/shiena/a1bada24b525314a7d5e/raw/c763aa7cda6e4fefaccf831e2617adc40b6151c7/main.png)

## See also:

- https://github.com/daviddengcn/go-colortext
- https://github.com/adoxa/ansicon
- https://github.com/aslakhellesoy/wac
- https://github.com/wsxiaoys/terminal

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

