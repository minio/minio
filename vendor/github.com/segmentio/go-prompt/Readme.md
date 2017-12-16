
# go-prompt

 Terminal prompts for Go.

 View the [docs](http://godoc.org/pkg/github.com/segmentio/go-prompt).

## Example

```go
package main

import "github.com/segmentio/go-prompt"

var langs = []string{
  "c",
  "c++",
  "lua",
  "go",
  "js",
  "ruby",
  "python",
}

func main() {
  i := prompt.Choose("What's your favorite language?", langs)
  println("picked: " + langs[i])
}
```

## License

 MIT