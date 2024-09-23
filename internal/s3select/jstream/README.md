<p align="center"><img width="350px" src="jstream.png" alt="jstream"/></p>

#

[![GoDoc](https://godoc.org/github.com/bcicen/jstream?status.svg)](https://godoc.org/github.com/bcicen/jstream)


`jstream` is a streaming JSON parser and value extraction library for Go.

Unlike most JSON parsers, `jstream` is document position- and depth-aware -- this enables the extraction of values at a specified depth, eliminating the overhead of allocating encompassing arrays or objects; e.g:

Using the below example document:
<img width="85%" src="https://bradley.codes/static/img/jstream-levels.gif" alt="jstream"/>

we can choose to extract and act only the objects within the top-level array:
```go
f, _ := os.Open("input.json")
decoder := jstream.NewDecoder(f, 1) // extract JSON values at a depth level of 1
for mv := range decoder.Stream() {
  fmt.Printf("%v\n ", mv.Value)
}
```

output:
```
map[desc:RGB colors:[red green blue]]
map[desc:CMYK colors:[cyan magenta yellow black]]
```

likewise, increasing depth level to `3` yields:
```
red
green
blue
cyan
magenta
yellow
black
```

optionally, kev:value pairs can be emitted as an individual struct:
```go
decoder := jstream.NewDecoder(f, 2).EmitKV() // enable KV streaming at a depth level of 2
```

```
jstream.KV{desc RGB}
jstream.KV{colors [red green blue]}
jstream.KV{desc CMYK}
jstream.KV{colors [cyan magenta yellow black]}
```

## Installing 

```bash
go get github.com/bcicen/jstream
```

## Commandline

`jstream` comes with a cli tool for quick viewing of parsed values from JSON input:

```bash
jstream -d 1 < input.json
```

```json
{"colors":["red","green","blue"],"desc":"RGB"}
{"colors":["cyan","magenta","yellow","black"],"desc":"CMYK"}
```

detailed output with `-v` option:
```bash
cat input.json | jstream -v -d -1

depth	start	end	type   | value
2	018	023	string | "RGB"
3	041	046	string | "red"
3	048	055	string | "green"
3	057	063	string | "blue"
2	039	065	array  | ["red","green","blue"]
1	004	069	object | {"colors":["red","green","blue"],"desc":"RGB"}
2	087	093	string | "CMYK"
3	111	117	string | "cyan"
3	119	128	string | "magenta"
3	130	138	string | "yellow"
3	140	147	string | "black"
2	109	149	array  | ["cyan","magenta","yellow","black"]
1	073	153	object | {"colors":["cyan","magenta","yellow","black"],"desc":"CMYK"}
0	000	155	array  | [{"colors":["red","green","blue"],"desc":"RGB"},{"colors":["cyan","magenta","yellow","black"],"desc":"CMYK"}]
```

### Options

Opt | Description
--- | ---
-d \<n\> | emit values at depth n. if n < 0, all values will be emitted
-kv | output inner key value pairs as newly formed objects
-v | output depth and offset details for each value
-h | display help dialog

## Benchmarks

Obligatory benchmarks performed on files with arrays of objects, where the decoded objects are to be extracted.

Two file sizes are used -- regular (1.6mb, 1000 objects) and large (128mb, 100000 objects)

input size | lib | MB/s | Allocated
--- | --- | --- | ---
regular | standard | 97 | 3.6MB
regular | jstream | 175 | 2.1MB
large | standard | 92 | 305MB
large | jstream | 404 | 69MB

In a real world scenario, including initialization and reader overhead from varying blob sizes, performance can be expected as below:
<img src="https://bradley.codes/static/img/bench.svg" alt="jstream"/>
