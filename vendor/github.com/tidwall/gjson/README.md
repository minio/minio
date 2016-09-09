<p align="center">
<img 
    src="logo.png" 
    width="240" height="78" border="0" alt="GJSON">
<br>
<a href="https://travis-ci.org/tidwall/gjson"><img src="https://img.shields.io/travis/tidwall/gjson.svg?style=flat-square" alt="Build Status"></a><!--
<a href="http://gocover.io/github.com/tidwall/gjson"><img src="https://img.shields.io/badge/coverage-97%25-brightgreen.svg?style=flat-square" alt="Code Coverage"></a>
-->
<a href="https://godoc.org/github.com/tidwall/gjson"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">get a json value quickly</a></p>

GJSON is a Go package the provides a [very fast](#performance) and simple way to get a value from a json document. The reason for this library it to give efficient json indexing for the [BuntDB](https://github.com/tidwall/buntdb) project. 

Getting Started
===============

## Installing

To start using GJSON, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/gjson
```

This will retrieve the library.

## Get a value
Get searches json for the specified path. A path is in dot syntax, such as "name.last" or "age". This function expects that the json is well-formed and validates. Invalid json will not panic, but it may return back unexpected results. When the value is found it's returned immediately.

```go
package main

import "github.com/tidwall/gjson"

const json = `{"name":{"first":"Janet","last":"Prichard"},"age":47}`

func main() {
	value := gjson.Get(json, "name.last")
	println(value.String())
}
```

This will print:

```
Prichard
```

## Path Syntax

A path is a series of keys separated by a dot.
A key may contain special wildcard characters '\*' and '?'.
To access an array value use the index as the key.
To get the number of elements in an array or to access a child path, use the '#' character.
The dot and wildcard characters can be escaped with '\'.

```json
{
  "name": {"first": "Tom", "last": "Anderson"},
  "age":37,
  "children": ["Sara","Alex","Jack"],
  "fav.movie": "Deer Hunter",
  "friends": [
	{"first": "James", "last": "Murphy"},
	{"first": "Roger", "last": "Craig"}
  ]
}
```
```
"name.last"          >> "Anderson"
"age"                >> 37
"children.#"         >> 3
"children.1"         >> "Alex"
"child*.2"           >> "Jack"
"c?ildren.0"         >> "Sara"
"fav\.movie"         >> "Deer Hunter"
"friends.#.first"    >> [ "James", "Roger" ]
"friends.1.last"     >> "Craig"
```
To query an array:
```
`friends.#[last="Murphy"].first` >> "James"
```

## Result Type

GJSON supports the json types `string`, `number`, `bool`, and `null`. 
Arrays and Objects are returned as their raw json types. 

The `Result` type holds one of these:

```
bool, for JSON booleans
float64, for JSON numbers
string, for JSON string literals
nil, for JSON null
```

To directly access the value:

```go
result.Type    // can be String, Number, True, False, Null, or JSON
result.Str     // holds the string
result.Num     // holds the float64 number
result.Raw     // holds the raw json
result.Multi   // holds nested array values
```

There are a variety of handy functions that work on a result:

```go
result.Value() interface{}
result.Int() int64
result.Float() float64
result.String() string
result.Bool() bool
result.Array() []gjson.Result
result.Map() map[string]gjson.Result
result.Get(path string) Result
```

The `result.Value()` function returns an `interface{}` which requires type assertion and is one of the following Go types:

```go
boolean >> bool
number  >> float64
string  >> string
null    >> nil
array   >> []interface{}
object  >> map[string]interface{}
```

## Get nested array values

Suppose you want all the last names from the following json:

```json
{
  "programmers": [
    {
      "firstName": "Janet", 
      "lastName": "McLaughlin", 
    }, {
      "firstName": "Elliotte", 
      "lastName": "Hunter", 
    }, {
      "firstName": "Jason", 
      "lastName": "Harold", 
    }
  ]
}`
```

You would use the path "programmers.#.lastName" like such:

```go
result := gjson.Get(json, "programmers.#.lastName")
for _,name := range result.Array() {
	println(name.String())
}
```

You can also query an object inside an array:

```go
name := gjson.Get(json, `programmers.#[lastName="Hunter"].firstName`)
println(name.String())  // prints "Elliotte"
```


## Simple Parse and Get

There's a `Parse(json)` function that will do a simple parse, and `result.Get(path)` that will search a result.

For example, all of these will return the same result:

```go
gjson.Parse(json).Get("name").Get("last")
gjson.Get(json, "name").Get("last")
gjson.Get(json, "name.last")
```

## Check for the existence of a value

Sometimes you just want to know you if a value exists.

```go
value := gjson.Get(json, "name.last")
if !value.Exists() {
	println("no last name")
} else {
	println(value.String())
}

// Or as one step
if gjson.Get(json, "name.last").Exists(){
	println("has a last name")
}
```

## Unmarshal to a map

To unmarshal to a `map[string]interface{}`:

```go
m, ok := gjson.Parse(json).Value().(map[string]interface{})
if !ok{
	// not a map
}
```

## Performance

Benchmarks of GJSON alongside [encoding/json](https://golang.org/pkg/encoding/json/), 
[ffjson](https://github.com/pquerna/ffjson), 
[EasyJSON](https://github.com/mailru/easyjson),
and [jsonparser](https://github.com/buger/jsonparser)

```
BenchmarkGJSONGet-8              	15000000	       333 ns/op	       0 B/op	       0 allocs/op
BenchmarkGJSONUnmarshalMap-8     	  900000	      4188 ns/op	    1920 B/op	      26 allocs/op
BenchmarkJSONUnmarshalMap-8      	  600000	      8908 ns/op	    3048 B/op	      69 allocs/op
BenchmarkJSONUnmarshalStruct-8   	  600000	      9026 ns/op	    1832 B/op	      69 allocs/op
BenchmarkJSONDecoder-8           	  300000	     14339 ns/op	    4224 B/op	     184 allocs/op
BenchmarkFFJSONLexer-8           	 1500000	      3156 ns/op	     896 B/op	       8 allocs/op
BenchmarkEasyJSONLexer-8         	 3000000	       938 ns/op	     613 B/op	       6 allocs/op
BenchmarkJSONParserGet-8         	 3000000	       442 ns/op	      21 B/op	       0 allocs/op
```

JSON document used:

```json
{
  "widget": {
    "debug": "on",
    "window": {
      "title": "Sample Konfabulator Widget",
      "name": "main_window",
      "width": 500,
      "height": 500
    },
    "image": { 
      "src": "Images/Sun.png",
      "hOffset": 250,
      "vOffset": 250,
      "alignment": "center"
    },
    "text": {
      "data": "Click Here",
      "size": 36,
      "style": "bold",
      "vOffset": 100,
      "alignment": "center",
      "onMouseUp": "sun1.opacity = (sun1.opacity / 100) * 90;"
    }
  }
}    
```

Each operation was rotated though one of the following search paths:

```
widget.window.name
widget.image.hOffset
widget.text.onMouseUp
```


*These benchmarks were run on a MacBook Pro 15" 2.8 GHz Intel Core i7 using Go 1.7.*

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

GJSON source code is available under the MIT [License](/LICENSE).
