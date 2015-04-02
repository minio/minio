package structs

import (
	"fmt"
	"time"
)

func ExampleNew() {
	type Server struct {
		Name    string
		ID      int32
		Enabled bool
	}

	server := &Server{
		Name:    "Arslan",
		ID:      123456,
		Enabled: true,
	}

	s := New(server)

	fmt.Printf("Name        : %v\n", s.Name())
	fmt.Printf("Values      : %v\n", s.Values())
	fmt.Printf("Value of ID : %v\n", s.Field("ID").Value())
	// Output:
	// Name        : Server
	// Values      : [Arslan 123456 true]
	// Value of ID : 123456

}

func ExampleMap() {
	type Server struct {
		Name    string
		ID      int32
		Enabled bool
	}

	s := &Server{
		Name:    "Arslan",
		ID:      123456,
		Enabled: true,
	}

	m := Map(s)

	fmt.Printf("%#v\n", m["Name"])
	fmt.Printf("%#v\n", m["ID"])
	fmt.Printf("%#v\n", m["Enabled"])
	// Output:
	// "Arslan"
	// 123456
	// true

}

func ExampleMap_tags() {
	// Custom tags can change the map keys instead of using the fields name
	type Server struct {
		Name    string `structs:"server_name"`
		ID      int32  `structs:"server_id"`
		Enabled bool   `structs:"enabled"`
	}

	s := &Server{
		Name: "Zeynep",
		ID:   789012,
	}

	m := Map(s)

	// access them by the custom tags defined above
	fmt.Printf("%#v\n", m["server_name"])
	fmt.Printf("%#v\n", m["server_id"])
	fmt.Printf("%#v\n", m["enabled"])
	// Output:
	// "Zeynep"
	// 789012
	// false

}

func ExampleMap_nested() {
	// By default field with struct types are processed too. We can stop
	// processing them via "omitnested" tag option.
	type Server struct {
		Name string    `structs:"server_name"`
		ID   int32     `structs:"server_id"`
		Time time.Time `structs:"time,omitnested"` // do not convert to map[string]interface{}
	}

	const shortForm = "2006-Jan-02"
	t, _ := time.Parse("2006-Jan-02", "2013-Feb-03")

	s := &Server{
		Name: "Zeynep",
		ID:   789012,
		Time: t,
	}

	m := Map(s)

	// access them by the custom tags defined above
	fmt.Printf("%v\n", m["server_name"])
	fmt.Printf("%v\n", m["server_id"])
	fmt.Printf("%v\n", m["time"].(time.Time))
	// Output:
	// Zeynep
	// 789012
	// 2013-02-03 00:00:00 +0000 UTC
}

func ExampleMap_omitEmpty() {
	// By default field with struct types of zero values are processed too. We
	// can stop processing them via "omitempty" tag option.
	type Server struct {
		Name     string `structs:",omitempty"`
		ID       int32  `structs:"server_id,omitempty"`
		Location string
	}

	// Only add location
	s := &Server{
		Location: "Tokyo",
	}

	m := Map(s)

	// map contains only the Location field
	fmt.Printf("%v\n", m)
	// Output:
	// map[Location:Tokyo]
}

func ExampleValues() {
	type Server struct {
		Name    string
		ID      int32
		Enabled bool
	}

	s := &Server{
		Name:    "Fatih",
		ID:      135790,
		Enabled: false,
	}

	m := Values(s)

	fmt.Printf("Values: %+v\n", m)
	// Output:
	// Values: [Fatih 135790 false]
}

func ExampleValues_omitEmpty() {
	// By default field with struct types of zero values are processed too. We
	// can stop processing them via "omitempty" tag option.
	type Server struct {
		Name     string `structs:",omitempty"`
		ID       int32  `structs:"server_id,omitempty"`
		Location string
	}

	// Only add location
	s := &Server{
		Location: "Ankara",
	}

	m := Values(s)

	// values contains only the Location field
	fmt.Printf("Values: %+v\n", m)
	// Output:
	// Values: [Ankara]
}

func ExampleValues_tags() {
	type Location struct {
		City    string
		Country string
	}

	type Server struct {
		Name     string
		ID       int32
		Enabled  bool
		Location Location `structs:"-"` // values from location are not included anymore
	}

	s := &Server{
		Name:     "Fatih",
		ID:       135790,
		Enabled:  false,
		Location: Location{City: "Ankara", Country: "Turkey"},
	}

	// Let get all values from the struct s. Note that we don't include values
	// from the Location field
	m := Values(s)

	fmt.Printf("Values: %+v\n", m)
	// Output:
	// Values: [Fatih 135790 false]
}

func ExampleFields() {
	type Access struct {
		Name         string
		LastAccessed time.Time
		Number       int
	}

	s := &Access{
		Name:         "Fatih",
		LastAccessed: time.Now(),
		Number:       1234567,
	}

	fields := Fields(s)

	for i, field := range fields {
		fmt.Printf("[%d] %+v\n", i, field.Name())
	}

	// Output:
	// [0] Name
	// [1] LastAccessed
	// [2] Number
}

func ExampleFields_nested() {
	type Person struct {
		Name   string
		Number int
	}

	type Access struct {
		Person        Person
		HasPermission bool
		LastAccessed  time.Time
	}

	s := &Access{
		Person:        Person{Name: "fatih", Number: 1234567},
		LastAccessed:  time.Now(),
		HasPermission: true,
	}

	// Let's get all fields from the struct s.
	fields := Fields(s)

	for _, field := range fields {
		if field.Name() == "Person" {
			fmt.Printf("Access.Person.Name: %+v\n", field.Field("Name").Value())
		}
	}

	// Output:
	// Access.Person.Name: fatih
}

func ExampleField() {
	type Person struct {
		Name   string
		Number int
	}

	type Access struct {
		Person        Person
		HasPermission bool
		LastAccessed  time.Time
	}

	access := &Access{
		Person:        Person{Name: "fatih", Number: 1234567},
		LastAccessed:  time.Now(),
		HasPermission: true,
	}

	// Create a new Struct type
	s := New(access)

	// Get the Field type for "Person" field
	p := s.Field("Person")

	// Get the underlying "Name field" and print the value of it
	name := p.Field("Name")

	fmt.Printf("Value of Person.Access.Name: %+v\n", name.Value())

	// Output:
	// Value of Person.Access.Name: fatih

}

func ExampleIsZero() {
	type Server struct {
		Name    string
		ID      int32
		Enabled bool
	}

	// Nothing is initalized
	a := &Server{}
	isZeroA := IsZero(a)

	// Name and Enabled is initialized, but not ID
	b := &Server{
		Name:    "Golang",
		Enabled: true,
	}
	isZeroB := IsZero(b)

	fmt.Printf("%#v\n", isZeroA)
	fmt.Printf("%#v\n", isZeroB)
	// Output:
	// true
	// false
}

func ExampleHasZero() {
	// Let's define an Access struct. Note that the "Enabled" field is not
	// going to be checked because we added the "structs" tag to the field.
	type Access struct {
		Name         string
		LastAccessed time.Time
		Number       int
		Enabled      bool `structs:"-"`
	}

	// Name and Number is not initialized.
	a := &Access{
		LastAccessed: time.Now(),
	}
	hasZeroA := HasZero(a)

	// Name and Number is initialized.
	b := &Access{
		Name:         "Fatih",
		LastAccessed: time.Now(),
		Number:       12345,
	}
	hasZeroB := HasZero(b)

	fmt.Printf("%#v\n", hasZeroA)
	fmt.Printf("%#v\n", hasZeroB)
	// Output:
	// true
	// false
}
