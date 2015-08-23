package structs

import (
	"reflect"
	"testing"
)

// A test struct that defines all cases
type Foo struct {
	A    string
	B    int    `structs:"y"`
	C    bool   `json:"c"`
	d    string // not exported
	E    *Baz
	x    string `xml:"x"` // not exported, with tag
	Y    []string
	Z    map[string]interface{}
	*Bar // embedded
}

type Baz struct {
	A string
	B int
}

type Bar struct {
	E string
	F int
	g []string
}

func newStruct() *Struct {
	b := &Bar{
		E: "example",
		F: 2,
		g: []string{"zeynep", "fatih"},
	}

	// B and x is not initialized for testing
	f := &Foo{
		A: "gopher",
		C: true,
		d: "small",
		E: nil,
		Y: []string{"example"},
		Z: nil,
	}
	f.Bar = b

	return New(f)
}

func TestField_Set(t *testing.T) {
	s := newStruct()

	f := s.Field("A")
	err := f.Set("fatih")
	if err != nil {
		t.Error(err)
	}

	if f.Value().(string) != "fatih" {
		t.Errorf("Setted value is wrong: %s want: %s", f.Value().(string), "fatih")
	}

	f = s.Field("Y")
	err = f.Set([]string{"override", "with", "this"})
	if err != nil {
		t.Error(err)
	}

	sliceLen := len(f.Value().([]string))
	if sliceLen != 3 {
		t.Errorf("Setted values slice length is wrong: %d, want: %d", sliceLen, 3)
	}

	f = s.Field("C")
	err = f.Set(false)
	if err != nil {
		t.Error(err)
	}

	if f.Value().(bool) {
		t.Errorf("Setted value is wrong: %s want: %s", f.Value().(bool), false)
	}

	// let's pass a different type
	f = s.Field("A")
	err = f.Set(123) // Field A is of type string, but we are going to pass an integer
	if err == nil {
		t.Error("Setting a field's value with a different type than the field's type should return an error")
	}

	// old value should be still there :)
	if f.Value().(string) != "fatih" {
		t.Errorf("Setted value is wrong: %s want: %s", f.Value().(string), "fatih")
	}

	// let's access an unexported field, which should give an error
	f = s.Field("d")
	err = f.Set("large")
	if err != errNotExported {
		t.Error(err)
	}

	// let's set a pointer to struct
	b := &Bar{
		E: "gopher",
		F: 2,
	}

	f = s.Field("Bar")
	err = f.Set(b)
	if err != nil {
		t.Error(err)
	}

	baz := &Baz{
		A: "helloWorld",
		B: 42,
	}

	f = s.Field("E")
	err = f.Set(baz)
	if err != nil {
		t.Error(err)
	}

	ba := s.Field("E").Value().(*Baz)

	if ba.A != "helloWorld" {
		t.Errorf("could not set baz. Got: %s Want: helloWorld", ba.A)
	}
}

func TestField(t *testing.T) {
	s := newStruct()

	defer func() {
		err := recover()
		if err == nil {
			t.Error("Retrieveing a non existing field from the struct should panic")
		}
	}()

	_ = s.Field("no-field")
}

func TestField_Kind(t *testing.T) {
	s := newStruct()

	f := s.Field("A")
	if f.Kind() != reflect.String {
		t.Errorf("Field A has wrong kind: %s want: %s", f.Kind(), reflect.String)
	}

	f = s.Field("B")
	if f.Kind() != reflect.Int {
		t.Errorf("Field B has wrong kind: %s want: %s", f.Kind(), reflect.Int)
	}

	// unexported
	f = s.Field("d")
	if f.Kind() != reflect.String {
		t.Errorf("Field d has wrong kind: %s want: %s", f.Kind(), reflect.String)
	}
}

func TestField_Tag(t *testing.T) {
	s := newStruct()

	v := s.Field("B").Tag("json")
	if v != "" {
		t.Errorf("Field's tag value of a non existing tag should return empty, got: %s", v)
	}

	v = s.Field("C").Tag("json")
	if v != "c" {
		t.Errorf("Field's tag value of the existing field C should return 'c', got: %s", v)
	}

	v = s.Field("d").Tag("json")
	if v != "" {
		t.Errorf("Field's tag value of a non exported field should return empty, got: %s", v)
	}

	v = s.Field("x").Tag("xml")
	if v != "x" {
		t.Errorf("Field's tag value of a non exported field with a tag should return 'x', got: %s", v)
	}

	v = s.Field("A").Tag("json")
	if v != "" {
		t.Errorf("Field's tag value of a existing field without a tag should return empty, got: %s", v)
	}
}

func TestField_Value(t *testing.T) {
	s := newStruct()

	v := s.Field("A").Value()
	val, ok := v.(string)
	if !ok {
		t.Errorf("Field's value of a A should be string")
	}

	if val != "gopher" {
		t.Errorf("Field's value of a existing tag should return 'gopher', got: %s", val)
	}

	defer func() {
		err := recover()
		if err == nil {
			t.Error("Value of a non exported field from the field should panic")
		}
	}()

	// should panic
	_ = s.Field("d").Value()
}

func TestField_IsEmbedded(t *testing.T) {
	s := newStruct()

	if !s.Field("Bar").IsEmbedded() {
		t.Errorf("Fields 'Bar' field is an embedded field")
	}

	if s.Field("d").IsEmbedded() {
		t.Errorf("Fields 'd' field is not an embedded field")
	}
}

func TestField_IsExported(t *testing.T) {
	s := newStruct()

	if !s.Field("Bar").IsExported() {
		t.Errorf("Fields 'Bar' field is an exported field")
	}

	if !s.Field("A").IsExported() {
		t.Errorf("Fields 'A' field is an exported field")
	}

	if s.Field("d").IsExported() {
		t.Errorf("Fields 'd' field is not an exported field")
	}
}

func TestField_IsZero(t *testing.T) {
	s := newStruct()

	if s.Field("A").IsZero() {
		t.Errorf("Fields 'A' field is an initialized field")
	}

	if !s.Field("B").IsZero() {
		t.Errorf("Fields 'B' field is not an initialized field")
	}
}

func TestField_Name(t *testing.T) {
	s := newStruct()

	if s.Field("A").Name() != "A" {
		t.Errorf("Fields 'A' field should have the name 'A'")
	}
}

func TestField_Field(t *testing.T) {
	s := newStruct()

	e := s.Field("Bar").Field("E")

	val, ok := e.Value().(string)
	if !ok {
		t.Error("The value of the field 'e' inside 'Bar' struct should be string")
	}

	if val != "example" {
		t.Errorf("The value of 'e' should be 'example, got: %s", val)
	}

	defer func() {
		err := recover()
		if err == nil {
			t.Error("Field of a non existing nested struct should panic")
		}
	}()

	_ = s.Field("Bar").Field("e")
}

func TestField_Fields(t *testing.T) {
	s := newStruct()
	fields := s.Field("Bar").Fields()

	if len(fields) != 3 {
		t.Errorf("We expect 3 fields in embedded struct, was: %d", len(fields))
	}
}

func TestField_FieldOk(t *testing.T) {
	s := newStruct()

	b, ok := s.FieldOk("Bar")
	if !ok {
		t.Error("The field 'Bar' should exists.")
	}

	e, ok := b.FieldOk("E")
	if !ok {
		t.Error("The field 'E' should exists.")
	}

	val, ok := e.Value().(string)
	if !ok {
		t.Error("The value of the field 'e' inside 'Bar' struct should be string")
	}

	if val != "example" {
		t.Errorf("The value of 'e' should be 'example, got: %s", val)
	}
}
