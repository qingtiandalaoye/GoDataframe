package dataframe

//this file comes from : github.com\kniren\gota
//a little change

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type stringElement struct {
	s *string
}

type intElement struct {
	i *int
}

type floatElement struct {
	f *float64
}

type timeElement struct {
	t *time.Time
}

type boolElement struct {
	b *bool
}

type elementInterface interface {
	Eq(elementInterface) bool
	Less(elementInterface) bool
	LessEq(elementInterface) bool
	Greater(elementInterface) bool
	GreaterEq(elementInterface) bool
	ToString() stringElement
	ToInt() intElement
	ToFloat() floatElement
	ToBool() boolElement
	ToTime() timeElement
	IsNA() bool
	Value() elementValue
}

type elementValue interface{}

//use go lang standard mode, this is hard code: src/pkg/time/format.go
const format_timeElement_style_default = "2006-01-02"

//const format_timeElement_style_default = "2006-01-02 15:04:05"

func createTimeElement(datetimeFormat string, s *string) timeElement {
	if len(datetimeFormat) == 0 {
		datetimeFormat = format_timeElement_style_default
	}
	if t, err := time.Parse(datetimeFormat, *s); err == nil {
		return timeElement{&t}
	} else {
		fmt.Printf("createTimeElement error: %s", err)
		panic("createTimeElement error: ")
	}

	return timeElement{nil}
}

//print the type
func checkit(v interface{}) {
	ret, ok := v.(int)
	fmt.Printf("check the type: %s, %b\n", ret, ok)
	var t = reflect.TypeOf(v)
	fmt.Printf("check the reflect type: %v\n", t)
}

func ToString(e elementValue) (stringElement, error) {
	if e == nil {
		return stringElement{nil}, errors.New("elementValue is nil")
	}
	//fmt.Printf("check the type of elementValue: %s\n",e)
	//checkit(e)
	switch e.(type) {
	case intElement:
		var ele intElement = e.(intElement)
		return ele.ToString(), nil
	case floatElement:
		var ele floatElement = e.(floatElement)
		return ele.ToString(), nil
	case stringElement:
		var ele stringElement = e.(stringElement)
		return ele.ToString(), nil
	case boolElement:
		var ele boolElement = e.(boolElement)
		return ele.ToString(), nil
	case timeElement:
		var ele timeElement = e.(timeElement)
		return ele.ToString(), nil
	default:
		var empty string = ""
		return stringElement{&empty}, nil
	}
}
func ToInt(e elementValue) (intElement, error) {
	if e == nil {
		var empty int = 0
		return intElement{&empty}, errors.New("elementValue is nil")
	}
	//fmt.Printf("check the type of elementValue: %s\n",e)
	//checkit(e)
	switch e.(type) {
	case intElement:
		var ele intElement = e.(intElement)
		return ele.ToInt(), nil
	case floatElement:
		var ele floatElement = e.(floatElement)
		return ele.ToInt(), nil
	case stringElement:
		var ele stringElement = e.(stringElement)
		return ele.ToInt(), nil
	case boolElement:
		var ele boolElement = e.(boolElement)
		return ele.ToInt(), nil
	case timeElement:
		var ele timeElement = e.(timeElement)
		return ele.ToInt(), nil
	default:
		var empty int = 0
		return intElement{&empty}, nil
	}
}
func ToFloat(e elementValue) (floatElement, error) {
	if e == nil {
		empty := 0.0
		return floatElement{&empty}, errors.New("elementValue is nil")
	}
	//fmt.Printf("check the type of elementValue: %s\n",e)
	//checkit(e)
	switch e.(type) {
	case intElement:
		var ele intElement = e.(intElement)
		return ele.ToFloat(), nil
	case floatElement:
		var ele floatElement = e.(floatElement)
		return ele.ToFloat(), nil
	case stringElement:
		var ele stringElement = e.(stringElement)
		return ele.ToFloat(), nil
	case boolElement:
		var ele boolElement = e.(boolElement)
		return ele.ToFloat(), nil
	case timeElement:
		var ele timeElement = e.(timeElement)
		return ele.ToFloat(), nil
	default:
		empty := 0.0
		return floatElement{&empty}, nil
	}
}
func ToBool(e elementValue) (boolElement, error) {
	if e == nil {
		var empty bool = false
		return boolElement{&empty}, errors.New("elementValue is nil")
	}
	//fmt.Printf("check the type of elementValue: %s\n",e)
	//checkit(e)
	switch e.(type) {
	case intElement:
		var ele intElement = e.(intElement)
		return ele.ToBool(), nil
	case floatElement:
		var ele floatElement = e.(floatElement)
		return ele.ToBool(), nil
	case stringElement:
		var ele stringElement = e.(stringElement)
		return ele.ToBool(), nil
	case boolElement:
		var ele boolElement = e.(boolElement)
		return ele.ToBool(), nil
	case timeElement:
		var ele timeElement = e.(timeElement)
		return ele.ToBool(), nil
	default:
		var empty bool = false
		return boolElement{&empty}, nil
	}
}

func ToTime(e elementValue) (timeElement, error) {
	if e == nil {
		var empty = time.Now()
		return timeElement{&empty}, errors.New("elementValue is nil")
	}
	//fmt.Printf("check the type of elementValue: %s\n",e)
	//checkit(e)
	switch e.(type) {
	case intElement:
		var ele intElement = e.(intElement)
		return ele.ToTime(), nil
	case floatElement:
		var ele floatElement = e.(floatElement)
		return ele.ToTime(), nil
	case stringElement:
		var ele stringElement = e.(stringElement)
		return ele.ToTime(), nil
	case boolElement:
		var ele boolElement = e.(boolElement)
		return ele.ToTime(), nil
	case timeElement:
		var ele timeElement = e.(timeElement)
		return ele.ToTime(), nil
	default:
		var empty = time.Now()
		return timeElement{&empty}, nil
	}
}

func (e stringElement) Len() int {
	if e.IsNA() {
		return 0
	}
	return len(*e.s)
}

func (e stringElement) Value() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.s
}
func (e intElement) Value() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.i
}
func (e floatElement) Value() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.f
}
func (e boolElement) Value() elementValue {
	if e.IsNA() {
		return nil
	}
	return *e.b
}
func (t timeElement) Value() elementValue {
	if t.IsNA() {
		return nil
	}
	return *t.t
}

func (s stringElement) ToString() stringElement {
	return s.Copy()
}
func (i intElement) ToString() stringElement {
	if i.IsNA() {
		return stringElement{nil}
	}
	s := i.String()
	return stringElement{&s}
}
func (f floatElement) ToString() stringElement {
	if f.IsNA() {
		return stringElement{nil}
	}
	s := f.String()
	return stringElement{&s}
}
func (t timeElement) ToString() stringElement {
	if t.IsNA() {
		return stringElement{nil}
	}
	s := t.String()
	return stringElement{&s}
}
func (b boolElement) ToString() stringElement {
	if b.IsNA() {
		return stringElement{nil}
	}
	s := b.String()
	return stringElement{&s}
}

func (s stringElement) ToInt() intElement {
	if s.s == nil {
		return intElement{nil}
	}
	i, err := strconv.Atoi(*s.s)
	if err != nil {
		return intElement{nil}
	}
	if s.IsNA() {
		return intElement{nil}
	}
	return intElement{&i}
}
func (i intElement) ToInt() intElement {
	return i.Copy()
}
func (f floatElement) ToInt() intElement {
	if f.f != nil {
		i := int(*f.f)
		return intElement{&i}
	}
	return intElement{nil}
}
func (b boolElement) ToInt() intElement {
	if b.b == nil {
		return intElement{nil}
	}
	var i int
	if *b.b {
		i = 1
	} else {
		i = 0
	}
	return intElement{&i}
}
func (t timeElement) ToInt() intElement {
	if t.t != nil {
		timeStr := t.t.Format("20060102150405")
		i, err := strconv.Atoi(timeStr)
		if err != nil {
			return intElement{nil}
		}
		return intElement{&i}
	}
	return intElement{nil}
}

func (s stringElement) ToTime() timeElement {
	if s.s == nil {
		return timeElement{nil}
	}
	return createTimeElement(format_timeElement_style_default, s.s)
}
func (t timeElement) ToTime() timeElement {
	return t.Copy()
}
func (i intElement) ToTime() timeElement {
	timeStr := strconv.Itoa(*i.i)
	return createTimeElement(format_timeElement_style_default, &timeStr)
}
func (f floatElement) ToTime() timeElement {
	timeStr := strconv.FormatFloat(float64(*f.f), 'f', 6, 64)
	return createTimeElement(format_timeElement_style_default, &timeStr)
}
func (b boolElement) ToTime() timeElement {
	return timeElement{nil}
}

func (s stringElement) ToFloat() floatElement {
	if s.s == nil {
		return floatElement{nil}
	}
	f, err := strconv.ParseFloat(*s.s, 64)
	if err != nil {
		return floatElement{nil}
	}
	return floatElement{&f}
}
func (i floatElement) ToFloat() floatElement {
	return i.Copy()
}
func (i intElement) ToFloat() floatElement {
	if i.i != nil {
		f := float64(*i.i)
		return floatElement{&f}
	}
	return floatElement{nil}
}
func (b boolElement) ToFloat() floatElement {
	if b.b == nil {
		return floatElement{nil}
	}
	var f float64
	if *b.b {
		f = 1.0
	} else {
		f = 0.0
	}
	return floatElement{&f}
}
func (t timeElement) ToFloat() floatElement {
	if t.t != nil {
		timeStr := t.t.Format("20060102150405")
		i, err := strconv.Atoi(timeStr)
		if err != nil {
			return floatElement{nil}
		}
		f := float64(i)
		return floatElement{&f}
	}
	return floatElement{nil}
}

func (s stringElement) ToBool() boolElement {
	if s.s == nil {
		return boolElement{nil}
	}
	var b bool
	if *s.s == "false" {
		b = false
		return boolElement{&b}
	}
	if *s.s == "true" {
		b = true
		return boolElement{&b}
	}
	return boolElement{nil}
}
func (i intElement) ToBool() boolElement {
	if i.i == nil {
		return boolElement{nil}
	}
	var b bool
	if *i.i == 1 {
		b = true
	}
	if *i.i == 0 {
		b = false
	}
	return boolElement{&b}
}
func (f floatElement) ToBool() boolElement {
	if f.f == nil {
		return boolElement{nil}
	}
	var b bool
	if *f.f == 1.0 {
		b = true
	} else if *f.f == 0.0 {
		b = false
	} else {
		return boolElement{nil}
	}
	return boolElement{&b}
}
func (i boolElement) ToBool() boolElement {
	return i.Copy()
}
func (t timeElement) ToBool() boolElement {
	var b bool
	return boolElement{&b}
}

func (s stringElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s <= *e.s
}
func (s intElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i <= *e.i
}
func (s floatElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f <= *e.f
}
func (s boolElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b && !*e.b {
		return false
	}
	return true
}
func (t timeElement) LessEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToTime()
	if t.IsNA() || e.IsNA() {
		return false
	}
	return (*t.t).Before(*e.t)
}

func (s stringElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s < *e.s
}
func (s intElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i < *e.i
}
func (s floatElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f < *e.f
}
func (s boolElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b {
		return false
	}
	if *e.b {
		return true
	}
	return false
}
func (t timeElement) Less(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToTime()
	if t.IsNA() || e.IsNA() {
		return false
	}
	return (*t.t).Before(*e.t)
}

func (s stringElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s >= *e.s
}
func (s intElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i >= *e.i
}
func (s floatElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f >= *e.f
}
func (s boolElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b {
		return true
	}
	if *e.b {
		return false
	}
	return true
}
func (t timeElement) GreaterEq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToTime()
	if t.IsNA() || e.IsNA() {
		return false
	}
	return (*t.t).After(*e.t)
}

func (s stringElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s > *e.s
}
func (s intElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i > *e.i
}
func (s floatElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f > *e.f
}
func (t timeElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToTime()
	if t.IsNA() || e.IsNA() {
		return false
	}
	return (*t.t).After(*e.t)
}

func (s boolElement) Greater(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	if *s.b && !*e.b {
		return true
	}
	return false
}

func (s stringElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToString()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.s == *e.s
}

func (s intElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToInt()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.i == *e.i
}

func (s floatElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToFloat()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.f == *e.f
}
func (t timeElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToTime()
	if t.IsNA() || e.IsNA() {
		return false
	}
	//use this will get : invalid indirect of t.t.Unix() (type int64)
	//time1 := *t.t.Unix()
	time1 := (*t.t).Unix()
	time2 := (*e.t).Unix()

	return time1 == time2
}

func (s boolElement) Eq(elem elementInterface) bool {
	if elem == nil {
		return false
	}
	e := elem.ToBool()
	if s.IsNA() || e.IsNA() {
		return false
	}
	return *s.b == *e.b
}

func (s stringElement) String() string {
	if s.s == nil {
		return "NA"
	}
	return *s.s
}
func (i intElement) String() string {
	if i.i == nil {
		return "NA"
	}
	return fmt.Sprintf("%d", *i.i)
}
func (f floatElement) String() string {
	if f.f == nil {
		return "NA"
	}
	return fmt.Sprintf("%.2f", *f.f)
}
func (t timeElement) String() string {
	if t.t == nil {
		return "NA"
	}
	return fmt.Sprintf("%v", *t.t)
}
func (b boolElement) String() string {
	if b.b == nil {
		return "NA"
	}
	if *b.b {
		return "true"
	}
	return "false"
}

func (s stringElement) Copy() stringElement {
	if s.s == nil {
		return stringElement{nil}
	}
	copy := *s.s
	return stringElement{&copy}
}

func (i intElement) Copy() intElement {
	if i.i == nil {
		return intElement{nil}
	}
	copy := *i.i
	return intElement{&copy}
}

func (t timeElement) Copy() timeElement {
	if t.t == nil {
		return timeElement{nil}
	}
	copy := *t.t
	return timeElement{&copy}
}

func (f floatElement) Copy() floatElement {
	if f.f == nil {
		return floatElement{nil}
	}
	copy := *f.f
	return floatElement{&copy}
}

func (b boolElement) Copy() boolElement {
	if b.b == nil {
		return boolElement{nil}
	}
	copy := *b.b
	return boolElement{&copy}
}

// IsNA returns true if the element is empty and viceversa
func (s stringElement) IsNA() bool {
	if s.s == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (i intElement) IsNA() bool {
	if i.i == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (f floatElement) IsNA() bool {
	if f.f == nil {
		return true
	}
	return false
}

func (t timeElement) IsNA() bool {
	if t.t == nil {
		return true
	}
	return false
}

// IsNA returns true if the element is empty and viceversa
func (b boolElement) IsNA() bool {
	if b.b == nil {
		return true
	}
	return false
}
