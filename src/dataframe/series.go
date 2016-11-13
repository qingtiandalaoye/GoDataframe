package dataframe

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const Int_type = "int"
const Float_type = "float"
const String_type = "string"
const Bool_type = "bool"
const Time_type = "time"

type Series struct {
	Name   string         // The name of the series
	t      string         // The type of the series
	Index  []elementValue // an Elements used as index
	values []elementValue
}

type SeriesInterface interface {
	Strings([]string) Series
	Ints([]int) Series
	Floats([]float64) Series
	Bools([]bool) Series
	Times(string, []string) Series

	NamedStrings(string, []string) Series
	NamedInts(string, []int) Series
	NamedFloats(string, []float64) Series
	NamedBools(string, []bool) Series
	NamedTimes(string, string, []string) Series

	Values(Series) []elementValue
	String(Series) string
	Len(Series) int
	Type() string

	setIndex(*Series, *[]elementValue) Series

	sort_index() SeriesInterface
	shift(int) SeriesInterface
	indexOf(int) elementValue      //get a row of Series, -1 is the last row
	loc(elementValue) elementValue //get a row of Series, elementValue is the index value
}

func setIndex(s *Series, index *[]elementValue) *Series {
	s.Index = *index
	return s
}

func (s *Series) loc(val elementValue) elementValue {
	if len(s.Index) == 0 {
		var empty string = ""
		return stringElement{&empty}
	}
	for i := 0; i < len(s.Index); i++ {
		s1, _ := ToString(val)
		s2, _ := ToString(s.Index[i])
		if strings.EqualFold(*s1.s, *s2.s) {
			return s.values[i]
		}
	}
	var empty string = ""
	return stringElement{&empty}
}

func (s *Series) indexOf(idx int) elementValue {
	if len(s.Index) <= idx {
		var empty string = ""
		return stringElement{&empty}
	}
	return s.values[idx]
}

func (s *Series) shift(idx int) *Series {
	//s.values[0] = nil
	s.values = s.values[1:len(s.values)]
	return s
}

func (s *Series) sort_index() *Series {
	//for i := 0; i < len(s.Index); i++ {
	//	s1, _ := ToString(val)
	//	s2, _ := ToString(s.Index[i])
	//	if s1.Eq(s2) {
	//		return s.values[i]
	//	}
	//}
	return s
}

// Strings is a constructor for a String series
func Strings(args []string) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	for i := 0; i < len(args); i++ {
		valuesArr[i] = stringElement{&args[i]}
	}
	//could not use the  "for i, v := range args"  here
	//the v will change to the latest address
	ret := Series{
		Name:   "",
		values: valuesArr,
		t:      String_type,
	}
	return ret
}

func Ints(args []int) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	for i := 0; i < len(args); i++ {
		valuesArr[i] = intElement{&(args[i])}
	}
	ret := Series{
		Name:   "",
		values: valuesArr,
		t:      Int_type,
	}
	return ret
}

func Floats(args []float64) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	for i := 0; i < len(args); i++ {
		valuesArr[i] = floatElement{&args[i]}
	}
	ret := Series{
		Name:   "",
		values: valuesArr,
		t:      Float_type,
	}
	return ret
}

func Bools(args []bool) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	for i := 0; i < len(args); i++ {
		valuesArr[i] = boolElement{&args[i]}
	}
	ret := Series{
		Name:   "",
		values: valuesArr,
		t:      Bool_type,
	}
	return ret
}

func Times(datetimeFormat string, args []string) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	for i := 0; i < len(args); i++ {
		valuesArr[i] = createTimeElement(datetimeFormat, &args[i])
	}
	ret := Series{
		Name:   "",
		values: valuesArr,
		t:      Time_type,
	}
	return ret
}

// NamedStrings is a constructor for a named String series
func NamedStrings(name string, args []string) Series {
	s := Strings(args)
	s.Name = name
	return s
}

// NamedInts is a constructor for a named Int series
func NamedInts(name string, args []int) Series {
	s := Ints(args)
	s.Name = name
	return s
}

// NamedFloats is a constructor for a named Float series
func NamedFloats(name string, args []float64) Series {
	s := Floats(args)
	s.Name = name
	return s
}

// NamedBools is a constructor for a named Bool series
func NamedBools(name string, args []bool) Series {
	s := Bools(args)
	s.Name = name
	return s
}

// NamedBools is a constructor for a named Bool series
func NamedTimes(name string, datetimeFormat string, args []string) Series {
	s := Times(datetimeFormat, args)
	s.Name = name
	return s
}

func Values(s Series) []elementValue {
	return s.values
}

// String implements the Stringer interface for Series
func String(s Series) string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "\nName: "+s.Name)
	}
	ret = append(ret, "Type: "+s.t)
	ret = append(ret, "Length: "+fmt.Sprint(Len(s)))
	if Len(s) != 0 {
		// Get the maximum number of characters of index
		maxIndexChars := 0
		if len(s.Index) > 0 {
			for _, v := range s.Index {
				//elementValue
				vStr, _ := ToString(v)
				if vStr.Len() > maxIndexChars {
					maxIndexChars = utf8.RuneCountInString(*vStr.s)
				}
			}
		}
		maxValueChars := 0
		if len(s.values) > 0 {
			for _, v := range s.values {
				//elementValue
				vStr, _ := ToString(v)
				if vStr.Len() > maxValueChars {
					maxValueChars = utf8.RuneCountInString(*vStr.s)
				}
			}
		}
		for i, v := range s.values {
			//fmt.Printf("--------A---1\n")
			//fmt.Printf("%d\n",i)
			//fmt.Printf("%v\n",v)
			//fmt.Printf("---------A--2\n")
			var aRow string
			if len(s.Index) >= i+1 {
				//has index title of this "i"
				idxStr, _ := ToString(s.Index[i])
				vStr, _ := ToString(v)
				aRow += AddLeftPadding(*idxStr.s, maxIndexChars+1) + AddLeftPadding(*vStr.s, maxValueChars+1)
			} else {
				vStr, _ := ToString(v)
				aRow += AddLeftPadding("", maxIndexChars+1) + AddLeftPadding(*vStr.s, maxValueChars+1)
			}
			ret = append(ret, aRow)
		}
	}
	return strings.Join(ret, "\n")
}

// Len returns the length of a given Series
func Len(s Series) int {
	return (len(s.values))
}

// Type returns the type of a given series
func (s Series) Type() string {
	return s.t
}
