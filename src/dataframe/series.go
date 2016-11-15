package dataframe

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const Int_type = "int"
const Float_type = "float"
const String_type = "string"
const Bool_type = "bool"
const Time_type = "time"

type Series struct {
	Name      string         // The name of the series
	t         string         // The type of the series
	Index     []elementValue // an Elements used as index
	values    []elementValue
	RuneCount int
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

	sort_indexASC() *Series
	sort_indexDESC() *Series
	shift(int) SeriesInterface
	indexOf(int) elementValue      //get a row of Series, -1 is the last row
	loc(elementValue) elementValue //get a row of Series, elementValue is the index value
}

func (s *Series) setValues(values *[]elementValue) {
	s.values = *values
	var RuneCount int
	for _, v := range *values {
		if stringElement, error := ToString(v); error == nil {
			count := utf8.RuneCountInString(*stringElement.s)
			if count > RuneCount {
				RuneCount = count
			}
		}
	}
	s.RuneCount = RuneCount
}

func (s *Series) setIndex(index *[]elementValue) {
	s.Index = *index
}

func (s *Series) loc(val elementValue) elementValue {
	if len(s.Index) == 0 {
		var empty string = ""
		return stringElement{&empty}
	}
	s1, _ := ToString(val)
	for i := 0; i < len(s.Index); i++ {
		s2, _ := ToString(s.Index[i])
		if strings.EqualFold(*s1.s, *s2.s) {
			return s.values[i]
		}
	}
	var empty string = ""
	return stringElement{&empty}
}

func (s *Series) indexOf(idx int) elementValue {
	if idx < 0 {
		return s.values[len(s.Index)+idx]
	}
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

// Strings is a constructor for a String series
func Strings(args []string) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	var RuneCount int
	for i := 0; i < len(args); i++ {
		valuesArr[i] = stringElement{&args[i]}
		count := utf8.RuneCountInString(args[i])
		if count > RuneCount {
			RuneCount = count
		}
	}
	//could not use the  "for i, v := range args"  here
	//the v will change to the latest address
	ret := Series{
		Name:      "",
		values:    valuesArr,
		RuneCount: RuneCount,
		t:         String_type,
	}
	return ret
}

func Ints(args []int) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	var RuneCount int
	for i := 0; i < len(args); i++ {
		valuesArr[i] = intElement{&(args[i])}
		strOfInt := strconv.Itoa(args[i])
		count := utf8.RuneCountInString(strOfInt)
		if count > RuneCount {
			RuneCount = count
		}
	}
	ret := Series{
		Name:      "",
		values:    valuesArr,
		RuneCount: RuneCount,
		t:         Int_type,
	}
	return ret
}

func Floats(args []float64) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	var RuneCount int
	for i := 0; i < len(args); i++ {
		valuesArr[i] = floatElement{&args[i]}
		strOfInt := strconv.FormatFloat(float64(args[i]), 'f', 6, 64)
		count := utf8.RuneCountInString(strOfInt)
		if count > RuneCount {
			RuneCount = count
		}
	}
	ret := Series{
		Name:      "",
		values:    valuesArr,
		RuneCount: RuneCount,
		t:         Float_type,
	}
	return ret
}

func Bools(args []bool) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	var RuneCount int = 4
	for i := 0; i < len(args); i++ {
		valuesArr[i] = boolElement{&args[i]}
	}
	ret := Series{
		Name:      "",
		values:    valuesArr,
		RuneCount: RuneCount,
		t:         Bool_type,
	}
	return ret
}

func Times(datetimeFormat string, args []string) Series {
	var valuesArr []elementValue = make([]elementValue, len(args))
	var RuneCount int = len("1994-01-11 00:00:00 +0000 UTC")
	for i := 0; i < len(args); i++ {
		valuesArr[i], _ = createTimeElement(datetimeFormat, &args[i])
	}
	ret := Series{
		Name:      "",
		values:    valuesArr,
		RuneCount: RuneCount,
		t:         Time_type,
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
func (s Series) String() string {
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

		for i, v := range s.values {
			var aRow string
			if len(s.Index) >= i+1 {
				//has index title of this "i"
				idxStr, _ := ToString(s.Index[i])
				vStr, _ := ToString(v)
				aRow += AddLeftPadding(*idxStr.s, maxIndexChars+1) + AddLeftPadding(*vStr.s, s.RuneCount+2) //hard code 2
			} else {
				vStr, _ := ToString(v)
				aRow += AddLeftPadding("", maxIndexChars+1) + AddLeftPadding(*vStr.s, s.RuneCount+2) //hard code 2
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

func (s *Series) Sort_indexASC() *Series {
	return s.sort_index(true)
}

func (s *Series) Sort_indexDESC() *Series {
	return s.sort_index(false)
}

func (s *Series) sort_index(asc bool) *Series {
	if len(s.Index) == 0 {
		// do nothing
		return s
	}
	for i := 0; i < len(s.Index); i++ {
		for j := 0; j < len(s.Index)-1; j++ {
			jGreaterThanJPlus1 := false
			switch s.t {
			case Int_type:
				s1 := s.Index[j].(intElement)
				s2 := s.Index[j+1].(intElement)
				if s1.Greater(s2) {
					jGreaterThanJPlus1 = true
				}
			case Float_type:
				if s.Index[j].(floatElement).Greater(s.Index[j+1].(floatElement)) {
					jGreaterThanJPlus1 = true
				}
			case String_type:
				if s.Index[j].(stringElement).Greater(s.Index[j+1].(stringElement)) {
					jGreaterThanJPlus1 = true
				}
			case Bool_type:
				if s.Index[j].(boolElement).Greater(s.Index[j+1].(boolElement)) {
					jGreaterThanJPlus1 = true
				}
			case Time_type:
				if s.Index[j].(timeElement).Greater(s.Index[j+1].(timeElement)) {
					jGreaterThanJPlus1 = true
				}
			default:
				//do nothing
			}
			//s.Index[j] > s.Index[j + 1]
			if asc {
				if jGreaterThanJPlus1 {
					tmp := s.Index[j+1]
					s.Index[j+1] = s.Index[j]
					s.Index[j] = tmp
				}
			} else {
				if !jGreaterThanJPlus1 {
					tmp := s.Index[j+1]
					s.Index[j+1] = s.Index[j]
					s.Index[j] = tmp
				}
			}
		}
	}
	return s
}
