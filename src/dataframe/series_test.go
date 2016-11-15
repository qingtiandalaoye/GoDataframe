package dataframe

import (
	"fmt"
	"strings"
	"testing"
)

func TestCreate_series(t *testing.T) {
	var se Series = Strings([]string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	fmt.Printf("%s\n", se)

	se = Ints([]int{123456, 678, 789})
	fmt.Printf("%s\n", se)

	se = Floats([]float64{123456.01, 56.7, 78.9})
	fmt.Printf("%s\n", se)

	se = Bools([]bool{true, true, false})
	fmt.Printf("%s\n", se)

	se = Times("2006-01-02", []string{"2016-01-02", "2016-12-12", "2016-12-11"})
	fmt.Printf("%s\n", se)
}

func TestCreate_named_series(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	fmt.Printf("string of Series is: %s\n", String(se))

	se = NamedInts("name1", []int{123456, 678, 789, 90})
	fmt.Printf("string of Series is: %s\n", String(se))

	se = NamedFloats("name1", []float64{123456.01, 56.7, 78.9})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	se = NamedBools("name1", []bool{true, true, false, true, true, false})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	se = NamedTimes("name1", "2006-01-02", []string{"2016-01-01", "2016-12-12", "2016-12-29"})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))
}

func TestCreate_named_series_value(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	fmt.Printf("%s  type=%s  length=%d \n", se, se.Type(), Len(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}

	se = NamedInts("name1", []int{123456, 678, 789})
	fmt.Printf("%s  type=%s  length=%d \n", se, se.Type(), Len(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}

	se = NamedFloats("name1", []float64{123456.01, 56.7, 78.9})
	fmt.Printf("%s  type=%s  length=%d \n", se, se.Type(), Len(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}

	se = NamedBools("name1", []bool{true, true, false})
	fmt.Printf("%s  type=%s  length=%d \n", se, se.Type(), Len(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}

	se = NamedTimes("name1", "2006-01-02", []string{"2016-01-01", "2016-12-12", "2016-12-11"})
	fmt.Printf("%s  type=%s  length=%d \n", se, se.Type(), Len(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}
}

func TestSet_Index_series(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}
	val := Values(se)
	myse := setIndex(&se, &val)
	se = *myse

	fmt.Printf("%s  string of Series is: %s\n", se, String(se))
}

func TestSet_Index_shift(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}
	se.shift(1)
	fmt.Printf("after shift \n")
	for i, v := range Values(se) {
		fmt.Printf("%d, %s\n", i, v)
	}
}

func TestSet_IndexOf_series(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "", "1", "2", "true", "false", "123456.01", "78.9", "2016-01-01", "2016-12-31"})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	val := Values(se)
	myse := setIndex(&se, &val)
	se = *myse

	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	x, _ := ToString(se.indexOf(0))
	fmt.Printf("indexOf(0) of Series is: %s\n", x)
	if !strings.EqualFold(*x.s, "A") {
		t.Error("not match")
		t.Fail()
	}

	xy, _ := ToString(se.indexOf(3))

	fmt.Printf("indexOf(3) of Series is: %s\n", xy)
	if !strings.EqualFold(*xy.s, "1") {
		t.Error("not match")
		t.Fail()
	}

	st := "123456.01"
	xy, _ = ToString(se.loc(stringElement{&st}))

	fmt.Printf("loc of Series is: %s\n", xy)
	if !strings.EqualFold(*xy.s, "123456.01") {
		t.Error("not match")
		t.Fail()
	}

}

func TestSet_order_series(t *testing.T) {
	var se Series = NamedStrings("nameABC", []string{"A", "B", "C", "DE", "fg", "1", "2", "34", "56", "35", "BB"})
	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	val := Values(se)
	myse := setIndex(&se, &val)
	se = *myse

	fmt.Printf("%s  string of Series is: %s\n", se, String(se))

	sasc := se.Sort_indexASC()
	fmt.Printf("%s  string of Series is: %s\n", *sasc, String(*sasc))
	sdesc := se.Sort_indexDESC()
	fmt.Printf("%s  string of Series is: %s\n", *sdesc, String(*sdesc))

}
