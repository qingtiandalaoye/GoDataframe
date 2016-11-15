package dataframe

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCreate_element(t *testing.T) {
	str := stringElement{}
	aString := "abc"
	str.s = &aString
	fmt.Printf("%s\n", str)
	if str.String() != "abc" {
		t.Errorf("string is not same")
		t.Fail()
	}

	inte := intElement{}
	integ := 789
	inte.i = &integ
	fmt.Printf("%s\n", inte)
	if inte.String() != "789" {
		t.Errorf("int is not same")
		t.Fail()
	}

	flo := floatElement{}
	floa := 89.098
	flo.f = &floa
	fmt.Printf("%.3f\n", *flo.f)
	fmt.Printf("%v\n", flo)

	//%.2f  is the default format
	if flo.String() != "89.10" {
		t.Errorf("float is not same")
		t.Fail()
	}

	bol := boolElement{}
	bole := true
	bol.b = &bole
	fmt.Printf("%s\n", bol)
	if bol.String() != "true" {
		t.Errorf("bool is not same")
		t.Fail()
	}

	tim := timeElement{}
	tim1 := time.Now()
	tim.t = &tim1
	fmt.Printf("%v\n", tim)
	fmt.Printf("%v\n", tim.t)
	fmt.Printf("%v\n", *tim.t)

	strOfTime := fmt.Sprintf("%v\n", *tim.t)
	//println(strOfTime)
	//println(tim.String())
	//println(string(tim.String()))

	//http://www.tuicool.com/articles/32Mn2q
	//go语言中，判断两个字符串是否相等，用
	//strings.EqualFold(str1, str2)
	if strings.EqualFold(tim.String(), strOfTime) {
		t.Errorf("time is not same")
		t.Fail()
	}
}

func TestToString_element(t *testing.T) {

	aString := "abc"
	str := stringElement{&aString}

	fmt.Printf("%s\n", str)
	if str.String() != "abc" {
		t.Errorf("string is not same")
		t.Fail()
	}

	fmt.Printf("length of string:%d\n", str.Len())

	inte := intElement{}
	integ := 789
	inte.i = &integ
	fmt.Printf("%s\n", inte)
	if inte.String() != "789" {
		t.Errorf("int is not same")
		t.Fail()
	}
	//convert to stringElement
	vStr, _ := ToString(inte)
	fmt.Printf("type of int:%v\n", vStr)
	fmt.Printf("length of int:%d\n", vStr.Len())

	flo := floatElement{}
	floa := 89.098
	flo.f = &floa
	fmt.Printf("%.3f\n", *flo.f)
	fmt.Printf("%v\n", flo)

	//%.2f  is the default format
	if flo.String() != "89.10" {
		t.Errorf("float is not same")
		t.Fail()
	}
	//convert to stringElement
	vStr, _ = ToString(flo)
	fmt.Printf("type of flo:%v\n", vStr)
	fmt.Printf("length of flo:%d\n", vStr.Len())

	bol := boolElement{}
	bole := true
	bol.b = &bole
	fmt.Printf("%s\n", bol)
	if bol.String() != "true" {
		t.Errorf("bool is not same")
		t.Fail()
	}
	//convert to stringElement
	vStr, _ = ToString(bol)
	fmt.Printf("type of bool:%v\n", vStr)
	fmt.Printf("length of bool:%d\n", vStr.Len())

	tim := timeElement{}
	tim1 := time.Now()
	tim.t = &tim1
	fmt.Printf("%v\n", tim)
	fmt.Printf("%v\n", tim.t)
	fmt.Printf("%v\n", *tim.t)

	strOfTime := fmt.Sprintf("%v\n", *tim.t)
	//println(strOfTime)
	//println(tim.String())
	//println(string(tim.String()))

	//http://www.tuicool.com/articles/32Mn2q
	//go语言中，判断两个字符串是否相等，用
	//strings.EqualFold(str1, str2)
	if strings.EqualFold(tim.String(), strOfTime) {
		t.Errorf("time is not same")
		t.Fail()
	}
	//convert to stringElement
	vStr, _ = ToString(tim)
	fmt.Printf("type of time:%v\n", vStr)
	fmt.Printf("length of time:%d\n", vStr.Len())
}

func TestTime_element(test *testing.T) {
	//could not paser other format
	aformat_timeElement_style1 := "2006-01-02 15:04:05"

	if result, err := createTimeElement("2006-01-02 15:04:05", &aformat_timeElement_style1); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}

	aformat_timeElement_style1 = "2016-11-10 15:16:17"
	if result, err := createTimeElement("2006-01-02 15:04:05", &aformat_timeElement_style1); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}

	aformat_timeElement_style1 = "2016-12-12"
	if result, err := createTimeElement("2006-01-02", &aformat_timeElement_style1); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}
	aformat_timeElement_style2 := "20160103190208"
	if result, err := createTimeElement("20060102150405", &aformat_timeElement_style2); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}
	aformat_timeElement_style3 := "2016/11/23 19:02:08"
	if result, err := createTimeElement("2006/01/02 15:04:05", &aformat_timeElement_style3); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}
	aformat_timeElement_style4 := "2016/10/23"
	if result, err := createTimeElement("2006/01/02", &aformat_timeElement_style4); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}

	aformat_timeElement_style5 := "20160105"
	if result, err := createTimeElement("20060102", &aformat_timeElement_style5); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {
		fmt.Printf("%s\n", result)
	}

}
func TestTime_element_eq(test *testing.T) {
	//could not paser other format
	aformat_timeElement_style1 := "2006-01-02 15:04:05"
	str1 := stringElement{&aformat_timeElement_style1}
	str2 := stringElement{&aformat_timeElement_style1}
	if !str1.Eq(str2) {
		test.Errorf("stringElement should same!")
		test.Fail()
	}

	if !Eq(str1, str2) {
		test.Errorf("stringElement should same!")
		test.Fail()
	}

	intE := 2006
	intEle := intElement{&intE}
	if Eq(intEle, str2) {
		test.Errorf("stringElement,intElement should not same!")
		test.Fail()
	}
}
