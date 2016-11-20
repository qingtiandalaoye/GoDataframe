package dataframe

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// .T
func TransposeRecords(x [][]string) [][]string {
	n := len(x)
	if n == 0 {
		return x
	}
	m := len(x[0])
	y := make([][]string, m)
	for i := 0; i < m; i++ {
		z := make([]string, n)
		for j := 0; j < n; j++ {
			z[j] = x[j][i]
		}
		y[i] = z
	}
	return y
}

func AddRightPadding(s string, nchar int) string {
	if utf8.RuneCountInString(s) < nchar {
		return s + strings.Repeat(" ", nchar-utf8.RuneCountInString(s))
	}
	return s
}

func AddLeftPadding(s string, nchar int) string {
	if utf8.RuneCountInString(s) < nchar {
		return strings.Repeat(" ", nchar-utf8.RuneCountInString(s)) + s
	}
	return s
}

func FindType(arr []string) string {
	hasFloats := false
	hasInts := false
	hasBools := false
	hasStrings := false
	for _, str := range arr {
		if _, err := strconv.Atoi(str); err == nil {
			hasInts = true
			continue
		}
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			hasFloats = true
			continue
		}
		if str == "true" || str == "false" {
			hasBools = true
			continue
		}
		if str == "" || str == "NA" {
			continue
		}
		hasStrings = true
	}
	if hasFloats && !hasBools && !hasStrings {
		return "float"
	}
	if hasInts && !hasFloats && !hasBools && !hasStrings {
		return "int"
	}
	if !hasInts && !hasFloats && hasBools && !hasStrings {
		return "bool"
	}
	return "string"
}

func Range(start, end int) []int {
	if start > end {
		start, end = end, start
	}
	var arr []int
	for i := start; i <= end; i++ {
		arr = append(arr, i)
	}
	return arr
}

func Seq(start, end, step int) []int {
	if start > end {
		start, end = end, start
	}
	if step == 0 {
		return []int{}
	}
	var arr []int
	if step < 0 {
		step = int(math.Abs(float64(step)))
		for i := end; i >= start; i = i - step {
			arr = append(arr, i)
		}
		return arr
	} else {
		for i := start; i <= end; i = i + step {
			arr = append(arr, i)
		}
		return arr
	}
}

func OrOfBool(a []bool, b []bool) ([]bool, error) {
	if len(a) != len(b) {
		return nil, errors.New("Different lengths")
	}
	ret := make([]bool, len(a), len(a))
	for i := 0; i < len(a); i++ {
		ret[i] = a[i] || b[i]
	}
	return ret, nil
}

func IsInIntSlice(i int, is []int) bool {
	for _, v := range is {
		if v == i {
			return true
		}
	}
	return false
}
