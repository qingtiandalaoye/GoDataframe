package dataframe

import (
	"fmt"
	"testing"
)

const stock_file_add string = "C:\\GOPATH\\GoBackTest\\test\\data\\000006.csv"

func TestRead_csv(t *testing.T) {
	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	csvFormat.csvFilePath = stock_file_add
	csvFormat.index_col = 0
	csvFormat.parse_dates = 0
	//dont need to paser date column
	csvFormat.date_paser_format = "2006-01-02"
	csvFormat.skiprows = 1

	//fmt.Printf("a file: %s\n", *csvFormat.csvFilePath)

	df, err := Read_csv(csvFormat)
	if err != nil {
		t.Errorf("read csv file error %v\n", err)
		t.FailNow()
	}

	fmt.Printf("\n%s", df)
}

func Test_Column(test *testing.T) {
	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	csvFormat.csvFilePath = stock_file_add
	csvFormat.index_col = 0
	csvFormat.parse_dates = 0
	//dont need to paser date column
	csvFormat.date_paser_format = "2006-01-02"
	csvFormat.skiprows = 1

	//fmt.Printf("a file: %s\n", *csvFormat.csvFilePath)

	df, err := Read_csv(csvFormat)
	if err != nil {
		test.Errorf("read csv file error %v\n", err)
		test.FailNow()
	}
	fmt.Printf("\n%s", df)

	shape := df.shape()
	if shape[0] < 0 || shape[1] < 0 {
		test.Errorf("shape error %s\n", shape)
		test.FailNow()
	}

	if se, err := df.column("open"); err == nil {
		if len(Values(se)) < 10 {
			test.Errorf("df.column(\"open\") get error\n")
			test.FailNow()
		} else {
			fmt.Printf("%s", se)
		}

	} else {
		test.Errorf("df.column(\"open\") get error\n", err)
		test.FailNow()
	}

	if _, err := df.column("highX"); err == nil {
		test.Errorf("df.column(\"highX\") should has error\n")
		test.FailNow()
	}

	m2 := df.indexOf(12)
	if m2 == nil {
		test.Errorf("should has value\n")
		test.FailNow()
	}
	fmt.Printf("%s", m2)
	fmt.Printf("%s", df.IndexType)

}

func Test_loc(test *testing.T) {
	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	csvFormat.csvFilePath = stock_file_add
	csvFormat.index_col = 0
	csvFormat.parse_dates = 0
	//dont need to paser date column
	csvFormat.date_paser_format = "2006-01-02"
	csvFormat.skiprows = 1

	//fmt.Printf("a file: %s\n", *csvFormat.csvFilePath)

	df, err := Read_csv(csvFormat)
	if err != nil {
		test.Errorf("read csv file error %v\n", err)
		test.FailNow()
	}
	//fmt.Printf("\n%s", df)
	strOftime := "2010-11-03 00:00:00"
	if result, err := createTimeElement("2006-01-02 15:04:05", &strOftime); err != nil {
		test.Errorf("createTimeElement error", err)
		test.Fail()
	} else {

		fmt.Printf("%s\n", result)
		m2 := df.loc(result)

		fmt.Printf("%s", m2)

		if m2 == nil {
			test.Errorf("should has loc value\n")
			test.FailNow()
		}

		floatVal := 3.79
		floatE := floatElement{&floatVal}
		if !Eq(m2["open"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		floatVal = 3.86
		floatE = floatElement{&floatVal}
		if !Eq(m2["high"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		floatVal = 3.81
		floatE = floatElement{&floatVal}
		if !Eq(m2["close"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		floatVal = 3.74
		floatE = floatElement{&floatVal}
		if !Eq(m2["low"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		floatVal = 13722173.00
		floatE = floatElement{&floatVal}
		if !Eq(m2["volume"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		floatVal = 102153632.00
		floatE = floatElement{&floatVal}
		if !Eq(m2["amount"], floatE) {
			test.Errorf("get the wrong loc value\n")
			test.FailNow()
		}

		//date	open	high	close	low	volume	amount
		//  2010-11-03 00:00:00 +0000 UTC   3.79   3.86   3.81   3.74   13722173.00   102153632.00

	}
}

func Test_ReferenceByAddress(test *testing.T) {

	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	csvFormat.csvFilePath = stock_file_add
	csvFormat.index_col = 0
	csvFormat.parse_dates = 0
	//dont need to paser date column
	csvFormat.date_paser_format = "2006-01-02"
	csvFormat.skiprows = 1

	//fmt.Printf("a file: %s\n", *csvFormat.csvFilePath)

	df, err := Read_csv(csvFormat)
	if err != nil {
		test.Errorf("read csv file error %v\n", err)
		test.FailNow()
	}
	fmt.Printf("for check the rederence by address manuelly :\n%s", df)
	fmt.Printf("the address of df Index:%p\n", df.Index)
	seriesList := df.columns

	for i := 0; i < len(seriesList); i++ {
		s := seriesList[i]
		fmt.Printf("the address of Series Index:%p\n", s.Index)
		fmt.Printf("the address of Series values:%p\n", s.values)
	}

}
