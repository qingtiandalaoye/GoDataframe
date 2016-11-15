package dataframe

import (
	"fmt"
	"testing"
)

func TestRead_csv(t *testing.T) {
	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	stock_file_add := "\\zwdat\\cn\\day\\000006.csv"
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

func Test_Get_Column_of_df(t *testing.T) {
	//fmt.Printf("running %s\n", "Read_csv")
	csvFormat := CsvPaserFormatter{}

	stock_file_add := "\\zwdat\\cn\\day\\000006.csv"
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

	shape := df.shape()
	if shape[0] < 0 || shape[1] < 0 {
		t.Errorf("shape error %s\n", shape)
		t.FailNow()
	}

	if se, err := df.column("open"); err == nil {
		if len(Values(se)) < 10 {
			t.Errorf("df.column(\"open\") get error\n")
			t.FailNow()
		} else {
			fmt.Printf("%s", se)
		}

	} else {
		t.Errorf("df.column(\"open\") get error\n", err)
		t.FailNow()
	}

	if _, err := df.column("highX"); err == nil {
		t.Errorf("df.column(\"highX\") should has error\n")
		t.FailNow()
	}

	m2 := df.indexOf(12)
	fmt.Printf("%s", m2)

}
