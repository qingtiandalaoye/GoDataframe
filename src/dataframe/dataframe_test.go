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
		fmt.Printf("error %s\n", err)
		t.Errorf("read csv file error %v\n", err)
		t.FailNow()
	}
	fmt.Printf("result %s", df)
}
