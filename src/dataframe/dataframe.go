package dataframe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

type DataFrame struct {
	Name        string
	columns     []Series
	ColumnNames []string
	ncols       int
	nrows       int
	Index       []elementValue // an Elements used as index
}

type DataFrameInterface interface {
	filterTimeIndexByRange(time.Time, time.Time) DataFrameInterface
	sort_index() DataFrameInterface
	tailOf(int) string
	headOf(int) string
	tail() string
	head() string
	shape() []int
	indexOf(int) map[string]elementValue      //get a row of dataframe, -1 is the last row
	loc(elementValue) map[string]elementValue //get a row of dataframe, elementValue is the index value

	//some thing about  :  df["sma_0"] = df["ma_" + str(shortPeriod)]
	//df["dvix"] = df["dprice"] / df["dprice"].shift(1) * 100
	//row["dprice"] <= row["sma_9"]:

}

var EMPTY DataFrame

func init() {
	EMPTY = DataFrame{}
}

//
//pd.read_csv(rss,
//engine='c',
//delimiter = ",", encoding="gbk", names=["date_str","hour_min","open", "high", "low", "close", "volume", "hold_amount", "adj_close"],
//dtype={"date_str" : np.str,"hour_min" : np.str},skip_blank_lines=True, skiprows=2, #skipfooter=2,
//error_bad_lines=True, warn_bad_lines = True)

type CsvPaserFormatter struct {
	csvFilePath       string //point to the file path string
	index_col         int    //indecate which is the index
	parse_dates       int    //indecator which is a date format
	date_paser_format string //indecator the date paser format
	skiprows          int
	skipfooter        int
}

func sort_index() DataFrame {
	return DataFrame{}
}

func New(series ...Series) DataFrame {
	return DataFrame{}
}

//like  pandas.read_csv(fileFullPath, index_col=0,parse_dates=[0])
//csvFilePath string, index_col int, parse_dates []int
func Read_csv(paserFormat CsvPaserFormatter) (DataFrame, error) {

	fileContent, _ := ioutil.ReadFile(paserFormat.csvFilePath)
	r := csv.NewReader(strings.NewReader(string(fileContent)))

	var records [][]string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return EMPTY, err
		}

		records = append(records, record)
	}

	return PaserCSV(records, paserFormat)
}

func PaserCSV(records [][]string, paserFormat CsvPaserFormatter) (DataFrame, error) {

	if len(records) == 0 {
		return DataFrame{}, errors.New("Empty records")
	}
	//paserFormat.skiprows
	if paserFormat.skiprows <= 0 {
		paserFormat.skiprows = 0
	}
	if paserFormat.skipfooter <= 0 {
		paserFormat.skipfooter = 0
	}

	records = TransposeRecords(records)

	var columnNames = make([]string, len(records))

	seriesArr := make([]Series, len(records))

	//fmt.Printf("len(records) =%d\n", len(records))
	for i := 0; i < len(records); i++ {
		//use the first line as column names
		columnNames[i] = records[i][0]

		//fmt.Printf("len(row) =%d", len(row))
		fmt.Printf("%s \n", records[i])

		newSeries := Series{}
		newSeries.Name = records[i][0]
		var stringArr = make([]string, len(records[i])-paserFormat.skipfooter-paserFormat.skiprows)
		stringArrIndex := 0

		for j := paserFormat.skiprows; j < len(records[i])-paserFormat.skipfooter; j++ {
			//all use stringElement
			stringArr[stringArrIndex] = records[i][j]
			stringArrIndex += 1

		}
		isIntType := true
		isFloatType := true
		isBoolType := true
		isStringType := true
		stringElementArr := Values(Strings(stringArr))
		var floatElementArr = make([]floatElement, len(stringElementArr))
		var intElementArr = make([]intElement, len(stringElementArr))
		var boolElementArr = make([]boolElement, len(stringElementArr))
		for i := 0; i < len(stringElementArr); i++ {
			intElementArr[i] = stringElementArr[i].(stringElement).ToInt()
			if intElementArr[i].i == nil {
				isIntType = false
			}
			floatElementArr[i] = stringElementArr[i].(stringElement).ToFloat()
			if floatElementArr[i].f == nil {
				isFloatType = false
			}
			boolElementArr[i] = stringElementArr[i].(stringElement).ToBool()
			if boolElementArr[i].b == nil {
				isBoolType = false
			}
		}

		if isFloatType {
			var valuesArr []elementValue = make([]elementValue, len(floatElementArr))
			for i := 0; i < len(floatElementArr); i++ {
				valuesArr[i] = floatElementArr[i]
			}
			seriesArr[i].values = valuesArr
		}
		if isIntType {
			var valuesArr []elementValue = make([]elementValue, len(intElementArr))
			for i := 0; i < len(intElementArr); i++ {
				valuesArr[i] = intElementArr[i]
			}
			seriesArr[i].values = valuesArr
		}
		if isBoolType {
			var valuesArr []elementValue = make([]elementValue, len(boolElementArr))
			for i := 0; i < len(boolElementArr); i++ {
				valuesArr[i] = boolElementArr[i]
			}
			seriesArr[i].values = valuesArr
		}
		if isStringType {
			seriesArr[i].values = stringElementArr
		}

	}
	//make special column to timeElement
	anyElementArr := Values(seriesArr[paserFormat.parse_dates])
	var timeElementArr = make([]timeElement, len(anyElementArr))
	for i := 0; i < len(anyElementArr); i++ {
		timeElementArr[i] = anyElementArr[i].(stringElement).ToTime()
	}

	var valuesArr []elementValue = make([]elementValue, len(timeElementArr))
	for i := 0; i < len(timeElementArr); i++ {
		valuesArr[i] = timeElementArr[i]
	}
	seriesArr[paserFormat.parse_dates].values = valuesArr

	resultDataFrame := DataFrame{}
	resultDataFrame.columns = seriesArr
	resultDataFrame.ColumnNames = columnNames
	resultDataFrame.Index = Values(seriesArr[paserFormat.index_col])
	resultDataFrame.ncols = len(seriesArr)
	resultDataFrame.nrows = len(resultDataFrame.Index)
	fmt.Printf("%s \t", resultDataFrame)

	return resultDataFrame, nil
}
