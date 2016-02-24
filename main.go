package main

import (
//"fmt"
	"os"
	"encoding/csv"
//"encoding/json"
	"github.com/artpar/gisio/types"
	"github.com/artpar/difference/flexbuckets"
	"fmt"
)

type ColumnInfo struct {
	UniqueValueCount int
	IsEnum           bool
	ColumnNumber     int
	UniqueValues     map[string]int
	Type             types.EntityType
	Total            int
	Percent          int
}

type ColumnInfoS []ColumnInfo

func (a ColumnInfoS) Len() int {
	return len(a)
}
func (a ColumnInfoS) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ColumnInfoS) Less(i, j int) bool {
	return a[i].UniqueValueCount < a[j].UniqueValueCount
}

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	row, err := reader.Read();
	//	skipColumn := make([]int, len(row))
	colData := make([][]string, len(row))
	c := 10
	rows := make([][]string, 0)
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		if c < 0 {
			break
		}
		for colNo, value := range row {
			colData[colNo] = append(colData[colNo], value)
		}
		rows = append(rows, row)
		c = c - 1
	}

	typesList := make([]types.EntityType, len(row))
	for i := 0; i < len(row); i++ {
		values := colData[i]
		typ, _, _ := types.DetectType(values)
		typesList[i] = typ
	}

	fmt.Printf("Types are: %v\n", typesList)
	myBucket := flexbuckets.BuildTree(typesList)

	for _, oldRow := range rows {
		myBucket.AddRow(ToInterface(oldRow))
		fmt.Printf("%v", myBucket.PrintBuckets(""))
	}
	fmt.Printf("Completed old\n")
	c = 0
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		myBucket.AddRow(ToInterface(row))
		c = c + 1
		if c > 5 {
			c = 0
			fmt.Printf("%v", myBucket.PrintBuckets(""))
		}
	}

	fmt.Printf("%v", myBucket.PrintBuckets(""))
}

func ToInterface(oldRow []string) []interface{} {
	length := len(oldRow)
	q := make([]interface{}, length)
	for o, v := range oldRow {
		q[o] = v
	}
	return q
}