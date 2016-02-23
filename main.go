package main

import (
	"fmt"
	"os"
	"encoding/csv"
	"encoding/json"
	"sort"
)

type ColumnInfo struct {
	UniqueValueCount int
	IsEnum           bool
	ColumnNumber int
	UniqueValues     map[string]int
	Total            int
	Percent          int
}

type ColumnInfoS []ColumnInfo

func (a ColumnInfoS) Len() int           { return len(a) }
func (a ColumnInfoS) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ColumnInfoS) Less(i, j int) bool { return a[i].UniqueValueCount < a[j].UniqueValueCount }

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(file)
	row, err := reader.Read();
	infoMap := make([]ColumnInfo, len(row))
	//	skipColumn := make([]int, len(row))
	for i, _ := range infoMap {
		infoMap[i] = ColumnInfo{
			UniqueValues:make(map[string]int),
			UniqueValueCount: 0,
			IsEnum:true,
			ColumnNumber: i,
		}
	}
	total := 0
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		total += 1
		for colNo, value := range row {
			column := infoMap[colNo]
			column.Total = total
			column.Percent = column.UniqueValueCount * 100 / column.Total
			_, ok := column.UniqueValues[value]
			if column.Percent > 14 && column.Total > 100 {
				if column.IsEnum {
					fmt.Printf("IsEnum: false for %v at line %d with current percent: %d\n", value, total, column.Percent)
					column.UniqueValues = make(map[string]int)
				}

				column.IsEnum = false
				if !ok {
					column.UniqueValueCount = 1 + column.UniqueValueCount
				}
			} else {
				if ok {
					column.UniqueValues[value] = column.UniqueValues[value] + 1
				} else {
					column.UniqueValueCount = 1 + column.UniqueValueCount
					column.UniqueValues[value] = 1
				}
			}
			infoMap[colNo] = column
		}
	}
	sort.Sort(ColumnInfoS(infoMap))
	js, err := json.MarshalIndent(&infoMap, "", "    ")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v", string(js))
}