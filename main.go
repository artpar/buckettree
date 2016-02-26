package main

import (
//"fmt"
	"os"
	"encoding/csv"
//"encoding/json"
	"github.com/artpar/gisio/types"
	"github.com/artpar/buckettree/flexbuckets"
	"fmt"
	_ "net/http/pprof"
	"log"
	"net/http"
	"sort"
	"encoding/json"
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
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
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
	colValues := make([]ColumnInfo, len(row))
	for i, _ := range colValues {
		colValues[i] = ColumnInfo{ColumnNumber: i, UniqueValues: make(map[string]int)}
	}
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		if c < 1 {
			break
		}
		for colNo, value := range row {
			colData[colNo] = append(colData[colNo], value)
			_, ok := colValues[colNo].UniqueValues[value]
			colValues[colNo].Total = colValues[colNo].Total + 1
			if ok {
				colValues[colNo].UniqueValues[value] = colValues[colNo].UniqueValues[value] + 1
			} else {
				colValues[colNo].UniqueValues[value] = 1
				colValues[colNo].UniqueValueCount = colValues[colNo].UniqueValueCount + 1
			}
		}
		rows = append(rows, row)
		c = c - 1
	}
	sort.Sort(ColumnInfoS(colValues))
	for i, c := range colValues {
		values := MapKeys(c.UniqueValues)
		typ, _, _ := types.DetectType(values)
		colValues[i].Type = typ
		colValues[i].Percent = colValues[i].UniqueValueCount * 100 / colValues[i].Total
		if colValues[i].Percent < 13 {
			colValues[i].IsEnum = true
		}
	}
	j, _ := json.MarshalIndent(colValues, "", "    ")
	fmt.Printf("Column Counts: %v\n", string(j))

	typesList := make([]flexbuckets.BucketType, len(row))
	for i, c := range colValues {
		typ := c.Type
		if typ == types.Number && !c.IsEnum {
			typesList[i] = flexbuckets.NumberBucketType
		} else {
			if c.IsEnum {
				typesList[i] = flexbuckets.IdentityBucketType
			} else {
				typesList[i] = flexbuckets.SingleBucketType
			}
		}
	}

	fmt.Printf("Types are: %v\n", typesList)
	myBucket := flexbuckets.BuildTree(typesList)

	convertedRow := make([]string, len(colValues))
	for _, oldRow := range rows {
		for i, c := range colValues {
			convertedRow[i] = oldRow[c.ColumnNumber]
		}
		//fmt.Printf("Old1: %v\nNew1: %v\n\n", oldRow, convertedRow)
		myBucket.AddRow(ToInterface(convertedRow))
		//fmt.Printf("%v", myBucket.PrintBuckets(""))
	}
	//fmt.Printf("Completed old\n")
	c = 0
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		for i, c := range colValues {
			convertedRow[i] = row[c.ColumnNumber]
		}
		//fmt.Printf("Old2: %v\nNew2: %v\n\n", row, convertedRow)
		myBucket.AddRow(ToInterface(convertedRow))
		c = c + 1
		if c % 1000 == 0 {
			fmt.Printf("Completed %d rows\n", c)
		}
	}

	fmt.Printf("%v", myBucket.PrintBuckets(""))
}

func MapKeys(m map[string]int) []string {
	x := make([]string, 0)
	for k, _ := range m {
		x = append(x, k)
	}
	return x
}

func ToInterface(oldRow []string) []interface{} {
	length := len(oldRow)
	q := make([]interface{}, length)
	for o, v := range oldRow {
		q[o] = v
	}
	return q
}