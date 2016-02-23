package main

import (
	//"fmt"
	"os"
	"encoding/csv"
	//"encoding/json"
	"sort"
	"github.com/artpar/gisio/types"
	"github.com/artpar/difference/numberbuckets"
	"strconv"
	"encoding/json"
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
	buckets := make([]*numberbuckets.BucketImpl, len(row))
	for row, err = reader.Read(); err == nil; row, err = reader.Read() {
		total += 1
		for colNo, value := range row {
			column := infoMap[colNo]
			column.Total = total
			column.Percent = column.UniqueValueCount * 100 / column.Total
			_, ok := column.UniqueValues[value]
			if buckets[colNo] != nil {
				ival, _ := strconv.ParseInt(value, 10, 32)
				// fmt.Printf("Add value to column[%d]: %v\n", colNo, ival)
				buckets[colNo].AddValue(int(ival))
			}
			if column.Percent > 14 && column.Total > 100 {
				if column.IsEnum {
					// fmt.Printf("IsEnum: false for %v at line %d with current percent: %d\n", value, total, column.Percent)
					str := make([]string, 0)
					for s, _ := range column.UniqueValues {
						str = append(str, s)
					}
					typ, hasHeaders, err := types.DetectType(str)
					if err != nil {
						panic(err)
					}
					column.Type = typ
					if hasHeaders {
						// fmt.Printf("Do something about this")
					}
					if column.Type == types.Number {
						ints := make([]int, len(str))
						for i, s := range str {
							ival, err := strconv.ParseInt(s, 10, 32)
							if err != nil {
								panic(err)
							}
							ints[i] = int(ival)
						}
						x := numberbuckets.NewBucket(100)
						buckets[colNo] = &x
						buckets[colNo].AddAllValues(ints...)
					}
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

					str := make([]string, 0)
					for s, _ := range column.UniqueValues {
						str = append(str, s)
					}
					typ, _, err := types.DetectType(str)
					if err != nil {
						column.Type = types.None
					} else {
						column.Type = typ
					}
				}
			}
			infoMap[colNo] = column
		}
	}
	for i, col := range infoMap {
		if buckets[i] != nil {
			ranges := buckets[i].Buckets()
			col.UniqueValues = ranges
			infoMap[i] = col
		}
	}
	sort.Sort(ColumnInfoS(infoMap))
	js, err := json.MarshalIndent(&infoMap, "", "    ")
	if err != nil {
		panic(err)
	}

	 fmt.Printf("%v", string(js))
}