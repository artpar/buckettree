package main

import (
	"math"
	"fmt"
	"github.com/crackcomm/go-clitable"
)

type FlexBucket interface {
	AddValue(val int)
	Buckets() map[string]float64
}

type BucketImpl struct {
	min             float64
	max             float64
	total           int64
	numberOfBuckets int
	lengthOfBucket  float64
	bucketHigh      []float64
	bucketLow       []float64
	bucketCount     []int64
}

func (b *BucketImpl) Buckets() map[string]int64 {
	r := make(map[string]int64)
	for i := 0; i < b.numberOfBuckets; i++ {
		key := fmt.Sprintf("%v - %v", b.bucketLow[i], b.bucketHigh[i])
		val, ok := r[key]
		if !ok {
			r[key] = b.bucketCount[i]
		} else {
			r[key] = val + b.bucketCount[i]
		}
	}
	return r
}

func (b BucketImpl) PrintBuckets() {
	tab := clitable.New([]string{"i", "low", "high", "count"})
	for i := 0; i < b.numberOfBuckets; i++ {
		tab.AddRow(map[string]interface{}{
			"i": i,
			"low": b.bucketLow[i],
			"high": b.bucketHigh[i],
			"count": b.bucketCount[i],
		})
	}
	tab.Print()
}
func NewBucket(l int) BucketImpl {
	b := BucketImpl{
		min : math.MaxInt64,
		max : math.MinInt64,
		bucketCount : make([]int64, l),
		bucketLow : make([]float64, l),
		bucketHigh :make([]float64, l),
		numberOfBuckets : l,
		total : 0,
	}
	return b
}

func makeBuckets(start, length float64, count int) ([]float64, []float64) {
	//fmt.Printf("Making %d buckets from %v of length %v\n", count, start, length)
	low := make([]float64, count)
	high := make([]float64, count)
	for i := 0; i < count; i++ {
		low[i] = start
		start = start + length
		high[i] = start
	}
	return low, high
}

var EPSILON float64 = 0.00000001

func floatEquals(a, b float64) bool {
	if ((a - b) < EPSILON && (b - a) < EPSILON) {
		return true
	}
	return false
}

func resetBuckets(counts []int64, lows []float64, high []float64, count int, newMin, newMax float64) ([]int64, []float64, []float64) {
	fmt.Printf("Reset buckets, make %d buckets from %v -> %v\n", count, newMin, newMax)
	newLength := (newMax - newMin) / float64(count)
	fmt.Printf("New length: %v\n", newLength)
	newLows, newHighs := makeBuckets(newMin, newLength, count)
	if newHighs[count - 1] < newMax {
		newHighs[count - 1] = newMax + 1
	}
	for i := 0; i < count; i++ {
		fmt.Printf("%v\t", newLows[i])
	}
	fmt.Printf("\n")
	for i := 0; i < count; i++ {
		fmt.Printf("%v\t", newHighs[i])
	}
	fmt.Printf("\n")
	newCounts := make([]int64, count)
	totalRecs := len(counts)
	j := 0
	for i := 0; i < totalRecs; i++ {
		fmt.Printf("%d going to new bucket[%d] %v -> %v from[%d] %v -> %v\n", counts[i], j, newLows[j], newHighs[j], i, lows[i], high[i])
		if lows[i] >= newLows[j] && high[i] < newHighs[j] {
			fmt.Printf("Simple bucket merge %d to %d\n", i, j)
			newCounts[j] = newCounts[j] + counts[i]
		} else if lows[i] > newLows[j] && lows[i] <= newHighs[j] && high[i] >= newHighs[j] {
			leftSmall := newHighs[i] - lows[j]
			rightSmall := high[i] - newHighs[j]
			//leftBig := newHighs[j] - newLows[j]
			//rightBig := newHighs[j] + newLength
			//fmt.Printf(" %v --- %v\n", leftSmall, rightSmall)
			leftPart := int64((leftSmall * float64(counts[i])) / (leftSmall + rightSmall))
			rightPart := counts[i] - leftPart
			fmt.Printf("Change new high for %d to %v from %v\n", j, high[i], newHighs[j])
			newHighs[j] = high[i]
			if j + 1 < len(newLows) {
				newLows[j + 1] = newHighs[j]
				newHighs[j + 1] = newLows[j + 1] + newLength
			}
			newCounts[j] = newCounts[j] + leftPart + rightPart
		} else if lows[i] >= newHighs[j] {
			fmt.Printf("Increase j\n")
			j = j + 1
			i = i - 1
			//if j >= count {
			//	fmt.Printf("%v >= %v", lows[i], newHighs[j - 1], )
			//}
			//fmt.Printf("\t%d went to %v -> %v instead of %v -> %v\n", counts[i], newLows[j], newHighs[j], newLows[j - 1], newHighs[j - 1])
			//newCounts[j] = newCounts[j] + counts[i]
		}
	}
	return newCounts, newLows, newHighs
}

func (b *BucketImpl) AddValue(vali int) {
	val := float64(vali)
	fmt.Printf("Add: [%d] Number of buckets: %d\n", vali, b.numberOfBuckets)
	b.total = b.total + 1
	//fmt.Printf("Final total: %v\n", b.total)
	//fmt.Printf("Counts: %v\n", b.bucketCount)
	if b.total == 1 {
		fmt.Printf("This is the first value: %v\n", vali)
		b.min = val
		b.max = val
		b.lengthOfBucket = (b.min - b.max) / float64(b.numberOfBuckets)
		l, h := makeBuckets(b.min, b.lengthOfBucket, b.numberOfBuckets)
		b.bucketLow = l
		b.bucketHigh = h
	} else {
		if val < b.min || val > b.max {
			if val < b.min {
				b.min = val
			}else if val > b.max {
				b.max = val
			}
			b.PrintBuckets()
			c, l, h := resetBuckets(b.bucketCount, b.bucketLow, b.bucketHigh, b.numberOfBuckets, b.min, b.max)
			b.bucketCount = c
			b.bucketLow = l
			b.bucketHigh = h
		}
	}
	for i := 0; i < b.numberOfBuckets; i++ {
		if ( val - b.bucketLow[i] > 0.00  && b.bucketHigh[i] - val > 0.00) || (i == b.numberOfBuckets - 1) {
			b.bucketCount[i] = b.bucketCount[i] + 1
			break
		}
	}
	b.PrintBuckets()
}


