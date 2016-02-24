package numberbuckets

import (
	"math"
	"fmt"
	"errors"
//"reflect"
	"reflect"
	"bytes"
)

type BucketImpl struct {
	min               float64
	max               float64
	total             int64
	numberOfBuckets   int
	lengthOfBucket    float64
	bucketHigh        []float64
	bucketLow         []float64
	bucketCount       []int
	objects           []FlexBucket
	newBucketFunction func() FlexBucket
}

func (b *BucketImpl) Buckets() map[string]int {
	//b.PrintBuckets()
	r := make(map[string]int)
	//fmt.Printf("Number of buckets: %d\n", b.numberOfBuckets)
	keys := make([]string, b.numberOfBuckets)
	for i := 0; i < b.numberOfBuckets; i++ {
		key := fmt.Sprintf("%v - %v", b.bucketLow[i], b.bucketHigh[i])
		keys[i] = key
		val, ok := r[key]
		//fmt.Printf("[%s] => %v\n", key, val)
		if !ok {
			r[key] = b.bucketCount[i]
		} else {
			r[key] = val + b.bucketCount[i]
		}
	}

	return r
}

func (b *BucketImpl) PrintBuckets(tab string) string {
	var bi bytes.Buffer
	keys := make([]string, b.numberOfBuckets)
	for i := 0; i < b.numberOfBuckets; i++ {
		key := fmt.Sprintf("%v - %v", b.bucketLow[i], b.bucketHigh[i])
		keys[i] = key
		bi.WriteString(fmt.Sprintf("%s|-%s: %d\n", tab, key, b.bucketCount[i]))
		bi.WriteString(b.objects[i].PrintBuckets(tab + "|   "))
	}
	return bi.String()
}

func NewNumberRangeBucket(l int, newBucketFunction func() FlexBucket) FlexBucket {
	b := &BucketImpl{
		min : math.MaxInt64,
		max : math.MinInt64,
		bucketCount : make([]int, l),
		bucketLow : make([]float64, l),
		bucketHigh :make([]float64, l),
		numberOfBuckets : l,
		total : 0,
		objects: make([]FlexBucket, l),
		newBucketFunction: newBucketFunction,
	}
	for i, _ := range b.objects {
		b.objects[i] = newBucketFunction()
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

func mergeBuckets(oCounts []int, oLows, oHighs []float64, oObjects []FlexBucket, nCounts[]int, nLows, nHighs []float64, nObjects []FlexBucket, newBucketFunc func() FlexBucket) ([]int, []float64, []float64, []FlexBucket) {
	min := oLows[0]
	if min > nLows[0] {
		min = nLows[0]
	}
	max := oHighs[len(oHighs) - 1]
	if max < nHighs[len(nHighs) - 1] {
		max = nHighs[len(nHighs) - 1]
	}

	count := len(nCounts)
	newLength := (max - min) / float64(count)

	newLows, newHighs := makeBuckets(min, newLength, count)
	if newHighs[count - 1] < max {
		newHighs[count - 1] = max + 0.0001
	}
	newCounts := make([]int, count)
	newObjects := make([]FlexBucket, count)
	for i, _ := range newObjects {
		newObjects[i] = newBucketFunc()
	}
	newCounts, newLows, newHighs, newObjects = mergeOldToNew(oCounts, oLows, oHighs, oObjects, newCounts, newLows, newHighs, newObjects)
	newCounts, newLows, newHighs, newObjects = mergeOldToNew(nCounts, nLows, nHighs, nObjects, newCounts, newLows, newHighs, newObjects)

	return newCounts, newLows, newHighs, newObjects
}

func (b *BucketImpl) AddBuckets(x FlexBucket) {
	newB, ok := x.(*BucketImpl)
	if !ok {
		panic("Cannot add bucket of this type: " + reflect.TypeOf(x).String())
	}
	c, l, h, o := mergeBuckets(b.bucketCount, b.bucketLow, b.bucketHigh, b.objects, newB.bucketCount, newB.bucketLow, newB.bucketHigh, newB.objects, b.newBucketFunction)
	b.bucketCount = c
	b.bucketLow = l
	b.bucketHigh = h
	b.objects = o
}

func mergeOldToNew(oCounts []int, oLows, oHighs []float64, oObjects []FlexBucket, nCounts []int, nLows, nHighs []float64, nObjects []FlexBucket) ([]int, []float64, []float64, []FlexBucket) {
	newLength := nHighs[0] - nLows[0]
	newHalfLength := newLength / 2
	totalRecs := len(oCounts)
	count := len(nCounts)
	j := 0
	for i := 0; i < totalRecs; i++ {
		if j == count {
			if i != totalRecs - 1 || oCounts[i] != 0 {
				panic("What has happened")
			}
			break;
		}
		if nHighs[j] <= nLows[j] {
			nHighs[j] = nLows[j] + newLength
		}
		//fmt.Printf("Current I:%v\n", i)
		//fmt.Printf("%d going to new bucket[%d] %v -> %v from[%d] %v -> %v\n", oCounts[i], j, nLows[j], nHighs[j], i, oLows[i], nHighs[i])
		if oLows[i] >= nHighs[j] {
			//fmt.Printf("Skip Merge, check next new bucket\n")
			j = j + 1
			i = i - 1
			continue
		}
		if oHighs[i] <= nHighs[j] {
			//fmt.Printf("Simple merge %d to %d\n", i, j)
			nCounts[j] = nCounts[j] + oCounts[i]
			//nObjects[j] = append(nObjects[j], oObjects[i]...)
			nObjects[j].AddBuckets(oObjects[i])
			// todo: merge objects
			continue
		}
		diffLength := oHighs[i] - nHighs[j]
		if (diffLength > newHalfLength && i > 0 && (oHighs[i - 1] - nLows[j] > newHalfLength)) || (i + 1 == totalRecs) {
			//fmt.Printf("Remaining length is more then new half length, shortening current high from %v to %v\n", nHighs[j], oHighs[i - 1])
			if oHighs[i - 1] > nLows[j] {
				nHighs[j] = oHighs[i - 1]
			} else {
				nHighs[j] = oHighs[i]
				if j < count - 1 {
					nLows[j + 1] = nHighs[j]
					nHighs[j + 1] = nLows[j + 1] + newLength
				}
				nCounts[j] = nCounts[j] + oCounts[i]
				//nObjects[j] = append(nObjects[j], oObjects[i]...)
				nObjects[j].AddBuckets(oObjects[i])
				// todo: merge objects
				continue
			}
			if j < count - 1 {
				nLows[j + 1] = nHighs[j]
				i = i - 1
				j = j + 1
			} else {
				nCounts[j] = nCounts[j] + oCounts[i]
				//nObjects[j] = append(nObjects[j], oObjects[i]...)
				nObjects[j].AddBuckets(oObjects[i])
				// todo: merge objects
			}
		} else {
			//fmt.Printf("This belongs to current\n")
			nHighs[j] = oHighs[i]
			if j + 1 < count {
				nLows[j + 1] = nHighs[j]
				nHighs[j + 1] = nLows[j + 1] + newLength
			}
			nCounts[j] = nCounts[j] + oCounts[i]
			//nObjects[j] = append(nObjects[j], oObjects[i]...)
			nObjects[j].AddBuckets(oObjects[i])
			// todo: merge objects
		}
		if diffLength <= 0 {
			//fmt.Printf("Time to inc j from %v\n", j)
			j = j + 1
		} else {
			//fmt.Printf("I think everyone missed this: %v => %v\n", nLows[i], nHighs[i])
		}
	}
	for ; j < count; j++ {
		if nHighs[j] <= nLows[j] {
			nHighs[j] = nLows[j] + newLength
		}
		if j + 1 < count {
			nLows[j + 1] = nHighs[j]
		}
	}
	return nCounts, nLows, nHighs, nObjects
}

func resetBuckets(counts []int, lows []float64, high []float64, oObjects []FlexBucket, count int, newMin, newMax float64, newF func() FlexBucket) ([]int, []float64, []float64, []FlexBucket) {
	//fmt.Printf("Reset buckets, make %d buckets from %v -> %v\n", count, newMin, newMax)
	newLength := (newMax - newMin) / float64(count)
	//fmt.Printf("New length: %v\n", newLength)
	newLows, newHighs := makeBuckets(newMin, newLength, count)
	if newHighs[count - 1] < newMax {
		newHighs[count - 1] = newMax + 0.0001
	}
	newCounts := make([]int, count)
	newObjects := make([]FlexBucket, count)
	for i, _ := range newObjects {
		newObjects[i] = newF()
	}
	return mergeOldToNew(counts, lows, high, oObjects, newCounts, newLows, newHighs, newObjects)
	//return newCounts, newLows, newHighs, newObjects
}

func (b *BucketImpl) AddAllValues(vals ...interface{}) {
	//fmt.Printf("Add all values: %v\n", vals)
	for _, v := range vals {
		b.AddValue(v.(int))
	}
}

func (b *BucketImpl) AddRow(row []interface{}) {
	if len(row) < 1 {
		return
	}
	val := row[0].(int)
	bucketNumber := b.AddValue(val)
	//fmt.Printf("Objects: %v\n", b.objects)
	b.objects[bucketNumber.(int)].AddRow(row[1:])
}

func (b *BucketImpl) AddValue(vali interface{}) interface{} {
	////fmt.Printf("Add value: %d\n", vali)
	val := float64(vali.(int))
	//fmt.Printf("Add: [%d] Number of buckets: %d\n", vali, b.numberOfBuckets)
	b.total = b.total + 1
	//fmt.Printf("Final total: %v\n", b.total)
	//fmt.Printf("Counts: %v\n", b.bucketCount)
	if b.total == 1 {
		//fmt.Printf("This is the first value: %v\n", vali)
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
			c, l, h, o := resetBuckets(b.bucketCount, b.bucketLow, b.bucketHigh, b.objects, b.numberOfBuckets, b.min, b.max, b.newBucketFunction)
			if h[len(h) - 1] > b.max {
				b.max = h[len(h) - 1]
			}
			b.bucketCount = c
			b.bucketLow = l
			b.bucketHigh = h
			b.objects = o
		}
	}
	var i int
	for i = 0; i < b.numberOfBuckets; i++ {
		if ( val - b.bucketLow[i] >= 0.00  && b.bucketHigh[i] - val > 0.00) || (i == b.numberOfBuckets - 1) {
			//fmt.Printf("%v added to bucket %d\n", val, i)
			b.bucketCount[i] = b.bucketCount[i] + 1
			break
		}
	}
	if (i == b.numberOfBuckets) {
		panic(errors.New("I was not added in any bucket"))
	}
	return i
}


