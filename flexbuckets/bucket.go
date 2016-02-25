package flexbuckets

import (
	"math"
	"fmt"
	"errors"
//"reflect"
	"reflect"
	"bytes"
	"strconv"
	"os"
//"time"
)

type BucketImpl struct {
	min                float64
	max                float64
	total              int64
	numberOfBuckets    int
	lengthOfBucket     float64
	bucketHigh         []float64
	bucketLow          []float64
	bucketCount        []int
	objects            []FlexBucket
	builderMap         []func(i int, m []interface{}) FlexBucket
	originalBuilderMap []interface{}
	columnIndex        int
}

func (b *BucketImpl) Buckets() map[string]int {
	//b.PrintBuckets()
	r := make(map[string]int)
	//// // fmt.Printf( cur() + ": Number of buckets: %d\n", b.numberOfBuckets)
	keys := make([]string, b.numberOfBuckets)
	for i := 0; i < b.numberOfBuckets; i++ {
		key := fmt.Sprintf("%v - %v", b.bucketLow[i], b.bucketHigh[i])
		keys[i] = key
		val, ok := r[key]
		//// // fmt.Printf( cur() + ": [%s] => %v\n", key, val)
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
		if b.bucketCount[i] < 1 {
			continue
		}
		key := fmt.Sprintf("%v - %v", b.bucketLow[i], b.bucketHigh[i])
		keys[i] = key
		bi.WriteString(fmt.Sprintf("%s|-%s: %d\n", tab, key, b.bucketCount[i]))
		bi.WriteString(b.objects[i].PrintBuckets(tab + "|   "))
	}
	return bi.String()
}

func NewNumberRangeBucket(index int, m []interface{}) FlexBucket {
	//// // fmt.Printf( cur() + ": New Number bucket with index: %d\n", index)
	flist := make([]func(index int, m []interface{}) FlexBucket, len(m))
	for i, w := range m {
		flist[i] = w.(func(in int, m []interface{}) FlexBucket)
	}
	l := 100
	b := &BucketImpl{
		min : math.MaxInt64,
		max : math.MinInt64,
		bucketCount : make([]int, l),
		bucketLow : make([]float64, l),
		bucketHigh :make([]float64, l),
		numberOfBuckets : l,
		total : 0,
		objects: make([]FlexBucket, l),
		columnIndex:index,
		builderMap: flist,
		originalBuilderMap: m,
	}
	for i, _ := range b.objects {
		//// // fmt.Printf( cur() + ": New bucket for: %d\n", i)
		b.objects[i] = flist[index](index + 1, b.originalBuilderMap)
	}
	return b
}

func makeBuckets(start, length float64, count int) ([]float64, []float64) {
	//// // fmt.Printf( cur() + ": Making %d buckets from %v of length %v\n", count, start, length)
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
	// fmt.Printf(cur() + ": MM: [%v:%v] L:%v, C:%v\n", min, max, newLength, count)
	if newHighs[count - 1] < max {
		newHighs[count - 1] = max + 0.0001
	}
	newCounts := make([]int, count)
	newObjects := make([]FlexBucket, count)
	for i, _ := range newObjects {
		newObjects[i] = newBucketFunc()
	}
	// fmt.Printf(cur() + ": Last bucket: [%v,%v]\n", newLows[count - 1], newHighs[count - 1])
	mergeOldToNew(oCounts, oLows, oHighs, oObjects, newCounts, newLows, newHighs, newObjects)
	// fmt.Printf(cur() + ": Last bucket after merge first: [%v,%v]\n", newLows[count - 1], newHighs[count - 1])
	// fmt.Printf(cur() + ": Last bucket to be merged: [%v,%v]\n", nLows[len(nLows) - 1], nHighs[len(nHighs) - 1])
	// fmt.Printf(cur() + ": Last bucket to be merged %d: [%v,%v]\n", 82, nLows[82], nHighs[82])
	mergeOldToNew(nCounts, nLows, nHighs, nObjects, newCounts, newLows, newHighs, newObjects)
	// fmt.Printf(cur() + ": Merge complete\n")

	return newCounts, newLows, newHighs, newObjects
}
//func cur() string {
//	t := time.Now()
//	return fmt.Sprintf("%v", t.UnixNano())
//}
func (b *BucketImpl) AddBuckets(x FlexBucket) {
	// fmt.Printf(cur() + ": Add Buckets:\n%s\n", x.PrintBuckets(""))
	// fmt.Printf(cur() + ": Current Buckets:\n%s\n", b.PrintBuckets(""))
	newB, ok := x.(*BucketImpl)
	if !ok {
		panic("Cannot add bucket of this type: " + reflect.TypeOf(x).String())
	}
	total := 0
	for i, c := range b.bucketCount {
		if b.bucketCount[i] > 0 {
			// fmt.Printf(cur() + ": [%v <= %d < %v]\n", b.bucketLow[i], b.bucketCount[i], b.bucketHigh[i])
		}
		total = total + c
	}
	anotherTotal := 0
	for _, c := range newB.bucketCount {
		anotherTotal = anotherTotal + c
	}
	if total == 0 {

		b.bucketCount = newB.bucketCount
		b.bucketLow = newB.bucketLow
		b.bucketHigh = newB.bucketHigh
		b.objects = newB.objects
	}else if anotherTotal == 0 {
		// do nothing ?
	} else {
		c, l, h, o := mergeBuckets(
			b.bucketCount, b.bucketLow, b.bucketHigh, b.objects,
			newB.bucketCount, newB.bucketLow, newB.bucketHigh, newB.objects,
			func() FlexBucket {
				return b.builderMap[b.columnIndex](b.columnIndex + 1, b.originalBuilderMap)
			})
		b.bucketCount = c
		b.bucketLow = l
		b.bucketHigh = h
		b.objects = o
	}
}

//func print(c []int, l, h []float64) {
//	for i, v := range c {
//		 fmt.Printf(cur() + ":%d=> %v : %d: %v\tL: %v\n", i, l[i], v, h[i], h[i] - l[i])
//	}
//}

func mergeOldToNew(oCounts []int, oLows, oHighs []float64, oObjects []FlexBucket, nCounts []int, nLows, nHighs []float64, nObjects []FlexBucket) ([]int, []float64, []float64, []FlexBucket) {
	newLength := nHighs[0] - nLows[0]
	newHalfLength := newLength / 2
	totalRecs := len(oCounts)
	count := len(nCounts)
	j := 0
	//print(oCounts, oLows, oHighs)
	for i := 0; i < totalRecs; i++ {
		//print(nCounts, nLows, nHighs)
		if j == count {
			if oCounts[i] != 0 {
				// fmt.Printf(cur() + ": The Highest new Bucket is: %v => %v\n", nLows[count - 1], nHighs[count - 1])
				// fmt.Printf(cur() + ": We wanted to merge %d counts from old %v => %v\n", oCounts[i], oLows[i], oHighs[i])
				// fmt.Printf(cur() + ": I: %d / %d\n", i, totalRecs)
				// fmt.Printf(cur() + ": J: %d / %d\n", j, count)
				fmt.Errorf("What has happened")
				os.Exit(1)
			}
			break;
		}
		if nHighs[j] <= nLows[j] {
			// fmt.Printf(cur() + ": Increase the length of %d high from %v to %v: length is %v\n", i, nHighs[j], nLows[j] + newLength, newLength)
			nHighs[j] = nLows[j] + newLength
		}
		// fmt.Printf(cur() + ": Current I:%v\n", i)
		// fmt.Printf(cur() + ": %d going to new bucket[%d] %v -> %v from[%d] %v -> %v\n", oCounts[i], j, nLows[j], nHighs[j], i, oLows[i], oHighs[i])
		if oLows[i] >= nHighs[j] && !(oLows[i] == nHighs[j] && j + 1 == count) {
			// fmt.Printf(cur() + ": Skip Merge, check next new bucket\n")
			j = j + 1
			i = i - 1
			continue
		}
		if oHighs[i] <= nHighs[j] {
			// fmt.Printf(cur() + ": Simple merge %d to %d\n", i, j)
			nCounts[j] = nCounts[j] + oCounts[i]
			//nObjects[j] = append(nObjects[j], oObjects[i]...)
			nObjects[j].AddBuckets(oObjects[i])
			// fmt.Printf(cur() + ": Children Buckets:\n%s\n", nObjects[j].PrintBuckets(""))
			// todo: merge objects
			continue
		}
		diffLength := oHighs[i] - nHighs[j]
		if (diffLength > newHalfLength && i > 0 && (oHighs[i - 1] - nLows[j] > newHalfLength)) || (i + 1 == totalRecs) {
			// fmt.Printf(cur() + ": Remaining length is more then new half length, shortening current high from %v to %v\n", nHighs[j], oHighs[i - 1])
			if oHighs[i - 1] > nLows[j] {
				// fmt.Printf(cur() + ": Changing nHigh for %d to %v from %v\n", j, oHighs[i - 1], nHighs[j])
				nHighs[j] = oHighs[i - 1]
			} else {
				// fmt.Printf(cur() + ": Changing nHigh for %d to %v from %v\n", j, oHighs[i], nHighs[j])
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
			// fmt.Printf(cur() + ": This belongs to current\n")
			nHighs[j] = oHighs[i]
			if j + 1 < count {
				nLows[j + 1] = nHighs[j]
				if nHighs[j + 1] - nLows[j + 1] < newLength {
					nHighs[j + 1] = nLows[j + 1] + newLength
				}
			}
			nCounts[j] = nCounts[j] + oCounts[i]
			//nObjects[j] = append(nObjects[j], oObjects[i]...)
			nObjects[j].AddBuckets(oObjects[i])
			// todo: merge objects
			continue
		}
		if diffLength <= 0 {
			// fmt.Printf(cur() + ": Time to inc j from %v\n", j)
			j = j + 1
		} else {
			// fmt.Printf(cur() + ": I think everyone missed this at %d: %v => %v\n", i, oLows[i], oHighs[i])
		}
	}
	for ; j < count; j++ {
		if nHighs[j] <= nLows[j] {
			// fmt.Printf(cur() + ": Changing the high of %d to %v\n", j, nLows[j] + newLength)
			nHighs[j] = nLows[j] + newLength
		}
		if j + 1 < count {
			// fmt.Printf(cur() + ": Changing the low of %d to %v\n", j + 1, nHighs[j])
			nLows[j + 1] = nHighs[j]
		}
	}
	return nCounts, nLows, nHighs, nObjects
}

func resetBuckets(counts []int, lows []float64, high []float64, oObjects []FlexBucket, count int, newMin, newMax float64, newF func() FlexBucket) ([]int, []float64, []float64, []FlexBucket) {
	// fmt.Printf(cur() + ": Reset buckets, make %d buckets from %v -> %v\n", count, newMin, newMax)
	newLength := (newMax - newMin) / float64(count)
	// fmt.Printf(cur() + ": New length: %v\n", newLength)
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
	// fmt.Printf(cur() + ": Add all values: %v\n", vals)
	for _, v := range vals {
		b.AddValue(v.(int))
	}
}

func (b *BucketImpl) AddRow(row []interface{}) {
	// fmt.Printf(cur() + ": Add %v to a NumberRange\n", row)
	if len(row) < 1 {
		return
	}
	val, ok := row[0].(float64)
	var err error
	if !ok {
		val, err = strconv.ParseFloat(row[0].(string), 64)
		if err != nil {
			panic(fmt.Sprintf("This is not a number: %v", row[0]))
		}
	}
	bucketNumber := b.AddValue(val)
	// fmt.Printf(cur() + ": Objects: %v\n", bucketNumber)
	b.objects[bucketNumber.(int)].AddRow(row[1:])
}

func (b *BucketImpl) AddValue(vali interface{}) interface{} {
	// fmt.Printf(cur() + ": Add value: %d\n", vali)
	val := vali.(float64)
	// fmt.Printf(cur() + ": Add: [%d] Number of buckets: %d\n", vali, b.numberOfBuckets)
	b.total = b.total + 1
	// fmt.Printf(cur() + ": Final total: %v\n", b.total)
	// fmt.Printf(cur() + ": Counts: %v\n", b.bucketCount)
	if b.total == 1 {
		// fmt.Printf(cur() + ": This is the first value: %v\n", vali)
		b.min = val
		b.max = val
		b.lengthOfBucket = (b.min - b.max) / float64(b.numberOfBuckets)
		l, h := makeBuckets(b.min, b.lengthOfBucket, b.numberOfBuckets)
		b.bucketLow = l
		b.bucketHigh = h
		for c := 0; c < b.numberOfBuckets; c++ {
			// fmt.Printf(cur() + ": New function: %v\n", b.builderMap[b.columnIndex])
			b.objects[c] = b.builderMap[b.columnIndex](b.columnIndex + 1, b.originalBuilderMap)
		}
	} else {
		if val < b.min || val > b.max {
			if val < b.min {
				b.min = val
			}else if val > b.max {
				b.max = val
			}
			c, l, h, o := resetBuckets(b.bucketCount, b.bucketLow, b.bucketHigh, b.objects, b.numberOfBuckets, b.min, b.max, func() FlexBucket {
				return b.builderMap[b.columnIndex](b.columnIndex + 1, b.originalBuilderMap)
			})
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
			// fmt.Printf(cur() + ": %v added to bucket %d\n", val, i)
			b.bucketCount[i] = b.bucketCount[i] + 1
			break
		}
	}
	if (i == b.numberOfBuckets) {
		panic(errors.New("I was not added in any bucket"))
	}
	return i
}


