package numberbuckets

import (
	"testing"
	_ "net/http/pprof"
	"net/http"
	"log"
	"fmt"
	"math/rand"
)

func TestBucket(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	b := NewNumberRangeBucket(100, func() FlexBucket {
		return NewIdentityBucket(NewNilBucket)
	})
	mf := []string{"M", "F"}
	count := int64(0)
	for i := 1; i <= 100000; i++ {
		count = count + 1
		b.AddRow([]interface{}{i, mf[rand.Intn(len(mf))]})
	}
	fmt.Printf("Final Print\n\n")
	s := b.PrintBuckets("")
	fmt.Println(s)
}

func TestMakeHighLows(t *testing.T) {
	newMin := float64(1)
	newLength := float64(1.2)
	count := 5
	newLows, newHighs := makeBuckets(newMin, newLength, count)
	for i := 0; i < count; i++ {
		fmt.Printf("%v\t", newLows[i])
	}
	fmt.Printf("\n")
	for i := 0; i < count; i++ {
		fmt.Printf("%v\t", newHighs[i])
	}
	fmt.Printf("\n")
}
