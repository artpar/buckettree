package flexbuckets

import (
	"testing"
	_ "net/http/pprof"
	"fmt"
	"math/rand"
	"github.com/artpar/gisio/types"
)


func TestConstruction(t *testing.T) {
	ty := []types.EntityType{types.None, types.Number, types.None}
	b := BuildTree(ty)
	mf := []string{"M", "F"}
	ab := []string{"A", "B"}
	for i := 1; i <= 100; i++ {
		b.AddRow([]interface{}{mf[rand.Intn(len(mf))], i, ab[rand.Intn(len(ab))]})
	}
	fmt.Printf(b.PrintBuckets(""))
}

//func TestBucket(t *testing.T) {
//	go func() {
//		log.Println(http.ListenAndServe("localhost:6060", nil))
//	}()
//	b := NewNumberRangeBucket()
//
//	mf := []string{"M", "F"}
//	count := int64(0)
//	for i := 1; i <= 100000; i++ {
//		count = count + 1
//		b.AddRow([]interface{}{i, mf[rand.Intn(len(mf))]})
//	}
//	fmt.Printf("Final Print\n\n")
//	s := b.PrintBuckets("")
//	fmt.Println(s)
//}

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
