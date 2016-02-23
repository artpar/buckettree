package main

import (
	"testing"
	_ "net/http/pprof"
	"net/http"
	"log"
//"math/rand"
	"math/rand"
	"fmt"
	"encoding/json"
)

func TestBucket(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	b := NewBucket(5)
	count := int64(0)
	for i := 1; i <= 10; i++ {
		count = count + 1
		b.AddValue(rand.Intn(10))
	}
	buckets := b.Buckets()
	total := int64(0)
	for _, v := range buckets {
		total = total + v
	}
	by, _ := json.MarshalIndent(buckets, "", "   ")
	fmt.Printf("%v\n", string(by))
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
