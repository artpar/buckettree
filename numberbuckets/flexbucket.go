package numberbuckets

import (
	"reflect"
	"fmt"
	"bytes"
)

type FlexBucket interface {
	AddAllValues(val ...interface{})
	AddValue(val interface{}) interface{}
	Buckets() map[string]int
	AddRow(row []interface{})
	AddBuckets(b FlexBucket)
	PrintBuckets(tab string) string
}

func (n NilBucket) AddRow(row []interface{}) {

}

func (n NilBucket)AddBuckets(b FlexBucket) {

}

func (n NilBucket) PrintBuckets(tab string) string {
	return fmt.Sprintf("%s|-Nil Bucket\n", tab)
}

type NilBucket struct {
	data map[string]int
}

func (n NilBucket) AddAllValues(val ...interface{}) {

}

func (n NilBucket) AddValue(val interface{}) interface{} {
	return 0
}

func (n NilBucket) Buckets() map[string]int {
	return n.data
}

func NewNilBucket() FlexBucket {
	return NilBucket{data: make(map[string]int)}
}

type IdentityBucket struct {
	buckets       map[string]int
	objects       map[string]FlexBucket
	newBucketFunc func() FlexBucket
}

func (i *IdentityBucket) PrintBuckets(tab string) string {
	var b bytes.Buffer
	for key, count := range i.buckets {
		b.WriteString(fmt.Sprintf("%s|-%s: %d\n", tab, key, count))
		b.WriteString(i.objects[key].PrintBuckets(tab + "|   "))
	}

	return b.String()
}

func NewIdentityBucket(newBuckFunction func() FlexBucket) FlexBucket {
	return &IdentityBucket{
		buckets:make(map[string]int),
		objects:make(map[string]FlexBucket),
		newBucketFunc: newBuckFunction,
	}
}

func (i *IdentityBucket) AddAllValues(vals ...interface{}) {
	for _, v := range vals {
		i.AddValue(v)
	}
}

func (i *IdentityBucket) AddValue(val interface{}) interface{} {
	//fmt.Printf("Add [%s] to the Bucket\n", val)
	str := val.(string)
	_, ok := i.buckets[str]
	if ok {
		i.buckets[str] = i.buckets[str] + 1
	}else {
		i.buckets[str] = 1
		i.objects[str] = i.newBucketFunc()
	}

	return str
}

func (i *IdentityBucket) Buckets() map[string]int {
	return i.buckets
}

func (i *IdentityBucket) AddRow(row []interface{}) {
	if len(row) < 1 {
		return
	}
	b := i.AddValue(row[0])
	i.objects[b.(string)].AddRow(row[1:])
}

func (i *IdentityBucket) AddBuckets(b FlexBucket) {
	strBucket, ok := b.(*IdentityBucket)
	if !ok {
		panic("Cannot merge non identity bucket to identity bucket: " + reflect.TypeOf(b).String())
	}
	for name, count := range strBucket.buckets {
		v, ok := i.buckets[name]
		if ok {
			//fmt.Printf("%s is already contained, increasing count by %d", name, count)
			i.buckets[name] = v + count
		} else {
			i.objects[name] = i.newBucketFunc()
			i.buckets[name] = count
		}
	}
}
