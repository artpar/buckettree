package flexbuckets

import (
	"reflect"
	"fmt"
	"bytes"
	"github.com/artpar/gisio/types"
)

type FlexBucket interface {
	AddAllValues(val ...interface{})
	AddValue(val interface{}) interface{}
	Buckets() map[string]int
	AddRow(row []interface{})
	AddBuckets(b FlexBucket)
	PrintBuckets(tab string) string
}

func BuildTree(t []types.EntityType) FlexBucket {

	builders := make([]interface{}, len(t))
	for i := 1; i < len(t); i++ {
		typ := t[i]
		if typ == types.Number {
			//fmt.Printf("%d children is a number range\n", i - 1)
			builders[i - 1] = NewNumberRangeBucket
		}else {
			//fmt.Printf("%d children is a identity range\n", i - 1)
			builders[i - 1] = NewIdentityBucket
		}
	}
	builders[len(t) - 1] = NewNilBucket
	typ := t[0]
	var b func(i int, m[]interface{}) FlexBucket
	if typ == types.Number {
		b = NewNumberRangeBucket
	}else {
		b = NewIdentityBucket
	}
	//fmt.Printf("builders: %v\n", builders)
	return b(0, builders)
}
func (n NilBucket) AddRow(row []interface{}) {
	//fmt.Printf("Add %v to NilBucket\n", row)

}

func (n NilBucket)AddBuckets(b FlexBucket) {

}

func (n NilBucket) PrintBuckets(tab string) string {
	return fmt.Sprintf("%s|-Nil Bucket\n", tab)
}

func (n NilBucket) SetConstructor(f func() FlexBucket) {

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

var e map[string]int

func NewNilBucket(ii int, m []interface{}) FlexBucket {
	//fmt.Printf("New Nil Bucket\n")
	return NilBucket{data: e}
}

type IdentityBucket struct {
	buckets            map[string]int
	objects            map[string]FlexBucket
	index              int
	builderMap         []func(i int, m []interface{}) FlexBucket
	originalBuilderMap []interface{}
}

func (i *IdentityBucket) PrintBuckets(tab string) string {
	var b bytes.Buffer
	for key, count := range i.buckets {
		b.WriteString(fmt.Sprintf("%s|-%s: %d\n", tab, key, count))
		b.WriteString(i.objects[key].PrintBuckets(tab + "|   "))
	}

	return b.String()
}

func NewIdentityBucket(index int, m []interface{}) FlexBucket {
	//fmt.Printf("New Identity bucket with index: %d\n", index)
	flist := make([]func(i int, m1 []interface{}) FlexBucket, len(m))
	for i, w := range m {
		y, ok := w.(func(i int, m1 []interface{}) FlexBucket)
		if !ok {
			panic("w is not that type of function: " + reflect.TypeOf(w).String())
		}
		flist[i] = y
	}
	return &IdentityBucket{
		buckets:make(map[string]int),
		objects:make(map[string]FlexBucket),
		builderMap: flist,
		originalBuilderMap: m,
		index: index,
	}
}

func (i *IdentityBucket) AddAllValues(vals ...interface{}) {
	for _, v := range vals {
		i.AddValue(v)
	}
}

func (i *IdentityBucket) AddValue(val interface{}) interface{} {
	//fmt.Printf("Add [%s] to the Bucket[%d]\n", val, i.index)
	str := val.(string)
	_, ok := i.buckets[str]
	if ok {
		i.buckets[str] = i.buckets[str] + 1
	}else {
		i.buckets[str] = 1
		//fmt.Printf("Me: %d\nMap: %v\n", i.index, i.builderMap)
		i.objects[str] = i.builderMap[i.index](i.index + 1, i.originalBuilderMap)
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
	//fmt.Printf("Add %v to IdentityBucket[%d] => Added to %v\n", row, i.index, b)
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
			//fmt.Printf("%s is already contained, increasing count by %d\n", name, count)
			i.buckets[name] = v + count
			i.objects[name].AddBuckets(strBucket.objects[name])
		} else {
			//fmt.Printf("%s is new, adding it with count: %d\n", name, count)
			i.objects[name] = i.builderMap[i.index](i.index + 1, i.originalBuilderMap)
			i.objects[name].AddBuckets(strBucket.objects[name])
			i.buckets[name] = count
		}
	}
}
