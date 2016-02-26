package flexbuckets

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

func (s *SingleBucket) AddRow(row []interface{}) {
	s.AddValue(row[0])
	s.object.AddRow(row[1:])
}

func (s *SingleBucket) PrintBuckets(tab string) string {
	var bi bytes.Buffer
	bi.WriteString(fmt.Sprintf("%s|-%s: %d\n", tab, "SingleBucket", s.count))
	bi.WriteString(s.object.PrintBuckets(tab + "|   "))
	return bi.String()
}

func (s *SingleBucket) AddBuckets(b FlexBucket) {
	si, ok := b.(*SingleBucket)
	if !ok {
		panic("Cannot add this type of bucket: " + reflect.TypeOf(b).String())
	}
	if si.count < 1 {
		return
	}
	if s.count == 0 {
		s.object = s.builderMap[s.index](s.index + 1, s.originalBuilderMap)
	}
	s.count = s.count + si.count
	s.object.AddBuckets(si.object)
}

type SingleBucket struct {
	count              int
	object             FlexBucket
	index              int
	originalBuilderMap []interface{}
	builderMap         []func(i int, m []interface{}) FlexBucket
}

func (s *SingleBucket) AddAllValues(val ...interface{}) {
	for _, v := range val {
		s.AddValue(v)
	}
}

func (s *SingleBucket) AddValue(val interface{}) interface{} {
	if s.count == 0 {
		s.object = s.builderMap[s.index](s.index + 1, s.originalBuilderMap)
	}
	s.count = s.count + 1
	return 0
}

func NewSingleBucket(index int, m []interface{}) FlexBucket {
	flist := make([]func(index int, m []interface{}) FlexBucket, len(m))
	for i, w := range m {
		flist[i] = w.(func(in int, m []interface{}) FlexBucket)
	}
	return &SingleBucket{
		count: 0,
		index: index,
		builderMap: flist,
		originalBuilderMap:m,
	}
}
func (s *SingleBucket) Buckets() map[string]int {
	return map[string]int{
		"one": s.count,
	}
}

type BucketType int

const (
	NumberBucketType = iota
	NilBucketType
	SingleBucketType
	IdentityBucketType
)


func (bt BucketType) String() string {
	switch bt {
	case NumberBucketType:
		return "NumberBucket"
	case NilBucketType:
		return "NilBucket"
	case SingleBucketType:
		return "SinglBucket"
	case IdentityBucketType:
		return "IdentityBucket"
	}
}

var builderMap = map[BucketType]func(int, []interface{}) FlexBucket{
	NumberBucketType        : NewNumberRangeBucket,
	NilBucketType: NewNilBucket,
	SingleBucketType: NewSingleBucket,
	IdentityBucketType: NewIdentityBucket,
}

func BuildTree(t []BucketType) FlexBucket {

	builders := make([]interface{}, len(t))
	for i := 1; i < len(t); i++ {
		builders[i - 1] = builderMap[t[i]]
	}
	builders[len(t) - 1] = NewNilBucket
	b := builderMap[t[0]]
	fmt.Printf("builders: %v\n", builders)
	return b(0, builders)
}
func (n NilBucket) AddRow(row []interface{}) {
	//fmt.Printf("Add %v to NilBucket\n", row)
}

func (n NilBucket) AddBuckets(b FlexBucket) {

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
	l := len(row)
	if l < 1 {
		return
	}
	b := i.AddValue(row[0])
	//fmt.Printf("Add %v to IdentityBucket[%d] => Added to %v\n", row, i.index, b)
	if l < 2 {
		return
	}
	i.objects[b.(string)].AddRow(row[1:])
}

func (i *IdentityBucket) AddBuckets(b FlexBucket) {
	if b == nil {
		return
	}
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
