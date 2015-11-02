package mrlevel

import (
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"github.com/weistn/runlevel"
	"github.com/weistn/uniclock"
	"encoding/binary"
	"encoding/json"
//	"encoding/ascii85"
	"bytes"
	"fmt"
)

type EmitFunc func(key interface{}, value interface{})
type MappingFunc func(key, value []byte, emit EmitFunc)
type ReduceFunc func(acc interface{}, value []byte) interface{}
type RereduceFunc func(acc interface{}, value []byte) interface{}
type ValueFactory func() interface{}

type MappingTask struct {
	task *runlevel.Task
	mapFunc MappingFunc
	source *sublevel.DB
	target *sublevel.DB
	taskDb *sublevel.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

type ReduceTask struct {
	task *runlevel.Task
	source *sublevel.DB
	target *sublevel.DB
	taskDb *sublevel.DB
	reduceFunc ReduceFunc
	rereduceFunc RereduceFunc
	valueFactory ValueFactory
	level int
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

type mapIterator struct {
	it *levigo.Iterator
	prefix []byte
	valid bool
}

/**************************************************************
 *
 * MappingTask
 *
 **************************************************************/


func Map(source *sublevel.DB, target *sublevel.DB, name string, mapFunc MappingFunc) *MappingTask {
	task := &MappingTask{source: source, target: target, taskDb: sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,65})), mapFunc: mapFunc}
	task.ro = levigo.NewReadOptions()
	task.wo = levigo.NewWriteOptions()

	filter := func(key, value []byte) []byte {
		return key
	}

	f := func(key, value []byte) bool {
		mapValues := new(bytes.Buffer)
		if value != nil {
			emit := func(key interface{}, value interface{}) {
				k := serializeKey(key, uniclock.Next())
				v := serializeValue(value)
				task.target.Put(task.wo, k, v)
				binary.Write(mapValues, binary.BigEndian, int32(len(k)))
				mapValues.Write(k)
			}
			mapFunc(key, value, emit)
		}
		val, err := task.taskDb.Get(task.ro, key)
		if err != nil {
			return false
		}
		if val != nil {
			valbuf := bytes.NewBuffer(val)
			var l int32
			off := 0
			for off < len(val) {
				err := binary.Read(valbuf, binary.BigEndian, &l)
				if err != nil {
					break
				}
				off += 4
				if l < 0 || int(l) > len(val) {
					panic("Something is very wrong with this data")
				}
				k := valbuf.Next(int(l))
				off += int(l)
				task.target.Delete(task.wo, k)
			}
		}
		task.taskDb.Put(task.wo, key, mapValues.Bytes())
		return true
	}

	task.task = runlevel.Trigger(source, sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,66})), filter, f)
	return task
}

func (this *MappingTask) Close() {
	this.ro.Close()
	this.task.Close()
}

func (this *MappingTask) WorkOff() {
	this.task.WorkOff()
}

func (this *MappingTask) NewIterator(key interface{}) sublevel.Iterator {
	prefix := serializeKey(key, 1)
	return &mapIterator{it: this.source.LevelDB().NewIterator(this.ro), prefix: append(append([]byte(this.target.Prefix()), 0), prefix[:len(prefix)-16]...), valid: false}
}

/**************************************************************
 *
 * ReduceTask
 *
 **************************************************************/

func Reduce(source *sublevel.DB, target *sublevel.DB, name string, reduceFunc ReduceFunc, rereduceFunc RereduceFunc, valueFactory ValueFactory, level int) *ReduceTask {
	task := &ReduceTask{source: source, target: target, taskDb: sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,65})), reduceFunc: reduceFunc, rereduceFunc: rereduceFunc, valueFactory: valueFactory, level: level}
	task.ro = levigo.NewReadOptions()
	task.wo = levigo.NewWriteOptions()

	filter := func(key, value []byte) []byte {
		return []byte{32}
		/*
		if task.level == 0 {
			return []byte{0}
		}
		s := bytes.Split(key[:len(key)-17], []byte{32})
		if len(s) < task.level {
			return nil
		}
		return bytes.Join(s[:task.level], []byte{32})
		*/
	}

	f := func(key, value []byte) bool {
//		println("Working on", string(key), string(value))
		s := bytes.Split(key[4:len(key)-17], []byte{32})
		off := 16
		for i := len(s); i >= task.level; i-- {
			val := task.valueFactory()
			if i > 0 {
				k := append(joinReduceKey(s[:i], false), 32)
				// Iterate over all similar rows in the source DB
				it := task.source.NewIterator(task.ro)
				for it.Seek(k); it.Valid(); it.Next() {
					if !bytes.HasPrefix(it.Key(), k) {
						break
					}
					val = task.reduceFunc(val, it.Value())
				}
				it.Close()
			}
			// Iterate over all rows in the target DB which are more specific 
			it := task.target.NewIterator(task.ro)
			k := joinReduceKey(s[:i], true)
			for it.Seek(k); it.Valid(); it.Next() {
				if !bytes.HasPrefix(it.Key(), k) {
					break
				}
				val = task.rereduceFunc(val, it.Value())
			}
			it.Close()
			task.target.Put(task.wo, joinReduceKey(s[:i], false), serializeValue(val))
			if i > 0 {
				off += len(s[i - 1]) + 1
			}
		}
		return true	
	}

	task.task = runlevel.Trigger(source, sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,66})), filter, f)
	return task
}

func (this *ReduceTask) Get(key interface{}) ([]byte, error) {
	prefix := serializeKey(key, 1)
	return this.target.Get(this.ro, prefix[:len(prefix)-9])
}

func (this *ReduceTask) Close() {
	this.ro.Close()
	this.task.Close()
}

func (this *ReduceTask) WorkOff() {
	this.task.WorkOff()
}

/**************************************************************
 *
 * mapIterator
 *
 **************************************************************/

func (this *mapIterator) Close() {
	this.it.Close()
}

func (this *mapIterator) GetError() error {
	return this.it.GetError()
}

func (this *mapIterator) Key() []byte {
	if !this.valid {
		return nil
	}
	k := this.it.Key()
	// Strip the prefix and strip the clock
	return k[len(this.prefix):len(k) - 9]
}

func (this *mapIterator) Next() {
	if !this.valid {
		return
	}
	this.it.Next()
	this.checkValid()
}

func (this *mapIterator) Prev() {
	if !this.valid {
		return
	}
	this.it.Prev()
	this.checkValid()
}

func (this *mapIterator) Seek(key []byte) {
	newkey := append(this.prefix, key...)
	this.it.Seek(newkey)
	this.checkValid()
}

func (this *mapIterator) SeekToFirst() {
	this.it.Seek(this.prefix)
	this.checkValid()
}

func (this *mapIterator) SeekToLast() {
	lastkey := make([]byte, len(this.prefix))
	copy(lastkey, this.prefix)
	lastkey[len(this.prefix)-1] = 1
	this.it.Seek(lastkey)
	// If the last row of this sublevel is the last row of the DB, then the previous seek
	// results in an invalid position
	if !this.it.Valid() {
		this.it.SeekToLast()
	} else {
		this.it.Prev()
	}
	this.checkValid()
}

func (this *mapIterator) Valid() bool {
	return this.valid && this.it.Valid()
}

func (this *mapIterator) Value() []byte {
	return this.it.Value()
}

func (this *mapIterator) checkValid() {
	if !this.it.Valid() {
		this.valid = false
		return
	}
	key := this.it.Key()
	if len(key) < len(this.prefix) {
		this.valid = false
		return
	}
	for i, v := range this.prefix {
		if v != key[i] {
			this.valid = false
			return
		}
	}	
	this.valid = true
}

/**************************************************************
 *
 * Helper functions
 *
 **************************************************************/


func serializeValue(value interface{}) []byte {
	if value == nil {
		return []byte{}
	}
	switch v := value.(type) {
	case []byte:
		return v
	}
	b, _ := json.Marshal(value)
	return b
}

func ascii85Enc(src []byte) []byte {
	result := make([]byte, (len(src) + 3) / 4 * 5)

	dst := result
	n := 0
	for len(src) > 0 {
		dst[0] = 0
		dst[1] = 0
		dst[2] = 0
		dst[3] = 0
		dst[4] = 0

		var v uint32
		switch len(src) {
		default:
			v |= uint32(src[3])
			fallthrough
		case 3:
			v |= uint32(src[2]) << 8
			fallthrough
		case 2:
			v |= uint32(src[1]) << 16
			fallthrough
		case 1:
			v |= uint32(src[0]) << 24
		}

		for i := 4; i >= 0; i-- {
			dst[i] = '!' + byte(v%85)
			v /= 85
		}

		m := 5
		if len(src) < 4 {
			m -= 4 - len(src)
			src = nil
		} else {
			src = src[4:]
		}
		dst = dst[m:]
		n += m
	}

	return result[:n]
}

func serializeKey(keys interface{}, clock int64) []byte {
	buf := new(bytes.Buffer)
	if keys == nil {
		fmt.Fprintf(buf, "%04x", 0)
		buf.WriteByte(32)
		binary.Write(buf, binary.BigEndian, clock)		
		return buf.Bytes()
	}
	switch v := keys.(type) {
	case []byte:
		fmt.Fprintf(buf, "%04x", 1)
		buf.Write(ascii85Enc(v))
	case string:
		fmt.Fprintf(buf, "%04x", 1)
		buf.Write(ascii85Enc([]byte(v)))
	case [][]byte:
		fmt.Fprintf(buf, "%04x", len(v))
		for i, s := range v {
			buf.Write(ascii85Enc(s))
			if i + 1 < len(v) {
				buf.WriteByte(32)
			}
		}		
	case []string:
		fmt.Fprintf(buf, "%04x", len(v))
		for i, s := range v {
			buf.Write(ascii85Enc([]byte(s)))
			if i + 1 < len(v) {
				buf.WriteByte(32)
			}
		}
	default:
		panic("Unsupported key type")
	}
	buf.WriteByte(32)
	fmt.Fprintf(buf, "%016x", clock)
	return buf.Bytes()
}


func serializeReduceKey(keys interface{}) []byte {
	buf := new(bytes.Buffer)
	if keys == nil {
		fmt.Fprintf(buf, "%04x", 0)
		return buf.Bytes()
	}
	switch v := keys.(type) {
	case []byte:
		fmt.Fprintf(buf, "%04x", 1)
		buf.Write(ascii85Enc(v))
	case string:
		fmt.Fprintf(buf, "%04x", 1)
		buf.Write(ascii85Enc([]byte(v)))
	case [][]byte:
		fmt.Fprintf(buf, "%04x", len(v))
		for i, s := range v {
			buf.Write(ascii85Enc(s))
			if i + 1 < len(v) {
				buf.WriteByte(32)
			}
		}		
	case []string:
		fmt.Fprintf(buf, "%04x", len(v))
		for i, s := range v {
			buf.Write(ascii85Enc([]byte(s)))
			if i + 1 < len(v) {
				buf.WriteByte(32)
			}
		}
	default:
		panic("Unsupported key type")
	}
	return buf.Bytes()
}

func joinReduceKey(encKeys [][]byte, next bool) []byte {
	buf := new(bytes.Buffer)
	if next {
		fmt.Fprintf(buf, "%04x", len(encKeys) + 1)
	} else {
		fmt.Fprintf(buf, "%04x", len(encKeys))			
	}
	for i, s := range encKeys {
		buf.Write(s)
		if i + 1 < len(encKeys) || next {
			buf.WriteByte(32)
		}
	}		
	return buf.Bytes()
}
