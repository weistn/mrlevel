package mrlevel

import (
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"github.com/weistn/runlevel"
	"github.com/weistn/uniclock"
	"encoding/binary"
	"bytes"
)

type EmitFunc func(key interface{}, value interface{})
type MappingFunc func(key, value []byte, emit EmitFunc)
type ReduceFunc func(acc interface{}, value []byte)
type RereduceFunc func(acc interface{}, value interface{})
type ValueFactory func() interface{}

type MappingTask struct {
	task *runlevel.Task
	mapFunc MappingFunc
	source *sublevel.DB
	target *sublevel.DB
	taskDb *sublevel.DB
	ro *levigo.ReadOptions
}

type ReduceTask struct {
	task *runlevel.Task
	reduceFunc ReduceFunc
	rereduceFunc RereduceFunc
	valueFactory ValueFactory
}

type mapIterator struct {
	it *levigo.Iterator
	prefix []byte
	valid bool
}


func Map(source *sublevel.DB, target *sublevel.DB, name string, mapFunc MappingFunc) *MappingTask {
	task := &MappingTask{source: source, target: target, taskDb: sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,65})), mapFunc: mapFunc}
	task.ro = levigo.NewReadOptions()

	filter := func(key, value []byte) []byte {
		return key
	}

	f := func(key, value []byte, hook *sublevel.Hook) bool {
		mapValues := new(bytes.Buffer)
		if value != nil {
			emit := func(key interface{}, value interface{}) {
				k := serializeKey(key, uniclock.Next())
				v := serializeValue(value)
				hook.Put(k, v, task.target)
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
				hook.Delete(k, target)
			}
		}
		hook.Put(key, mapValues.Bytes(), task.taskDb)
		return true
	}

	task.task = runlevel.TriggerBefore(source, sublevel.Sublevel(target.LevelDB(), name + string([]byte{0,66})), filter, f)
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
	return &mapIterator{it: this.source.LevelDB().NewIterator(this.ro), prefix: append(append([]byte(this.target.Prefix()), 0), prefix[:len(prefix)-8]...), valid: false}
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
	case string:
		return []byte(v)
	case []byte:
		return v
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, value)
	return buf.Bytes()
}

func serializeKey(keys interface{}, clock int64) []byte {
	buf := new(bytes.Buffer)
	if keys == nil {
		buf.WriteByte(0)
		binary.Write(buf, binary.BigEndian, clock)		
		return buf.Bytes()
	}
	switch v := keys.(type) {
	case []byte:
		buf.Write(v)
		buf.WriteByte(0)		
	case []string:
		for _, s := range v {
			buf.WriteString(s)
			buf.WriteByte(0)
		}
	case []int8:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []int16:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []int32:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []int64:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []uint16:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []uint32:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case []uint64:
		for _, s := range v {
			binary.Write(buf, binary.BigEndian, s)
			buf.WriteByte(0)
		}
	case string:
		buf.WriteString(v)
	default:
		binary.Write(buf, binary.BigEndian, v)
	}
	buf.WriteByte(0)
	binary.Write(buf, binary.BigEndian, clock)
	return buf.Bytes()
}
