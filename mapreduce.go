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
				k := val[off:off+int(l)]
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


func serializeValue(value interface{}) []byte {
	if value == nil {
		return []byte{}
	}
	switch v := value.(type) {
	case string:
		return []byte(v)
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
	case []uint8:
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
	default:
		binary.Write(buf, binary.BigEndian, v)
	}
	binary.Write(buf, binary.BigEndian, clock)		
	return buf.Bytes()
}
