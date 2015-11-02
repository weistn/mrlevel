package mrlevel

import (
	"testing"
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"time"
	"encoding/json"
)

func TestReduce(t *testing.T) {
	opts := levigo.NewOptions()
	levigo.DestroyDatabase("test.ldb", opts)
	// opts.SetCache(levigo.NewLRUCache(3<<30))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("test.ldb", opts)
	if err != nil {
		t.Fatal(err)
	}

	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()

	sub1 := sublevel.Sublevel(db, "input")
	index := sublevel.Sublevel(db, "index")
	reduce := sublevel.Sublevel(db, "reduce")

	mapcount := 0
	task := Map(sub1, index, "mapjob", func(key, value []byte, emit EmitFunc) {
		mapcount++
		doc := make(map[string]string)
		err := json.Unmarshal(value, &doc)
		if err != nil {
			t.Fatal(err)
		}
		emit([]string{doc["Country"], doc["State"], doc["City"]}, doc["Kind"])
	})

	reducecount := 0
	rereducecount := 0
	task2 := Reduce(index, reduce, "mapjob2", func(acc interface{}, value []byte) interface{} {
		reducecount++
		var kind string
		err := json.Unmarshal(value, &kind)
		if err != nil {
			t.Fatal(err)
		}
		val := acc.(map[string]int)
		if n, ok := val[kind]; ok {
			val[kind] = n + 1
		} else {
			val[kind] = 1
		}
		return val
	}, func(acc interface{}, value []byte) interface{} {
		rereducecount++
		var acc2 map[string]int
		err := json.Unmarshal(value, &acc2)
		if err != nil {
			t.Fatal(err)
		}
		val := acc.(map[string]int)
		for k, v := range acc2 {
			if n, ok := val[k]; ok {
				val[k] = n + v
			} else {
				val[k] = v
			}			
		}
		return val
	}, func() interface{} {
		return make(map[string]int)	
	}, 0)

	sub1.Put(wo, []byte("Bella Vista"), []byte("{\"Country\":\"Germany\",\"State\":\"NRW\",\"City\":\"Duisburg\",\"Kind\":\"Pizza\"}"))
	sub1.Put(wo, []byte("Tokio"), []byte("{\"Country\":\"Germany\",\"State\":\"NRW\",\"City\":\"Düsseldorf\",\"Kind\":\"Sushi\"}"))
	sub1.Put(wo, []byte("Maria"), []byte("{\"Country\":\"Germany\",\"State\":\"NRW\",\"City\":\"Duisburg\",\"Kind\":\"Pizza\"}"))
	sub1.Put(wo, []byte("Formagio"), []byte("{\"Country\":\"Germany\",\"State\":\"NRW\",\"City\":\"Essen\",\"Kind\":\"Pizza\"}"))
	sub1.Put(wo, []byte("Fungi"), []byte("{\"Country\":\"Germany\",\"State\":\"Hessen\",\"City\":\"Frankfurt\",\"Kind\":\"Pizza\"}"))
	sub1.Put(wo, []byte("New Tokio"), []byte("{\"Country\":\"Germany\",\"State\":\"Bayern\",\"City\":\"Munich\",\"Kind\":\"Sushi\"}"))
	sub1.Put(wo, []byte("San Torino"), []byte("{\"Country\":\"USA\",\"State\":\"CA\",\"City\":\"San Jose\",\"Kind\":\"Pizza\"}"))
	sub1.Delete(wo, []byte("Formagio"))

	// Wait until map reduce has completed
	time.Sleep(1000 * time.Millisecond)


	it2 := db.NewIterator(ro)
	for it2.SeekToFirst(); it2.Valid(); it2.Next() {
		println("DB", len(it2.Key()), string(it2.Key()), string(it2.Value()))
	}


	task.Close()
	task2.Close()

	ro.Close()
	wo.Close()
	db.Close()
}
