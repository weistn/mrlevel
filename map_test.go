package mrlevel

import (
	"testing"
	"github.com/jmhodges/levigo"
	"github.com/weistn/sublevel"
	"strings"
)

func TestMRLevel(t *testing.T) {
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

	mapcount := 0
	task := Map(sub1, index, "job", func(key, value []byte, emit EmitFunc) {
		mapcount++
		ingredients := strings.Split(string(value), ",")
		for _, ingredient := range ingredients {
			println("emit", ingredient, string(key))
			emit([]byte(ingredient), key)
		}
	})

	sub1.Put(wo, []byte("Sushi"), []byte("Fish,Rice"))
	sub1.Put(wo, []byte("Forelle Blau"), []byte("Fish,Potatoes"))
	sub1.Put(wo, []byte("Wiener Schnitzel"), []byte("Pig,Potatoes"))
	sub1.Put(wo, []byte("Pulled Pork"), []byte("Pig,ColeSlaw"))

	if mapcount != 4 {
		t.Fatal(mapcount)
	}

	it := task.NewIterator("Fish")
	for it.SeekToFirst(); it.Valid(); it.Next() {
		println("Fish", string(it.Key()), string(it.Value()))
	}

	it2 := db.NewIterator(ro)
	for it2.SeekToFirst(); it2.Valid(); it2.Next() {
		println("DB", len(it2.Key()), string(it2.Key()), string(it2.Value()))
	}

	task.Close()

	ro.Close()
	wo.Close()
	db.Close()
}
