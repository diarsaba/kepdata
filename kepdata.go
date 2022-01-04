package kepdata

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"

	//"strings"
	"crypto/sha256"
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
)

// Hello returns a greeting for the named person.
func Hello(name string) (string, error) {
	// If no name was given, return an error with a message.
	if name == "" {
		return "", errors.New("empty name")
	}

	// If a name was received, return a value that embeds the name
	// in a greeting message.
	message := fmt.Sprintf("Hi, %v. Welcome!", name)
	return message, nil
}

//keys := strings.Split(key, ",")

type KPD struct {
	name     string
	fragment uint8
	spliter  string
}

func DB(ms string, spliter string, fragment uint8) KPD {
	return KPD{ms, fragment, spliter}
}

func (kpd *KPD) fridke(pre []byte, key []byte) []byte {
	hash := sha256.Sum256(append(pre, key...))
	return hash[:kpd.fragment]
}

func (kpd *KPD) primarykey(pre []byte, key []byte) []byte {

	db, err := leveldb.OpenFile(kpd.name, nil)
	if err != nil {
		log.Println("err failed to load or create database")
	}
	defer db.Close()

	primarykey := kpd.fridke(pre, key)

	collectionID, err := db.Get(primarykey, nil)
	if err != nil {
		log.Println("no existe esta llave")
	}
	data, err := db.Get(collectionID, nil)
	if err != nil {
		log.Println("no existe la colleccion")
	}
	return data
}

func (kpd *KPD) MapCollection(key string, private string, data string) []byte {
	var akey map[string]string
	//var akey map[byte]byte
	if err := json.Unmarshal([]byte(key), &akey); err != nil {
		panic(err)
	}
	var aprivate map[string]string
	if err := json.Unmarshal([]byte(private), &aprivate); err != nil {
		panic(err)
	}
	var adata map[string]string
	if err := json.Unmarshal([]byte(data), &adata); err != nil {
		panic(err)
	}
	return kpd.Createcollection(akey, aprivate, adata)
}

func (kpd *KPD) Createcollection(key map[string]string, private map[string]string, data map[string]string) []byte {
	db, err := leveldb.OpenFile(kpd.name, nil)
	if err != nil {
	}
	defer db.Close()

	var k, v string
	for k, v = range key {
		break
	}
	keyWordID := kpd.fridke([]byte(k), []byte(v))
	collectionID, err := db.Get(keyWordID, nil)

	if err != nil {
		var ak, av string
		for ak, av = range private {
			break
		}
		var k, v string
		for k, v = range data {
			break
		}
		collectionID = kpd.fridke([]byte(k), []byte(ak+av+k+v))

		db.Put(keyWordID, collectionID, nil)
	}

	for k, v := range key {
		tempkeyWordID := kpd.fridke([]byte(k), []byte(v))
		tempcollectionID, err := db.Get(keyWordID, nil)
		if err != nil {
			db.Put(tempkeyWordID, collectionID, nil)
		} else if bytes.Compare(collectionID, tempcollectionID) == 0 {
			log.Println("posible error primary key exist")
		}
	}

	rawcollection, err := db.Get(collectionID, nil)
	if err != nil {
		kpd.indexer(key, collectionID, db)
		kpd.indexer(data, collectionID, db)

		merge := make(map[string]string)

		for k, v := range key {
			merge[k] = v
		}
		for k, v := range private {
			merge[k] = v
		}
		for k, v := range data {
			merge[k] = v
		}
		d, _ := json.Marshal(merge)
		db.Put(collectionID, d, nil)
		return collectionID
	}

	collection := make(map[string]string)
	if err := json.Unmarshal(rawcollection, &collection); err != nil {
		panic(err)
	}

	var rem_v []string
	var new_v []string

	newpair := make(map[string]string)

	for k, v = range data {
		if val, ok := collection[k]; ok {
			if val != v {
				rem_v = append(rem_v, val)
				new_v = append(new_v, v)
			}
		} else {
			newpair[k] = v
		}
	}

	for k, v := range data {
		collection[k] = v
	}
	d, _ := json.Marshal(collection)
	db.Put(collectionID, d, nil)

	kpd.remw(rem_v, collectionID, db) //TODO rem no work
	kpd.addw(new_v, collectionID, db)
	kpd.indexer(newpair, collectionID, db)
	return collectionID
}
func (kpd *KPD) Keycollection(name string) [][]byte {

	db, err := leveldb.OpenFile(kpd.name, nil)
	if err != nil {
	}
	defer db.Close()

	tempkey := kpd.fridke([]byte("key:"), []byte(name))
	collectionword, err := db.Get(tempkey, nil)
	if err != nil {
	}

	step := int(kpd.fragment)

	var collection [][]byte

	for i := 0; i < len(collectionword); i += step {
		temp, err := db.Get(collectionword[i:i+step], nil)
		if err != nil {
		} else {
			collection = append(collection, temp)
		}
	}
	return collection
}
func (kpd *KPD) Wordcollection(name string) [][]byte {

	db, err := leveldb.OpenFile(kpd.name, nil)
	if err != nil {
	}
	defer db.Close()

	tempkey := kpd.fridke([]byte("word:"), []byte(name))
	collectionword, err := db.Get(tempkey, nil)
	if err != nil {
	}

	step := int(kpd.fragment)

	var collection [][]byte

	for i := 0; i < len(collectionword); i += step {
		temp, err := db.Get(collectionword[i:i+step], nil)
		if err != nil {
		} else {
			collection = append(collection, temp)
		}
	}
	return collection
}
func (kpd *KPD) Contains(key string, word string) [][]byte {
	db, err := leveldb.OpenFile(kpd.name, nil)
	if err != nil {
	}
	defer db.Close()

	words := strings.Split(word, " ")

	keyb := kpd.fridke([]byte("key:"), []byte(key))
	collectionkey, err := db.Get(keyb, nil)
	if err != nil {
		return make([][]byte, 0)
	}

	ready := make(map[string]string)
	var collection [][]byte
	step := int(kpd.fragment)
	for i := 0; i < len(collectionkey); i += step {
		for _, w := range words {
			wordb := kpd.fridke([]byte("word:"), []byte(w))
			Wordcollection, err := db.Get(wordb, nil)
			if err != nil {
				return make([][]byte, 0)
			}
			tempk := collectionkey[i : i+step]
			temps := string(tempk)
			if _, ok := ready[temps]; !ok {
				if bytes.Contains(Wordcollection, tempk) {
					ready[temps] = ""
					temp, err := db.Get(tempk, nil)
					if err != nil {
					} else {
						collection = append(collection, temp)
					}
				}
			}
		}
	}
	return collection
}

func changePrimarys() {

}
func removeKey() {

}

func (kpd *KPD) addw(data []string, collectionID []byte, db *leveldb.DB) {

	for _, str := range data {
		splits := strings.Split(str, kpd.spliter)
		for _, s := range splits {
			tempw := kpd.fridke([]byte("word:"), []byte(s))
			collectionval, err := db.Get(tempw, nil)
			if err != nil {
				db.Put(tempw, collectionID, nil)
			} else {
				db.Put(tempw, append(collectionval, collectionID...), nil)
			}
		}
	}
}

func (kpd *KPD) remw(data []string, collectionID []byte, db *leveldb.DB) {
	for _, str := range data {

		splits := strings.Split(str, kpd.spliter)
		for _, s := range splits {
			tempv := kpd.fridke([]byte("word:"), []byte(s))
			collectionval, err := db.Get(tempv, nil)
			if err != nil {
			} else {
				new := bytes.Replace(collectionval, collectionID, make([]byte, 0), 1)
				db.Put(tempv, new, nil)
			}
		}
	}
}

func (kpd *KPD) indexer(data map[string]string, collectionID []byte, db *leveldb.DB) {

	for k, v := range data {
		//index key
		//TODO: split?
		tempk := kpd.fridke([]byte("key:"), []byte(k))
		collectionkey, err := db.Get(tempk, nil)
		if err != nil {
			db.Put(tempk, collectionID, nil)
		} else {
			db.Put(tempk, append(collectionkey, collectionID...), nil)
		}

		splits := strings.Split(v, kpd.spliter)
		for _, s := range splits {
			tempv := kpd.fridke([]byte("word:"), []byte(s))
			collectionval, err := db.Get(tempv, nil)
			if err != nil {
				db.Put(tempv, collectionID, nil)
			} else {
				db.Put(tempv, append(collectionval, collectionID...), nil)
			}
		}
	}
}
