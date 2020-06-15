package main

import (
	"bolt"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"strconv"
	"time"
)

var (
	bucket = []byte("users")
)

type User struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var (
	db *bolt.DB
)

func main() {
	var err error
	db, err = bolt.Open("./demo.db", 0666, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		pp.Fatal(err)
	}
	defer db.Close()

	// 创建 bucket
	if err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket(bucket)
		if err != nil {
			return fmt.Errorf("create bucket failed: %+v", err)
		}
		_ = b
		return nil
	}); err != nil {
		pp.Fatal(err)
	}

	// 读写事务
	if err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Put([]byte("user_1"), []byte("wuYin"))
	}); err != nil {
		pp.Fatal(err)
	}

	// 只读事务
	if err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		raw := b.Get([]byte("user_1"))
		pp.Println("Get(user_1) ->", string(raw))
		return nil
	}); err != nil {
		pp.Fatal(err)
	}

	// 自增 key
	for i := 0; i < 3; i++ {
		u := &User{Name: "user_" + strconv.Itoa(i), Age: i * 10}
		if err := createUser(u); err != nil {
			pp.Fatal(err)
		}
	}

	// 迭代 key
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("%x -> %q\n", k, v)
		}
		return nil
	})

	// 前缀扫描
	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		prefix := []byte("user_")
		k, v := c.Seek(prefix)
		fmt.Printf("%x --> %q\n", k, v)
		return nil
	})

	// 范围扫描
	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		min := make([]byte, 8)
		max := make([]byte, 8)
		binary.BigEndian.PutUint64(min, 2)
		binary.BigEndian.PutUint64(max, 3)
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			fmt.Printf("%x ---> %q\n", k, v)
		}
		return nil
	})

	// foreach
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		b.ForEach(func(k, v []byte) error {
			fmt.Printf("%x -> %q\n", k, v)
			return nil
		})
		return nil
	})

	// 嵌套 bucket
	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(strconv.FormatUint(10, 10)))
		return err
	}); err != nil {
		pp.Fatal(err)
	}

	if err = func() error {
		tx, err := db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		root := tx.Bucket([]byte(strconv.FormatUint(10, 10)))   // 10th root bucket
		childBucket, err := root.CreateBucket([]byte("admins")) // child bucket
		if err != nil {
			return err
		}

		uid, err := childBucket.NextSequence()
		if err != nil {
			return err
		}
		u := &User{Id: int(uid)}
		buf, _ := json.Marshal(u)
		if err = childBucket.Put([]byte(strconv.FormatUint(uid, 10)), buf); err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		pp.Fatal(err)
	}
}

func createUser(u *User) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		u.Id = int(id)
		buf, err := json.Marshal(u)
		if err != nil {
			return err
		}
		return b.Put(i2b(u.Id), buf)
	})
}

func i2b(v int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}
