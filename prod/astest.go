package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/saenuma/flaarum"
)

func astCommand(addr, fks string, noOfThreads int) {

	cl := flaarum.NewClient(addr, fks, "first_proj")
	err := cl.Ping()
	if err != nil {
		log.Println(err)
		return
	}

	var wg sync.WaitGroup
	err = cl.CreateTable(`
    table: vals
    fields:
      f1 string
      f2 string
      f3 string
    ::
  `)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i != noOfThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				_, err = cl.InsertRowStr("vals", map[string]string{
					"f1": untestedRandomString(10), "f2": untestedRandomString(30), "f3": untestedRandomString(5),
				})
				if err != nil {
					fmt.Println(err.Error())
				}
			}

		}()

	}

	wg.Wait()

}

func untestedRandomString(length int) string {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	const charset = "abcdefghijklmnopqrstuvwxyz1234567890"

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
