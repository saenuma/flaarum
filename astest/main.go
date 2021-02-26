package main

import (
	"github.com/bankole7782/flaarum"
	"os"
	"time"
	"math/rand"
	"fmt"
	"sync"
  "github.com/gookit/color"
  "strconv"
)


func main() {

  if len(os.Args) < 2 {
		color.Red.Println("Wrong number of inputs. Run this program with --help to view the help message.")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "help", "h", "--help":
		fmt.Println(`flaarum's astest program is used to test autoscaling deployments. The method of test
used here is the generation of random inserts.

The name of the table created is called 'vals'. Please delete the table after tests.

To begin testing autoscaling deployments set the resize_frequency to 1 hour and then run this program.
This program should be ran from the same local network but from a different machine. Keep checking the
CPU usage of the flaarum server to see if it passes 70%. Autoscaling upwards starts at 70%.

Supported Commands:

    t    t starts the test. It expects three inputs: the address, the keystring and the number of threads.
         The number of threads should start from twenty.

         Example: flaarum.astest t 127.0.0.1 not-yet-set 50

			`)

  case "t":
    if len(os.Args) < 5 {
      color.Red.Println("Expecting three inputs: the address, the keystring and the number of threads. The number of threads should start from twenty.")
      return
    }

    addr := os.Args[2]
    fks := os.Args[3]
    noOfThreads, err := strconv.Atoi(os.Args[4])
    if err != nil {
      color.Red.Println(err.Error())
      return
    }

    cl := flaarum.NewClient(addr, fks, "first_proj")
  	err = cl.Ping()
  	if err != nil {
  		panic(err)
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
      fmt.Println(err)
    }

  	for i := 0; i != noOfThreads; i++ {
  		wg.Add(1)

  		go func() {
  			defer wg.Done()

  			for {
  				_, err = cl.InsertRowStr("vals", map[string]string {
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
