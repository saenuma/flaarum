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
  "log"
  "strings"
  "path/filepath"
  "github.com/bankole7782/flaarum/flaarum_shared"
)

func astCommand() {
  log.Println("Beginning autoscaling test ")

  dataPath, _ := flaarum_shared.GetDataPath()
  astCommandInstrPath := filepath.Join(dataPath, "ast.ast_instr")
  rawCommandOpts, err := os.ReadFile(astCommandInstrPath)
  if err != nil {
    log.Println(err)
    return
  }

  partsOfFile := strings.Split(strings.TrimSpace(string(rawCommandOpts)), "\n")
  addr := partsOfFile[0]
  fks := partsOfFile[1]
  noOfThreadsStr := partsOfFile[2]

  noOfThreads, err := strconv.Atoi(noOfThreadsStr)
  if err != nil {
    color.Red.Println(err.Error())
    return
  }

  cl := flaarum.NewClient(addr, fks, "first_proj")
	err = cl.Ping()
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

func untestedRandomString(length int) string {
  var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
  const charset = "abcdefghijklmnopqrstuvwxyz1234567890"

  b := make([]byte, length)
  for i := range b {
    b[i] = charset[seededRand.Intn(len(charset))]
  }
  return string(b)
}
