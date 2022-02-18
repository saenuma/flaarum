// tindexer is a program that creates indexes for tables which contains text fields.
package main

import (
	"github.com/saenuma/flaarum/flaarum_shared"
	"fmt"
	"log"
	"time"
	"github.com/radovskyb/watcher"
	"strings"
	"path/filepath"
	"os"
	"sync"
)


func P(err error) {
	if debugMode {
		fmt.Printf("%+v\n", err)
	} else {
		fmt.Println(err.Error())
	}
}


func main() {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	count := 1
	// create indexes in case the tindexer was off and some insertions went on.
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if ! info.IsDir() {
			if strings.HasSuffix(info.Name(), ".text") {
				wg.Add(1)
				go doIndexWG(path, &wg)
				count += 1
			} else if strings.HasSuffix(info.Name(), ".rtext") {
				wg.Add(1)
				go removeIndexWG(path, &wg)
				count += 1
			}
		}

		return nil
	})

	wg.Wait()

	if err != nil {
		P(err)
		return
	}

	fmt.Println("Started...")

	// watch for new files
	w := watcher.New()

	go func() {
		for {
			select {
			case event := <-w.Event:
				if strings.HasSuffix(event.Path, ".text") {
					go doIndex(event.Path)
					if debugMode {
						fmt.Println("indexed: " + event.Path)
					}
				}

				if strings.HasSuffix(event.Path, ".rtext") {
					go removeIndex(event.Path)
					if debugMode {
						fmt.Println("remove index from instruction file: " + event.Path)
					}
				}

				if strings.HasSuffix(event.Path, ".out_instr") {
					go outCommand()
				} else if strings.HasSuffix(event.Path, ".in_instr") {
					go inCommand()
				} else if strings.HasSuffix(event.Path, ".ast_instr") {
					go astCommand()
				}
			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	if err := w.AddRecursive(dataPath); err != nil {
		log.Fatalln(err)
	}

	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}

}
