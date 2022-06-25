// tindexer is a program that creates indexes for tables which contains text fields.
package main

import (
	"github.com/saenuma/flaarum/flaarum_shared"
	"fmt"
	"log"
	"time"
	"github.com/radovskyb/watcher"
	"strings"
)


func P(err error) {
	fmt.Println(err.Error())
}


func main() {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		panic(err)
	}

	fmt.Println("Started...")

	// watch for new files
	w := watcher.New()

	go func() {
		for {
			select {
			case event := <-w.Event:
				if strings.HasSuffix(event.Path, ".out_instr") {
					go outCommand()
				} else if strings.HasSuffix(event.Path, ".in_instr") {
					go inCommand()
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
