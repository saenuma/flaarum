package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/radovskyb/watcher"
	"github.com/saenuma/flaarum/flaarum_shared"
)

func P(err error) {
	// if debugMode {
	// 	fmt.Printf("%+v\n", err)
	// } else {
	// 	fmt.Println(err.Error())
	// }
	fmt.Println(err.Error())
}

func main() {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		panic(err)
	}

	if err != nil {
		P(err)
		return
	}

	fmt.Println("Started...")

	watchPath := filepath.Join(dataPath, "flaarum_instrs")
	// watch for new files
	w := watcher.New()

	go func() {
		for {
			select {
			case event := <-w.Event:
				if strings.HasSuffix(event.Path, ".json") {
					// go doIndex(event.Path)
					// if debugMode {
					// 	fmt.Println("indexed: " + event.Path)
					// }
					var instrData map[string]string
					rawJson, _ := os.ReadFile(event.Path)
					json.Unmarshal(rawJson, &instrData)
					if instrData["cmd"] == "reindex" {
						go reindex(instrData["project"], instrData["table"])
					}
				}

			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	if err := w.AddRecursive(watchPath); err != nil {
		log.Fatalln(err)
	}

	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}

}
