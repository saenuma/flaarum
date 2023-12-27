package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gookit/color"
	"github.com/radovskyb/watcher"
	"github.com/saenuma/flaarum"
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

	// watch for new files
	w := watcher.New()

	go func() {
		for {
			select {
			case event := <-w.Event:
				if strings.HasSuffix(event.Path, ".instr_json") {
					// go doIndex(event.Path)
					// if debugMode {
					// 	fmt.Println("indexed: " + event.Path)
					// }
					var instrData map[string]string
					rawJson, _ := os.ReadFile(event.Path)
					json.Unmarshal(rawJson, &instrData)
					if instrData["cmd"] == "reindex" {
						go reindex(instrData["project"], instrData["table"], event.Path)
					} else if instrData["cmd"] == "json" {
						go exportAsJSON(instrData["project"], instrData["table"], event.Path)
					} else if instrData["cmd"] == "csv" {
						go exportAsCSV(instrData["project"], instrData["table"], event.Path)
					}
				}

			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	if err := w.Add(dataPath); err != nil {
		log.Fatalln(err)
	}

	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}

}

func getFlaarumCLIClient() flaarum.Client {
	var keyStr string
	inProd := flaarum_shared.GetSetting("in_production")
	if inProd == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := flaarum_shared.GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	port := flaarum_shared.GetSetting("port")
	if port == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	var cl flaarum.Client

	portInt, err := strconv.Atoi(port)
	if err != nil {
		color.Red.Println("Invalid port setting.")
		os.Exit(1)
	}

	if portInt != flaarum_shared.PORT {
		cl = flaarum.NewClientCustomPort("127.0.0.1", keyStr, "first_proj", portInt)
	} else {
		cl = flaarum.NewClient("127.0.0.1", keyStr, "first_proj")
	}

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return cl
}
