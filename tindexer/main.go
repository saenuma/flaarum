package main

import (
	"github.com/bankole7782/flaarum/flaarum_shared"
	"fmt"
	"log"
	"time"
	"github.com/radovskyb/watcher"
	"strings"
	"io/ioutil"
	"github.com/pkg/errors"
	"path/filepath"
	"os"
	"sync"
	"github.com/microcosm-cc/bluemonday"
)

var (
	mutexesMap map[string]*sync.Mutex
	debugMode bool
)

func init() {
	mutexesMap = make(map[string]*sync.Mutex)

	debug, err := flaarum_shared.GetSetting("debug")
	if err != nil {
		panic(err)
	}
	if debug == "true" || debug == "t" {
		debugMode = true
	}
}


func createMutexIfNecessary(projName, tableName, fieldName string) {
	objName := projName + ":" + tableName + ":" + fieldName
	_, ok := mutexesMap[objName]
	if ! ok {
		mutexesMap[objName] = &sync.Mutex{}
	}
}


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


func doIndex(textPath string) {
	raw, err := ioutil.ReadFile(textPath)
	if err != nil {
		return
	}
	
	textStrippedOfHtml := bluemonday.StrictPolicy().Sanitize(string(raw))
	words := strings.Fields(textStrippedOfHtml)

	wordCountMap := make(map[string]int64)
	for _, word := range words {
		// clean the word.
		word = flaarum_shared.CleanWord(word)
		if word == "" {
			continue
		}
		if flaarum_shared.FindIn(flaarum_shared.STOP_WORDS, word) != -1 {
			continue
		}

		oldCount, ok := wordCountMap[word]
		if ! ok {
			wordCountMap[word] = 1
		} else {
			wordCountMap[word] = oldCount + 1
		}
	}

	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		log.Println(err)
		return
	}

	if ! strings.HasSuffix(dataPath, "/") {
		dataPath += "/"
	}

	strippedPath := strings.ReplaceAll(textPath, dataPath, "")
	parts := strings.Split(strippedPath, "/")
	if len(parts) != 4 {
		P(errors.New("improperly configured."))
		return
	}

	projName := parts[0]
	tableName := parts[1]
	nameFrag := strings.ReplaceAll(parts[3], ".text", "")
	parts2 := strings.Split(nameFrag, flaarum_shared.TEXT_INTR_DELIM)
	textIndex := parts2[0]
	fieldName := parts2[1]

	createMutexIfNecessary(projName, tableName, fieldName)
	mutexName := projName + ":" + tableName + ":" + fieldName
	mutexesMap[mutexName].Lock()
	defer mutexesMap[mutexName].Unlock()

	removeIndexInner(projName, tableName, fieldName, textIndex)

	for word, wordCount := range wordCountMap {
		dirToMake := filepath.Join(dataPath, projName, tableName, "tindexes", fieldName, word)
		err := os.MkdirAll(dirToMake, 0777)
		if err != nil {
			P(errors.Wrap(err, "os error."))
			return
		}
		err = ioutil.WriteFile(filepath.Join(dirToMake, textIndex), []byte(fmt.Sprintf("%d", wordCount)), 0777)
		if err != nil {
			fmt.Printf("word is : '%s'\n", word)
			P(errors.Wrap(err, "ioutil error"))
			return
		}
	}

	err = os.Remove(textPath)
	if err != nil {
		P(errors.Wrap(err, "os remove error."))
		return
	}
}


func doIndexWG(textPath string, wg *sync.WaitGroup) {
	defer wg.Done()
	doIndex(textPath)
}


func removeIndexInner(projName, tableName, fieldName, textIndex string) {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		P(err)
		return
	}

	if ! flaarum_shared.DoesPathExists(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName)) {
		return
	}

	dirsFIs, err := ioutil.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName))
	if err != nil {
		P(errors.Wrap(err, "ioutil error."))
		return
	}	

	for _, dirFI := range dirsFIs {
		lookingForPath := filepath.Join(dataPath, projName, tableName, "tindexes", fieldName, dirFI.Name(), textIndex)
		if flaarum_shared.DoesPathExists(lookingForPath) {
			err := os.RemoveAll(lookingForPath)
			if err != nil {
				P(errors.Wrap(err, "os remove error."))
				return
			}
		}
	}

	for _, dirFI := range dirsFIs {
		filesFIs, err := ioutil.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName, dirFI.Name()))
		if err == nil && len(filesFIs) == 0 {
			err = os.RemoveAll(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName, dirFI.Name()))
			if err != nil {
				P(errors.Wrap(err, "os remove error."))
				return
			}
		}
	}

}


func removeIndex(textPath string) {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		P(err)
		return
	}

	if ! strings.HasSuffix(dataPath, "/") {
		dataPath += "/"
	}

	strippedPath := strings.ReplaceAll(textPath, dataPath, "")
	parts := strings.Split(strippedPath, "/")
	if len(parts) != 4 {
		P(errors.New("improperly configured."))
		return
	}
	projName := parts[0]
	tableName := parts[1]

	nameFrag := strings.ReplaceAll(parts[3], ".rtext", "")
	parts2 := strings.Split(nameFrag, flaarum_shared.TEXT_INTR_DELIM)
	textIndex := parts2[0]
	fieldName := parts2[1]

	createMutexIfNecessary(projName, tableName, fieldName)
	mutexName := projName + ":" + tableName + ":" + fieldName
	mutexesMap[mutexName].Lock()
	defer mutexesMap[mutexName].Unlock()

	removeIndexInner(projName, tableName, fieldName, textIndex)

	err = os.RemoveAll(textPath)
	if err != nil {
		P(errors.Wrap(err, "os remove error."))
		return
	}
}


func removeIndexWG(textPath string, wg *sync.WaitGroup) {
	defer wg.Done()
	removeIndex(textPath)
}
