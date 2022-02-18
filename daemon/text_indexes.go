package main

import (
  "github.com/microcosm-cc/bluemonday"
  "github.com/pkg/errors"
  "github.com/saenuma/flaarum/flaarum_shared"
  "os"
  "strings"
  "log"
  "path/filepath"
  "sync"
  "fmt"
  "encoding/json"
)



var (
	mutexesMap map[string]*sync.Mutex
	debugMode bool
	STOP_WORDS []string
)


func init() {
	mutexesMap = make(map[string]*sync.Mutex)

	debug := flaarum_shared.GetSetting("debug")
	if debug == "true" {
		debugMode = true
	}

  // load stop words once
  stopWordsJsonPath := flaarum_shared.G("english-stopwords.json")
  jsonBytes, err := os.ReadFile(stopWordsJsonPath)
  if err != nil {
    panic(err)
  }
  stopWordsList := make([]string, 0)
  err = json.Unmarshal(jsonBytes, &stopWordsList)
  if err != nil {
    panic(err)
  }
  STOP_WORDS = stopWordsList

  // create daemon_instrs path

}


func createMutexIfNecessary(projName, tableName, fieldName string) {
	objName := projName + ":" + tableName + ":" + fieldName
	_, ok := mutexesMap[objName]
	if ! ok {
		mutexesMap[objName] = &sync.Mutex{}
	}
}


func doIndex(textPath string) {
	raw, err := os.ReadFile(textPath)
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
		if flaarum_shared.FindIn(STOP_WORDS, word) != -1 {
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
		err = os.WriteFile(filepath.Join(dirToMake, textIndex), []byte(fmt.Sprintf("%d", wordCount)), 0777)
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

	dirsFIs, err := os.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName))
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
		filesFIs, err := os.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes", fieldName, dirFI.Name()))
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
