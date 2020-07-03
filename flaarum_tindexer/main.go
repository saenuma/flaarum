package main

import (
	"github.com/bankole7782/flaarum/flaarum_shared"
	"fmt"
	"log"
	"time"
	"github.com/radovskyb/watcher"
	"strings"
	"encoding/json"
	"io/ioutil"
	"github.com/pkg/errors"
	"path/filepath"
	"os"
	"github.com/kljensen/snowball"	
)


var (
	STOP_WORDS []string
)


func init() {
	// load stop words once
	stopWordsJsonPath := flaarum_shared.G("english-stopwords.json")
	jsonBytes, err := ioutil.ReadFile(stopWordsJsonPath)
	if err != nil {
		panic(err)
	}
	stopWordsList := make([]string, 0)
	err = json.Unmarshal(jsonBytes, &stopWordsList)
	if err != nil {
		panic(err)
	}
	STOP_WORDS = stopWordsList

}


func P(err error) {
	fmt.Printf("%+v\n", err)
}

func main() {
	w := watcher.New()

	go func() {
		for {
			select {
			case event := <-w.Event:
				if strings.HasSuffix(event.Path, ".text") && (event.Op == watcher.Write || event.Op == watcher.Create) {
					doIndex(event.Path)
					fmt.Println("indexed: " + event.Path)
				}

				if strings.HasSuffix(event.Path, ".rtext") && (event.Op == watcher.Write || event.Op == watcher.Create) {
					removeIndex(event.Path)
					fmt.Println("remove index from instruction file: " + event.Path)
				}
			case err := <-w.Error:
				log.Fatalln(err)
			case <-w.Closed:
				return
			}
		}
	}()

	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		panic(err)
	}

	if err := w.AddRecursive(dataPath); err != nil {
		log.Fatalln(err)
	}

	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Fatalln(err)
	}

}

var ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"

func cleanWord(word string) string {
	word = strings.ToLower(word)

	allowedCharsList := strings.Split(ALLOWED_CHARS, "")

	if strings.HasSuffix(word, "'s") {
		word = word[: len(word) - len("'s")]
	}

	newWord := ""
	for _, ch := range strings.Split(word, "") {
		if flaarum_shared.FindIn(allowedCharsList, ch) != -1 {
			newWord += ch
		}
	}

	var toWriteWord string
	stemmed, err := snowball.Stem(newWord, "english", true)
	if err != nil {
		toWriteWord = newWord
		fmt.Println(errors.Wrap(err, "stemmer error."))
	}
	toWriteWord = stemmed

	return toWriteWord
}


func doIndex(textPath string) {
	raw, err := ioutil.ReadFile(textPath)
	if err != nil {
		P(errors.Wrap(err, "ioutil error"))
		return
	}
	words := strings.Fields(string(raw))

	wordCountMap := make(map[string]int64)
	for _, word := range words {
		// clean the word.
		word = cleanWord(word)
		if word == "" {
			continue
		}

		oldCount, ok := wordCountMap[word]

		if flaarum_shared.FindIn(STOP_WORDS, word) != -1 {
			continue
		}

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
	textIndex := strings.ReplaceAll(parts[3], ".text", "")
	removeIndexInner(projName, tableName, textIndex)

	for word, wordCount := range wordCountMap {
		dirToMake := filepath.Join(dataPath, projName, tableName, "tindexes", word)
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


func removeIndexInner(projName, tableName, textIndex string) {
	dataPath, err := flaarum_shared.GetDataPath()
	if err != nil {
		P(err)
		return
	}

	dirsFIs, err := ioutil.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes"))
	if err != nil {
		P(errors.Wrap(err, "ioutil error."))
		return
	}	

	for _, dirFI := range dirsFIs {
		lookingForPath := filepath.Join(dataPath, projName, tableName, "tindexes", dirFI.Name(), textIndex)
		if flaarum_shared.DoesPathExists(lookingForPath) {
			err := os.RemoveAll(lookingForPath)
			if err != nil {
				P(errors.Wrap(err, "os remove error."))
				return
			}
		}
	}

	for _, dirFI := range dirsFIs {
		filesFIs, err := ioutil.ReadDir(filepath.Join(dataPath, projName, tableName, "tindexes", dirFI.Name()))
		if err == nil && len(filesFIs) == 0 {
			err = os.RemoveAll(filepath.Join(dataPath, projName, tableName, "tindexes", dirFI.Name()))
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
	textIndex := strings.ReplaceAll(parts[3], ".rtext", "")

	removeIndexInner(projName, tableName, textIndex)

	err = os.RemoveAll(textPath)
	if err != nil {
		P(errors.Wrap(err, "os remove error."))
		return
	}
}
