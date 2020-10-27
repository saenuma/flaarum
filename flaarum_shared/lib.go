// This package contains functions shared by the programs of this project. Some of these functions is expected to run
// on the same machine as a flaarum server.
package flaarum_shared

import (
	"strings"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"io/ioutil"
	"math/rand"
	"time"
	"encoding/json"
  "github.com/kljensen/snowball"  
  "fmt"
  "strconv"
  "github.com/adam-hanna/arrayOperations"
)


const (
  BROWSER_DATE_FORMAT = "2006-01-02"
  BROWSER_DATETIME_FORMAT = "2006-01-02T15:04"
  STRING_MAX_LENGTH = 100
  TEXT_INTR_DELIM = "~~~"
  BACKUP_EXT = "flaa1"
)


func DoesPathExists(p string) bool {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return false
	}
	return true
}


func G(objectName string) string {
  homeDir, err := os.UserHomeDir()
  if err != nil {
    panic(err)
  }
  folders := make([]string, 0)
  folders = append(folders, filepath.Join(homeDir, "flaarum/flaarum_store"))
  folders = append(folders, filepath.Join(homeDir, "flaarum"))
  folders = append(folders, os.Getenv("SNAP"))

  for _, dir := range folders {
    testPath := filepath.Join(dir, objectName)
    if DoesPathExists(testPath) {
      return testPath
    }
  }

  panic("Improperly configured.")
}


func GetConfigPath() (string, error) {
	dd := os.Getenv("SNAP_DATA")
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, ".flaarum.json")	
	} else {
		dd = filepath.Join(dd, "flaarum.json")
	}
	return dd, nil	
}


func GetCtlConfigPath() (string, error) {
  confPath, err := GetConfigPath()
  if err != nil {
    return "", err
  }
  return strings.Replace(confPath, "flaarum.json", "flaarumctl.json", 1), nil
}


func GetDataPath() (string, error) {
	dd := os.Getenv("SNAP_DATA")		
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, ".flaarum_data")	
	} else {
		dd = filepath.Join(dd, "data")
	}
	return dd, nil
}


func GetSetting(settingName string) (string, error) {
	settingFilePath, err := GetConfigPath()
	if err != nil {
		return "", err
	}
  raw, err := ioutil.ReadFile(settingFilePath)
  if err != nil {
    return "", errors.Wrap(err, "read failed.")
  }

  settingObj := make(map[string]string)
  err = json.Unmarshal(raw, &settingObj)
  if err != nil {
    return "", errors.Wrap(err, "json failed.")
  }

  return settingObj[settingName], nil
}


func GetKeyStrPath() string {
	var keyPath string
  dd := os.Getenv("SNAP_DATA")
  if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
    hd, err := os.UserHomeDir()
    if err != nil {
      panic(errors.Wrap(err, "os error"))
    }
    keyPath = filepath.Join(hd, "flaarum.keyfile")
  } else {
    keyPath = filepath.Join(dd, "flaarum.keyfile")
  }
  return keyPath
}


func GetFlaarumPath(fileName string) (string, error) {
  hd, err := os.UserHomeDir()
  if err != nil {
    return "", errors.Wrap(err, "os error")
  }
  dd := os.Getenv("SNAP_USER_DATA")

  if strings.HasPrefix(dd, filepath.Join(hd, "snap", "go")) || dd == "" {
    dd = filepath.Join(hd, fileName)  
  } else {
    dd = filepath.Join(dd, fileName)
  }
  return dd, nil  
}


func GetPort() string {
  port, err := GetSetting("port")
  if err != nil {
    panic(err)
  }

  return port
}



func FindIn(container []string, elem string) int {
	for i, o := range container {
		if o == elem {
			return i
		}
	}
	return -1
}


func DoesTableExists(projName, tableName string) bool {
  dataPath, _ := GetDataPath()
  if _, err := os.Stat(filepath.Join(dataPath, projName, tableName)); os.IsNotExist(err) {
    return false
  } else {
    return true
  }
}


func UntestedRandomString(length int) string {
  var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
  const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

  b := make([]byte, length)
  for i := range b {
    b[i] = charset[seededRand.Intn(len(charset))]
  }
  return string(b)
}


var ALLOWED_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"

func CleanWord(word string) string {
  word = strings.ToLower(word)

  allowedCharsList := strings.Split(ALLOWED_CHARS, "")

  if strings.HasSuffix(word, "'s") {
    word = word[: len(word) - len("'s")]
  }

  newWord := ""
  for _, ch := range strings.Split(word, "") {
    if FindIn(allowedCharsList, ch) != -1 {
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


func GetTableStructureParsed(projName, tableName string, versionNum int) (TableStruct, error) {
  dataPath, _ := GetDataPath()
  raw, err := ioutil.ReadFile(filepath.Join(dataPath, projName, tableName, "structures", strconv.Itoa(versionNum)))
  if err != nil {
    return TableStruct{}, errors.Wrap(err, "ioutil error")
  }

  return ParseTableStructureStmt(string(raw))
}


func MakeSafeIndexName(v string) string {
  return strings.ReplaceAll(v, "/", "~~a~~")
}


func MakeIndex(projName, tableName, fieldName, newData, rowId string) error {
  dataPath, _ := GetDataPath()
  indexFolder := filepath.Join(dataPath, projName, tableName, "indexes", fieldName)
  err := os.MkdirAll(indexFolder, 0777)
  if err != nil {
    return errors.Wrap(err, "create directory failed.")
  }
  indexPath := filepath.Join(indexFolder, MakeSafeIndexName(newData))
  if _, err := os.Stat(indexPath); os.IsNotExist(err) {
    err = ioutil.WriteFile(indexPath, []byte(rowId), 0777)
    if err != nil {
      return errors.Wrap(err, "file write failed.")
    }
  } else {
    raw, err := ioutil.ReadFile(indexPath)
    if err != nil {
      return errors.Wrap(err, "read failed.")
    }
    previousEntries := strings.Split(string(raw), "\n")
    newEntries := arrayOperations.UnionString(previousEntries, []string{rowId})
    err = ioutil.WriteFile(indexPath, []byte(strings.Join(newEntries, "\n")), 0777)
    if err != nil {
      return errors.Wrap(err, "write failed.")
    }
  }

  return nil
}