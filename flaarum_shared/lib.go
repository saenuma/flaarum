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
  "github.com/kljensen/snowball"  
  "fmt"
  "strconv"
  "github.com/adam-hanna/arrayOperations"
  "github.com/bankole7782/zazabul"
)


const (
  BROWSER_DATE_FORMAT = "2006-01-02"
  BROWSER_DATETIME_FORMAT = "2006-01-02T15:04"
  STRING_MAX_LENGTH = 100
  TEXT_INTR_DELIM = "~~~"
  BACKUP_EXT = "flaa1"
  PORT = 22318
)

var RootConfigTemplate = `// debug can be set to either false or true
// when it is set to true it would print more detailed error messages
debug: false

// in_production can be set to either false or true.
// when set to true, it makes the flaarum installation enforce a key
// this key can be gotten from 'flaarum.prod r' if it has been created with 'flaarum.prod c'
in_production: false

// backup_bucket is only required during production.
// You are to create a bucket in Google cloud storage and set it to this value.
// This is where the backups for your flaarum installation would be saved to.
backup_bucket: 

// backup_frequency is the number of days before conducting a backup. 
// It must be a number not a float. The default is 14 which is two weeks.
// You can set it to a lower value to test if the backup works perfectly.
// The minimum value is 1
backup_frequency: 14

`

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
  folders = append(folders, filepath.Join(homeDir, "flaarum/store"))
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
		dd = filepath.Join(hd, ".flaarum.zconf")	
	} else {
		dd = filepath.Join(dd, "flaarum.zconf")
	}
	return dd, nil	
}


func GetCtlConfigPath() (string, error) {
  confPath, err := GetConfigPath()
  if err != nil {
    return "", err
  }
  return strings.Replace(confPath, "flaarum.zconf", "flaarumctl.zconf", 1), nil
}


func GetDataPath() (string, error) {
  inProd := GetSetting("in_production")
  if inProd == "true" {
    return "/var/snap/flaarum/current/data_btrfs", nil
  } else {
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
  
}


func GetSetting(settingName string) string {
	confPath, err := GetConfigPath()
	if err != nil {
    fmt.Println("%+v", err)
    return ""
	}

  conf, err := zazabul.LoadConfigFile(confPath)
  if err != nil {
    fmt.Println("%+v", err)
  }

  return conf.Get(settingName)
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