// This package contains functions shared by the programs of this project. Some of these functions is expected to run
// on the same machine as a flaarum server.
package flaarum_shared

import (
	"strings"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"math/rand"
	"time"
  "github.com/kljensen/snowball"
  "fmt"
  "strconv"
  "github.com/adam-hanna/arrayOperations"
  "github.com/bankole7782/zazabul"
	"math"
	"sort"
)


const (
  DATE_FORMAT = "2006-01-02"
  DATETIME_FORMAT = "2006-01-02T15:04 MST"
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
	dd := os.Getenv("SNAP_COMMON")
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, "flaarum_data", "flaarum.zconf")
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
    return "/var/snap/flaarum/common/data_btrfs", nil
  } else {
    dd := os.Getenv("SNAP_COMMON")
    if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
      hd, err := os.UserHomeDir()
      if err != nil {
        return "", errors.Wrap(err, "os error")
      }
      dd = filepath.Join(hd, "flaarum_data")
			os.MkdirAll(dd, 0777)
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
  dd := os.Getenv("SNAP_COMMON")
  if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
    hd, err := os.UserHomeDir()
    if err != nil {
      panic(errors.Wrap(err, "os error"))
    }
    keyPath = filepath.Join(hd, "flaarum_data", "flaarum.keyfile")
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
  dd := os.Getenv("SNAP_USER_COMMON")

  if strings.HasPrefix(dd, filepath.Join(hd, "snap", "go")) || dd == "" {
    dd = filepath.Join(hd, "flaarum_data", fileName)
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


func GetCurrentVersionNum(projName, tableName string) (int, error) {
	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	tableStructsFIs, err := os.ReadDir(filepath.Join(tablePath, "structures"))
	if err != nil {
		return -1, errors.Wrap(err, "ioutil error")
	}

	versionNumbers := make([]int, 0)
	for _, tsfi := range tableStructsFIs {
		num, err := strconv.Atoi(tsfi.Name())
		if err != nil {
			return -1, errors.Wrap(err, "strconv error.")
		}
		versionNumbers = append(versionNumbers, num)
	}

	sort.Ints(versionNumbers)
	currentVersionNum := versionNumbers[len(versionNumbers) - 1]
	return currentVersionNum, nil
}


func GetTableStructureParsed(projName, tableName string, versionNum int) (TableStruct, error) {
  dataPath, _ := GetDataPath()
  raw, err := os.ReadFile(filepath.Join(dataPath, projName, tableName, "structures", strconv.Itoa(versionNum)))
  if err != nil {
    return TableStruct{}, errors.Wrap(err, "ioutil error")
  }

  return ParseTableStructureStmt(string(raw))
}


func MakeSafeIndexName(v string) string {
  return strings.ReplaceAll(v, "/", "~~a~~")
}


func GetFieldType(projName, tableName, fieldName string) string {
	versionNum, _ := GetCurrentVersionNum(projName, tableName)
	tableStruct, _ := GetTableStructureParsed(projName, tableName, versionNum)

	fieldNamesToFieldTypes := make(map[string]string)

	if fieldName == "_version" || fieldName == "id" {
		return "int"
	}

	for _, fieldStruct := range tableStruct.Fields {
		fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
	}

	if strings.HasSuffix(fieldName, "_year") {
		genFieldName := fieldName[0 : len(fieldName) - len("_year")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_month") {
		genFieldName := fieldName[0 : len(fieldName) - len("_month")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_day") {
		genFieldName := fieldName[0 : len(fieldName) - len("_day")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_hour") {
		genFieldName := fieldName[0 : len(fieldName) - len("_hour")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_date") {
		genFieldName := fieldName[0 : len(fieldName) - len("_date")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "date"
		}
	} else if strings.HasSuffix(fieldName, "_tzname") {
		genFieldName := fieldName[0 : len(fieldName) - len("_tzname")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "string"
		}
	}


	return fieldNamesToFieldTypes[fieldName]
}


func MakeIndex(projName, tableName, fieldName, newData, rowId string) error {
	// make exact search indexes
  dataPath, _ := GetDataPath()
  indexFolder := filepath.Join(dataPath, projName, tableName, "indexes", fieldName)
  err := os.MkdirAll(indexFolder, 0777)
  if err != nil {
    return errors.Wrap(err, "create directory failed.")
  }
  indexPath := filepath.Join(indexFolder, MakeSafeIndexName(newData))
  if _, err := os.Stat(indexPath); os.IsNotExist(err) {
    err = os.WriteFile(indexPath, []byte(rowId), 0777)
    if err != nil {
      return errors.Wrap(err, "file write failed.")
    }
  } else {
    raw, err := os.ReadFile(indexPath)
    if err != nil {
      return errors.Wrap(err, "read failed.")
    }
    previousEntries := strings.Split(string(raw), "\n")
    newEntries := arrayOperations.UnionString(previousEntries, []string{rowId})
    err = os.WriteFile(indexPath, []byte(strings.Join(newEntries, "\n")), 0777)
    if err != nil {
      return errors.Wrap(err, "write failed.")
    }
  }

	// make integer indexes
	fieldType := GetFieldType(projName, tableName, fieldName)
	if fieldType == "int" {
		intIndexesFile := filepath.Join(dataPath, projName, tableName, "intindexes", fieldName)
		if ! DoesPathExists(intIndexesFile) {
			out := fmt.Sprintf("%s: %s\n", newData, rowId)
			os.WriteFile(intIndexesFile, []byte(out), 0777)
		} else {
			intIndexes, err := ReadIntIndexesFromFile(intIndexesFile)
			if err != nil {
				return err
			}
			intIndex, err := strconv.ParseInt(newData, 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv error")
			}
			rowIdInt, err := strconv.ParseInt(rowId, 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv error")
			}

			newIntIndexes := AppendToIntIndexes(intIndexes, intIndex, rowIdInt)
			err = WriteIntIndexesToFile(newIntIndexes, intIndexesFile)
			if err != nil {
				return err
			}
		}
	} else if fieldType == "float" {
		// make integer indexes
		intIndexesFile := filepath.Join(dataPath, projName, tableName, "intindexes", fieldName)
		if ! DoesPathExists(intIndexesFile) {
			newDataFloat, _ := strconv.ParseFloat(newData, 64)
			newDataInt := int64( math.Ceil(newDataFloat) )
			out := fmt.Sprintf("%d: %s", newDataInt, rowId)
			os.WriteFile(intIndexesFile, []byte(out), 0777)
		} else {
			intIndexes, err := ReadIntIndexesFromFile(intIndexesFile)
			if err != nil {
				return err
			}
			newDataFloat, _ := strconv.ParseFloat(newData, 64)
			newDataInt := int64( math.Ceil(newDataFloat) )

			rowIdInt, err := strconv.ParseInt(rowId, 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv error")
			}

			newIntIndexes := AppendToIntIndexes(intIndexes, newDataInt, rowIdInt)
			err = WriteIntIndexesToFile(newIntIndexes, intIndexesFile)
			if err != nil {
				return err
			}

		}

	} else if fieldType == "date" || fieldType == "datetime" {
		// make time indexes
		timeIndexesFile := filepath.Join(dataPath, projName, tableName, "timeindexes", fieldName)
		if ! DoesPathExists(timeIndexesFile) {
			out := fmt.Sprintf("%s::%s\n", newData, rowId)
			os.WriteFile(timeIndexesFile, []byte(out), 0777)
		} else {
			oldTimeIndexes, err := ReadTimeIndexesFromFile(timeIndexesFile, fieldType)
			if err != nil {
				return err
			}

			var timeValue time.Time
			if fieldType == "date" {
				timeValue, err = time.Parse(DATE_FORMAT, newData)
				if err != nil {
					return errors.Wrap(err, "time error")
				}
			} else if fieldType == "datetime" {
				timeValue, err = time.Parse(DATETIME_FORMAT, newData)
				if err != nil {
					return errors.Wrap(err, "time error")
				}
			}

			rowIdInt, err := strconv.ParseInt(rowId, 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv error")
			}

			newTimeIndexes := AppendToTimeIndexes(oldTimeIndexes, timeValue, rowIdInt)
			err = WriteTimeIndexesToFile(newTimeIndexes, timeIndexesFile, fieldType)
		}

	} else if fieldType == "string" {
		likeIndexesPath := filepath.Join(dataPath, projName, tableName, "likeindexes", fieldName)
		os.MkdirAll(likeIndexesPath, 0777)
		charsOfData := strings.Split(strings.ToLower(newData), "")
		for _, char := range charsOfData {
			if char == "/" || char == " " || char == "\t" {
				continue
			}
			indexesForAChar := filepath.Join(likeIndexesPath, strings.ToLower(char))
			if ! DoesPathExists(indexesForAChar) {
				err = os.WriteFile(indexesForAChar, []byte(rowId), 0777)
				if err != nil {
					return errors.Wrap(err, "file write failed.")
				}
			} else {
				raw, err := os.ReadFile(indexesForAChar)
		    if err != nil {
		      return errors.Wrap(err, "read failed.")
		    }
		    previousEntries := strings.Split(string(raw), "\n")
		    newEntries := arrayOperations.UnionString(previousEntries, []string{rowId})
		    err = os.WriteFile(indexesForAChar, []byte(strings.Join(newEntries, "\n")), 0777)
		    if err != nil {
		      return errors.Wrap(err, "write failed.")
		    }
			}
		}
	}

  return nil
}


func IsNotIndexedFieldVersioned(projName, tableName, fieldName, version string) bool {
	versionInt, _ := strconv.Atoi(version)
	ts, _ := GetTableStructureParsed(projName, tableName, versionInt)
	for _, fd := range ts.Fields {
		if fd.FieldName == fieldName && fd.NotIndexed == true {
			return true
		}
	}
	return false
}
