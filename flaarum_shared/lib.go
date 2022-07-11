// This package contains functions shared by the programs of this project. Some of these functions is expected to run
// on the same machine as a flaarum server.
package flaarum_shared

import (
	"strings"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
  "fmt"
  "strconv"
  arrayOperations "github.com/adam-hanna/arrayOperations"
  "github.com/saenuma/zazabul"
	// "math"
	"sort"
	"runtime"
)


const (
  DATE_FORMAT = "2006-01-02"
  DATETIME_FORMAT = "2006-01-02T15:04 MST"
  STRING_MAX_LENGTH = 100
  BACKUP_EXT = "flaa3"
  PORT = 22318
	TEXT_INTR_DELIM = "~~~"
	FLAARUM_PATH = "/var/lib/flaarum"
)

var RootConfigTemplate = `// debug can be set to either false or true
// when it is set to true it would print more detailed error messages
debug: false

// in_production can be set to either false or true.
// when set to true, it makes the flaarum installation enforce a key
// this key can be gotten from 'flaarum.prod r' if it has been created with 'flaarum.prod c'
in_production: false

// port is used while connecting to the database
// changing the port can be used to hide your database during production
port: 22318

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
  folders = append(folders, filepath.Join(homeDir, "p", "flaarum", "store"))
	folders = append(folders, "C:\\Program Files (x86)\\Flaarum\\")
	folders = append(folders, "/opt/saenuma/flaarum/bin/")
  folders = append(folders, "/opt/saenuma/flaarum/")

  for _, dir := range folders {
    testPath := filepath.Join(dir, objectName)
    if DoesPathExists(testPath) {
      return testPath
    }
  }

	fmt.Println("Could not find: ", objectName)
  panic("Improperly configured.")
}

func GetConfigPath() (string, error) {
	var dd string
	if runtime.GOOS == "windows" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, "Flaarum", "flaarum.zconf")
	} else {
		dd = filepath.Join(FLAARUM_PATH, "flaarum.zconf")
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
	var dd string
	if runtime.GOOS == "windows" {
		hd, err := os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "os error")
		}
		dd = filepath.Join(hd, "Flaarum")
	} else {
		dd = FLAARUM_PATH
	}

	err := os.MkdirAll(dd, 0777)
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}

	return dd, nil
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
	dataPath, err := GetDataPath()
	if err != nil {
		panic(err)
	}
	return filepath.Join(dataPath, "flaarum.keyfile")
}


func GetFlaarumPath(fileName string) (string, error) {
  hd, err := os.UserHomeDir()
  if err != nil {
    return "", errors.Wrap(err, "os error")
  }

  dd := filepath.Join(hd, "Flaarum", fileName)
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


func GetCurrentVersionNum(projName, tableName string) (int, error) {
	dataPath, _ := GetDataPath()
	tablePath := filepath.Join(dataPath, projName, tableName)

	tableObjsFIs, err := os.ReadDir(tablePath)
	if err != nil {
		return -1, errors.Wrap(err, "ioutil error")
	}

	versionNumbers := make([]int, 0)
	for _, tofi := range tableObjsFIs {
		if strings.HasPrefix(tofi.Name(), "structure") {
			left := strings.Replace(tofi.Name(), "structure", "", 1)
			left = strings.Replace(left, ".txt", "", 1)
			num, err := strconv.Atoi(left)
			if err != nil {
				return -1, errors.Wrap(err, "strconv error.")
			}
			versionNumbers = append(versionNumbers, num)
		}

	}

	sort.Ints(versionNumbers)
	currentVersionNum := versionNumbers[len(versionNumbers) - 1]
	return currentVersionNum, nil
}


func GetTableStructureParsed(projName, tableName string, versionNum int) (TableStruct, error) {
  dataPath, _ := GetDataPath()
  raw, err := os.ReadFile(filepath.Join(dataPath, projName, tableName, fmt.Sprintf("structure%d.txt", versionNum)))
  if err != nil {
    return TableStruct{}, errors.Wrap(err, "ioutil error")
  }

  return ParseTableStructureStmt(string(raw))
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
	indexesF1Path := filepath.Join(dataPath, projName, tableName, fieldName + "_indexes.flaa1")
  indexesF2Path := filepath.Join(dataPath, projName, tableName, fieldName + "_indexes.flaa2")

	var begin int64
	var end int64
	if ! DoesPathExists(indexesF1Path) {
		begin = 0
		err := os.WriteFile(indexesF2Path, []byte(rowId + ","), 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		end = int64(len([]byte(rowId + ",")))
	} else {
		elemsMap, err := ParseDataF1File(indexesF1Path)
		if err != nil {
			return err
		}

		elem, ok := elemsMap[newData]
		var newDataToWrite string
		if ! ok {
			newDataToWrite = rowId + ","
		} else {
			readBytes, err := ReadPortionF2File(projName, tableName, fieldName + "_indexes", elem.DataBegin, elem.DataEnd)
			if err != nil {
				return err
			}
			previousEntries := strings.Split(string(readBytes), ",")
			newEntries := arrayOperations.Union(previousEntries, []string{rowId})
			newDataToWrite = strings.Join(newEntries, ",") + ","
		}

		f2IndexesHandle, err := os.OpenFile(indexesF2Path,	os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return errors.Wrap(err, "os error")
		}
		defer f2IndexesHandle.Close()

		stat, err := f2IndexesHandle.Stat()
		if err != nil {
			return errors.Wrap(err, "os error")
		}

		size := stat.Size()
		f2IndexesHandle.Write([]byte(newDataToWrite))
		begin = size
		end = int64(len([]byte(newDataToWrite))) + size
	}

	elem := DataF1Elem{newData, begin, end}
	err := AppendDataF1File(projName, tableName, fieldName + "_indexes", elem)
	if err != nil {
		return errors.Wrap(err, "os error")
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


// Platform independent newline
func GetNewline() string {
	if runtime.GOOS == "windows" {
		return "\r\n"
	} else {
		return "\n"
	}
}
