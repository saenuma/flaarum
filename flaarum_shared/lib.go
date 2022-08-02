// This package contains functions shared by the programs of this project. Some of these functions is expected to run
// on the same machine as a flaarum server.
package flaarum_shared

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/saenuma/zazabul"
)

const (
	DATE_FORMAT       = "2006-01-02"
	DATETIME_FORMAT   = "2006-01-02T15:04 MST"
	STRING_MAX_LENGTH = 100
	PORT              = 22318
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
	folders = append(folders, os.Getenv("SNAP"))

	for _, dir := range folders {
		testPath := filepath.Join(dir, objectName)
		if DoesPathExists(testPath) {
			return testPath
		}
	}

	fmt.Println("Could not find: ", objectName)
	panic("Improperly configured.")
}

func GetRootPath() (string, error) {
	hd, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}

	var dd string
	if runtime.GOOS == "windows" {
		dd = filepath.Join(hd, "Flaarum")
		os.MkdirAll(dd, 0777)

	} else {

		dd = os.Getenv("SNAP_COMMON")
		if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
			dd = filepath.Join(hd, "Flaarum")
			os.MkdirAll(dd, 0777)
		}
	}

	return dd, nil
}

func GetDataPath() (string, error) {
	return GetRootPath()
}

func GetSetting(settingName string) string {
	confPath, err := GetConfigPath()
	if err != nil {
		fmt.Printf("%+v\n", err)
		return ""
	}

	conf, err := zazabul.LoadConfigFile(confPath)
	if err != nil {
		fmt.Printf("%+v\n", err)
	}

	return conf.Get(settingName)
}

func GetConfigPath() (string, error) {
	rootPath, err := GetRootPath()
	if err != nil {
		return "", err
	}

	return filepath.Join(rootPath, "flaarum.zconf"), nil
}

func GetKeyStrPath() string {
	rootPath, err := GetRootPath()
	if err != nil {
		panic(err)
	}
	return filepath.Join(rootPath, "flaarum.keyfile")
}

// this function is used to get input paths to flaarum
func GetFlaarumPath(fileName string) (string, error) {
	var dd string
	hd, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}

	if runtime.GOOS == "windows" {
		dd = filepath.Join(hd, "Flaarum", fileName)
	} else {
		dd = os.Getenv("SNAP_USER_COMMON")
		if strings.HasPrefix(dd, filepath.Join(hd, "snap", "go")) || dd == "" {
			dd = filepath.Join(hd, "Flaarum", fileName)
		} else {
			dd = filepath.Join(dd, fileName)
		}
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
	currentVersionNum := versionNumbers[len(versionNumbers)-1]
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
		genFieldName := fieldName[0 : len(fieldName)-len("_year")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_month") {
		genFieldName := fieldName[0 : len(fieldName)-len("_month")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_day") {
		genFieldName := fieldName[0 : len(fieldName)-len("_day")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && (ans == "datetime" || ans == "date") {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_hour") {
		genFieldName := fieldName[0 : len(fieldName)-len("_hour")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "int"
		}
	} else if strings.HasSuffix(fieldName, "_date") {
		genFieldName := fieldName[0 : len(fieldName)-len("_date")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "date"
		}
	} else if strings.HasSuffix(fieldName, "_tzname") {
		genFieldName := fieldName[0 : len(fieldName)-len("_tzname")]
		ans, ok := fieldNamesToFieldTypes[genFieldName]

		if ok && ans == "datetime" {
			return "string"
		}
	}

	return fieldNamesToFieldTypes[fieldName]
}

// Platform independent newline
func GetNewline() string {
	if runtime.GOOS == "windows" {
		return "\r\n"
	} else {
		return "\n"
	}
}
