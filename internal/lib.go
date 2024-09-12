// This package contains functions shared by the programs of this project. Some of these functions is expected to run
// on the same machine as a flaarum server.
package internal

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/saenuma/zazabul"
)

const (
	PORT = 22318
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
	folders = append(folders, filepath.Join(homeDir, "Flaarum"))
	folders = append(folders, filepath.Join(homeDir, ".flaar312"))
	folders = append(folders, os.Getenv("SNAP_COMMON"))

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
	dd = os.Getenv("SNAP_COMMON")
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		dd = filepath.Join(hd, "Flaarum")
		os.MkdirAll(dd, 0777)
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
	hd, err := os.UserHomeDir()
	if err != nil {
		return "", errors.Wrap(err, "os error")
	}

	var dd string
	dd = os.Getenv("SNAP_USER_COMMON")
	if strings.HasPrefix(dd, hd) || dd == "" {
		dd = filepath.Join(hd, "Flaarum")
		os.MkdirAll(dd, 0777)
	}

	return filepath.Join(dd, fileName), nil
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

	return fieldNamesToFieldTypes[fieldName]
}

func PrintError(w http.ResponseWriter, err error) {
	fmt.Printf("%+v\n", err)
	debug := GetSetting("debug")
	if debug == "true" {
		http.Error(w, fmt.Sprintf("%+v", err), http.StatusInternalServerError)
	} else {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
	}
}

func GetTablePath(projName, tableName string) string {
	dataPath, _ := GetDataPath()
	return filepath.Join(dataPath, projName, tableName)
}

func GetCurrentTableStructureParsed(projName, tableName string) (TableStruct, error) {
	currentVersionNum, err := GetCurrentVersionNum(projName, tableName)
	if err != nil {
		return TableStruct{}, err
	}
	return GetTableStructureParsed(projName, tableName, currentVersionNum)
}
