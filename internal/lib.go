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

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"github.com/saenuma/flaarumlib"
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

// data_folder is the folder in /var/snap/flaarum/common containing flaarum's generated
// data. This is to support using flaarum with mounted disks.
// default is / . meaning /var/snap/flaarum/common
// use paths relative to /var/snap/flaarum/common
data_folder: /
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
	if strings.HasPrefix(dd, "/var/snap/go") || dd == "" {
		dd = filepath.Join(hd, "Flaarum")
		os.MkdirAll(dd, 0777)
	} else {
		dd = os.Getenv("SNAP_COMMON")

		confPath := filepath.Join(dd, "flaarum.zconf")
		conf, err := zazabul.LoadConfigFile(confPath)
		if err != nil {
			fmt.Printf("%+v\n", err)
			return dd, err
		}

		dataFolderSetting := conf.Get("data_folder")
		if dataFolderSetting != "/" {
			dd = filepath.Join(dd, dataFolderSetting)
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

func GetTableStructureParsed(projName, tableName string, versionNum int) (flaarumlib.TableStruct, error) {
	dataPath, _ := GetDataPath()
	raw, err := os.ReadFile(filepath.Join(dataPath, projName, tableName, fmt.Sprintf("structure%d.txt", versionNum)))
	if err != nil {
		return flaarumlib.TableStruct{}, errors.Wrap(err, "ioutil error")
	}

	return flaarumlib.ParseTableStructureStmt(string(raw))
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

func GetCurrentTableStructureParsed(projName, tableName string) (flaarumlib.TableStruct, error) {
	currentVersionNum, err := GetCurrentVersionNum(projName, tableName)
	if err != nil {
		return flaarumlib.TableStruct{}, err
	}
	return GetTableStructureParsed(projName, tableName, currentVersionNum)
}

func IsInternalProjectName(projName string) bool {
	if projName == "keyfile" || projName == "first_proj" {
		return true
	}

	if strings.HasPrefix(projName, "flaarum_export_") {
		return true
	}

	return false
}

func ListTables(projName string) ([]string, error) {
	dataPath, _ := GetDataPath()
	tablesPath := filepath.Join(dataPath, projName)

	tablesFIs, err := os.ReadDir(tablesPath)
	if err != nil {
		return nil, errors.Wrap(err, "read directory failed.")
	}

	tables := make([]string, 0)
	for _, tfi := range tablesFIs {
		if tfi.IsDir() {
			tables = append(tables, tfi.Name())
		}
	}

	return tables, nil
}

func ConfirmFieldType(projName, tableName, fieldName, fieldType, version string) bool {
	versionInt, _ := strconv.Atoi(version)
	tableStruct, err := GetTableStructureParsed(projName, tableName, versionInt)
	if err != nil {
		return false
	}

	if fieldName == "id" && fieldType == "int" {
		return true
	}

	for _, fd := range tableStruct.Fields {
		if fd.FieldName == fieldName && fd.FieldType == fieldType {
			return true
		}
	}
	return false
}

func GetLocalFlaarumClient(project string) flaarumlib.Client {
	var keyStr string
	inProd := GetSetting("in_production")
	if inProd == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	if inProd == "true" {
		keyStrPath := GetKeyStrPath()
		raw, err := os.ReadFile(keyStrPath)
		if err != nil {
			color.Red.Println(err)
			os.Exit(1)
		}
		keyStr = string(raw)
	} else {
		keyStr = "not-yet-set"
	}
	port := GetSetting("port")
	if port == "" {
		color.Red.Println("unexpected error. Have you installed  and launched flaarum?")
		os.Exit(1)
	}
	var cl flaarumlib.Client

	portInt, err := strconv.Atoi(port)
	if err != nil {
		color.Red.Println("Invalid port setting.")
		os.Exit(1)
	}

	if portInt != PORT {
		cl = flaarumlib.NewClientCustomPort("127.0.0.1", keyStr, project, portInt)
	} else {
		cl = flaarumlib.NewClient("127.0.0.1", keyStr, project)
	}

	err = cl.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return cl
}
