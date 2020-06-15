package flaarum

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"time"
	"fmt"
)


// InsertRowStr inserts a row into a table. It expects the input to be of type map[string]string
func (cl *Client) InsertRowStr(tableName string, toInsert map[string]string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Add("keyStr", cl.KeyStr)
	for k, v := range toInsert {
		urlValues.Add(k, v)
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%sinsert-row/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return -1, errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, errors.Wrap(err, "ioutil error")
	}

	if resp.StatusCode == 200 {
		retId, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return -1, errors.Wrap(err, "strconv error")
		}
		return retId, nil
	} else {
		return -1, errors.New(string(body))
	}
}


// InsertRowStr inserts a row into a table. It expects the toInsert to be of type map[string]interface{}.
func (cl *Client) InsertRowAny(tableName string, toInsert map[string]interface{}) (int64, error) {
	currentVersionNum, err := cl.GetCurrentTableVersionNum(tableName)
	if err != nil {
		return -1, err
	}
	tableStruct, err := cl.GetTableStructureParsed(tableName, currentVersionNum)
	if err != nil {
		return -1, err
	}
	fieldNamesToFieldTypes := make(map[string]string)

	for _, fieldStruct := range tableStruct.Fields {
		fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
	}

	toInsertStr := make(map[string]string)
	for k, v := range toInsert {
		switch vInType := v.(type) {
		case int:
			vInStr := strconv.Itoa(vInType)
			toInsertStr[k] = vInStr
		case int64:
			vInStr := strconv.FormatInt(vInType, 10)
			toInsertStr[k] = vInStr
		case float64:
			vInStr := strconv.FormatFloat(vInType, 'g', -1, 64)
			toInsertStr[k] = vInStr
		case bool:
			var vInStr string
			if vInType == true {
				vInStr = "t"
			} else if vInType == false {
				vInStr = "f"
			}
			toInsertStr[k] = vInStr
		case time.Time:
			ft, ok := fieldNamesToFieldTypes[k]
			if ! ok {
				return -1, errors.New(fmt.Sprintf("The field '%s' is not in the structure of table '%s' of project '%s'", 
					k, tableName, cl.ProjName))
			}
			if ft == "date" {
				toInsertStr[k] = RightDateFormat(vInType)
			} else if ft == "datetime" {
				toInsertStr[k] = RightDateTimeFormat(vInType)
			}
		}
	}

	return cl.InsertRowStr(tableName, toInsertStr)
}

