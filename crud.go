package flaarum

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/saenuma/flaarum/flaarum_shared"
)

// InsertRowStr inserts a row into a table. It expects the input to be of type map[string]string.
// It returns the id of the newly created row
func (cl *Client) InsertRowStr(tableName string, toInsert map[string]string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	for k, v := range toInsert {
		urlValues.Add(k, v)
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%sinsert-row/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return 0, ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		retId, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if err != nil {
			return 0, ServerError{err.Error()}
		}
		return retId, nil
	} else if resp.StatusCode == 400 {
		return 0, ValidationError{string(body)}
	} else {
		return 0, ServerError{string(body)}
	}
}

func (cl *Client) convertInterfaceMapToStringMap(tableName string, oldMap map[string]interface{}) (map[string]string, error) {
	currentVersionNum, err := cl.GetCurrentTableVersionNum(tableName)
	if err != nil {
		return nil, err
	}
	tableStruct, err := cl.GetTableStructureParsed(tableName, currentVersionNum)
	if err != nil {
		return nil, err
	}
	fieldNamesToFieldTypes := make(map[string]string)

	for _, fieldStruct := range tableStruct.Fields {
		fieldNamesToFieldTypes[fieldStruct.FieldName] = fieldStruct.FieldType
	}

	newMap := make(map[string]string)
	for k, v := range oldMap {
		if strings.Contains(k, ".") || strings.Contains(k, ",") || strings.Contains(k, " ") || strings.Contains(k, "\n") {
			return nil, errors.New("a field name cannot contain any of '.', ',', ' ', '\\n',")
		}

		switch vInType := v.(type) {
		case int:
			vInStr := strconv.Itoa(vInType)
			newMap[k] = vInStr
		case int64:
			vInStr := strconv.FormatInt(vInType, 10)
			newMap[k] = vInStr
		case bool:
			var vInStr string
			if vInType {
				vInStr = "t"
			} else if !vInType {
				vInStr = "f"
			}
			newMap[k] = vInStr
		case string:
			newMap[k] = vInType
		}
	}

	return newMap, nil
}

// InsertRowStr inserts a row into a table. It expects the toInsert to be of type map[string]interface{}.
func (cl *Client) InsertRowAny(tableName string, toInsert map[string]interface{}) (int64, error) {
	toInsertStr, err := cl.convertInterfaceMapToStringMap(tableName, toInsert)
	if err != nil {
		return 0, ValidationError{err.Error()}
	}

	return cl.InsertRowStr(tableName, toInsertStr)
}

// ParseRow given a TableStruct would convert a map of strings to a map of interfaces.
func (cl *Client) ParseRow(rowStr map[string]string, tableStruct flaarum_shared.TableStruct) (map[string]interface{}, error) {
	fTypeMap := make(map[string]string)
	for _, fd := range tableStruct.Fields {
		fTypeMap[fd.FieldName] = fd.FieldType
	}

	for _, fkd := range tableStruct.ForeignKeys {
		if _, ok := rowStr[fkd.FieldName+"._version"]; !ok {
			continue
		}

		versionInt, err := strconv.ParseInt(rowStr[fkd.FieldName+"._version"], 10, 64)
		if err != nil {
			return nil, err
		}

		otherTableStruct, err := cl.GetTableStructureParsed(fkd.PointedTable, versionInt)
		if err != nil {
			return nil, err
		}

		for _, fd2 := range otherTableStruct.Fields {
			fTypeMap[fkd.FieldName+"."+fd2.FieldName] = fd2.FieldType
		}
	}

	tmpRow := make(map[string]interface{})
	for k, v := range rowStr {
		fieldType, ok := fTypeMap[k]
		if v == "" {
			tmpRow[k] = nil
		} else if ok {
			if fieldType == "text" || fieldType == "string" {
				tmpRow[k] = v
			} else if fieldType == "int" {
				vInt, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("the value '%s' to field '%s' is not of type 'int'", v, k)
				}
				tmpRow[k] = vInt
			} else if fieldType == "bool" {
				if v == "t" {
					tmpRow[k] = true
				} else {
					tmpRow[k] = false
				}
			}

		}
	}

	idInt, err := strconv.ParseInt(rowStr["id"], 10, 64)
	if err != nil {
		return nil, err
	}

	tmpRow["id"] = idInt

	if _, ok := rowStr["_version"]; ok {
		versionInt, err := strconv.ParseInt(rowStr["_version"], 10, 64)
		if err != nil {
			return nil, err
		}
		tmpRow["_version"] = versionInt
	}
	for _, fkd := range tableStruct.ForeignKeys {
		if _, ok := rowStr[fkd.FieldName+"._version"]; ok {
			versionInt, err := strconv.ParseInt(rowStr[fkd.FieldName+"._version"], 10, 64)
			if err != nil {
				return nil, err
			}
			tmpRow[fkd.FieldName+"._version"] = versionInt
		}
	}

	return tmpRow, nil
}

func (cl *Client) Search(stmt string) (*[]map[string]interface{}, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)

	resp, err := httpCl.PostForm(cl.Addr+"search-table/"+cl.ProjName, urlValues)
	if err != nil {
		return nil, ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		rowsStr := make([]map[string]string, 0)
		err = json.Unmarshal(body, &rowsStr)
		if err != nil {
			return nil, ConnError{"json error\n" + err.Error()}
		}

		ret := make([]map[string]interface{}, 0)
		stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
		if err != nil {
			return nil, err
		}

		for _, rowStr := range rowsStr {
			versionInt, err := strconv.ParseInt(rowStr["_version"], 10, 64)
			if err != nil {
				return nil, ConnError{"strconv error\n" + err.Error()}
			}
			versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
			if err != nil {
				return nil, ConnError{err.Error()}
			}
			row, err := cl.ParseRow(rowStr, versionedTableStruct)
			if err != nil {
				return nil, ConnError{err.Error()}
			}
			ret = append(ret, row)
		}

		return &ret, nil
	} else {
		return nil, ServerError{string(body)}
	}
}

func (cl Client) SearchForOne(stmt string) (*map[string]interface{}, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)
	urlValues.Set("query-one", "t")

	resp, err := httpCl.PostForm(cl.Addr+"search-table/"+cl.ProjName, urlValues)
	if err != nil {
		return nil, ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		rowStr := make(map[string]string)
		err = json.Unmarshal(body, &rowStr)
		if err != nil {
			return nil, ConnError{"json error\n" + err.Error()}
		}

		stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
		if err != nil {
			return nil, err
		}

		versionInt, err := strconv.ParseInt(rowStr["_version"], 10, 64)
		if err != nil {
			return nil, ConnError{"strconv error\n" + err.Error()}
		}
		versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
		if err != nil {
			return nil, ConnError{err.Error()}
		}

		row, err := cl.ParseRow(rowStr, versionedTableStruct)
		if err != nil {
			return nil, ConnError{err.Error()}
		}
		return &row, nil
	} else {
		return nil, ServerError{string(body)}
	}
}

func (cl Client) DeleteRows(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sdelete-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return ServerError{string(body)}
	}
}

func (cl Client) CountRows(stmt string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)

	resp, err := httpCl.PostForm(fmt.Sprintf("%scount-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return 0, ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return 0, ConnError{"strconv error\n" + err.Error()}
		}
		return trueR, nil
	} else {
		return 0, ServerError{string(body)}
	}
}

func (cl Client) AllRowsCount(tableName string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sall-rows-count/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return 0, ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return 0, ConnError{"strconv error\n" + err.Error()}
		}
		return trueR, nil
	} else {
		return 0, ServerError{string(body)}
	}
}

// Sums the fields of a row and returns int64 if it is an int field
func (cl Client) SumRows(stmt string) (interface{}, error) {
	urlValues := url.Values{}
	urlValues.Add("stmt", stmt)
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%ssum-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return 0, ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return 0, ConnError{"strconv error\n" + err.Error()}
		}
		return trueR, nil

	} else {
		return 0, ServerError{string(body)}
	}
}

func (cl Client) UpdateRowsStr(stmt string, updateDataStr map[string]string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	keys := make([]string, 0)
	for k := range updateDataStr {
		keys = append(keys, k)
	}

	for i, k := range keys {
		urlValues.Add(fmt.Sprintf("set%d_k", i+1), k)
		urlValues.Add(fmt.Sprintf("set%d_v", i+1), updateDataStr[k])
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%supdate-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return ConnError{err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return ServerError{string(body)}
	}
}

func (cl Client) UpdateRowsAny(stmt string, updateData map[string]interface{}) error {
	stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
	if err != nil {
		return ValidationError{err.Error()}
	}
	updateDataStr, err := cl.convertInterfaceMapToStringMap(stmtStruct.TableName, updateData)
	if err != nil {
		return ValidationError{err.Error()}
	}

	return cl.UpdateRowsStr(stmt, updateDataStr)
}
