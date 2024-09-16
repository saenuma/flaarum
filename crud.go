package flaarum

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/saenuma/flaarum/internal"
)

// InsertRowStr inserts a row into a table. It expects the input to be of type map[string]string.
// It returns the id of the newly created row
func (cl *Client) InsertRowStr(tableName string, toInsert map[string]string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	for k, v := range toInsert {
		urlValues.Add(k, v)
	}

	tableStruct, err := cl.GetCurrentTableStructureParsed(tableName)
	if err != nil {
		return -1, err
	}

	fields := make([]string, 0)
	for _, fd := range tableStruct.Fields {
		fields = append(fields, fd.FieldName)
	}

	for k := range toInsert {
		if k == "id" || k == "_version" {
			msg := fmt.Sprintf("The field '%s' would be generated. Please remove.", k)
			return -1, retError(20, msg)
		}

		if !slices.Contains(fields, k) {
			msg := fmt.Sprintf("The field '%s' is not part of this table structure", k)
			return -1, retError(20, msg)
		}

	}

	for _, fd := range tableStruct.Fields {
		v, ok := toInsert[fd.FieldName]

		if ok && v != "" {
			if fd.FieldType == "string" {
				if len(v) > 200 {
					msg := fmt.Sprintf("The value '%s' to field '%s' is longer than 200 characters", v, fd.FieldName)
					return -1, retError(24, msg)
				}
				if strings.Contains(v, "\n") || strings.Contains(v, "\r\n") {
					msg := fmt.Sprintf("The value of field '%s' contains new line.", fd.FieldName)
					return -1, retError(24, msg)
				}
			}

			if fd.FieldType == "int" {

				_, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					msg := fmt.Sprintf("The value '%s' to field '%s' is not of type 'int'", v, fd.FieldName)
					return -1, retError(24, msg)
				}
			}

		}
		if !ok && fd.Required {
			return -1, retError(22, fmt.Sprintf("The field '%s' is required.", fd.FieldName))
		}
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%sinsert-row/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return 0, retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		retId, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if err != nil {
			return 0, retError(11, err.Error())
		}
		return retId, nil
	} else if resp.StatusCode == 400 {
		retStr := string(body)
		if strings.HasPrefix(retStr, "UE:") {
			return -1, retError(21, retStr[3:])
		} else if strings.HasPrefix(retStr, "FKE:") {
			return -1, retError(23, retStr[4:])
		} else {
			return 0, retError(20, string(body))
		}
	} else {
		return 0, retError(11, string(body))
	}
}

func (cl *Client) ConvertInterfaceMapToStringMap(tableName string, oldMap map[string]any) (map[string]string, error) {
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
		case string:
			newMap[k] = vInType
		}
	}

	return newMap, nil
}

// InsertRowStr inserts a row into a table. It expects the toInsert to be of type map[string]any.
func (cl *Client) InsertRowAny(tableName string, toInsert map[string]any) (int64, error) {
	toInsertStr, err := cl.ConvertInterfaceMapToStringMap(tableName, toInsert)
	if err != nil {
		return 0, retError(20, err.Error())
	}

	return cl.InsertRowStr(tableName, toInsertStr)
}

// ParseRow given a TableStruct would convert a map of strings to a map of interfaces.
func (cl *Client) ParseRow(rowStr map[string]string, tableStruct internal.TableStruct) (map[string]any, error) {
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

	tmpRow := make(map[string]any)
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

func (cl *Client) Search(stmt string) (*[]map[string]any, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)

	_, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return nil, retError(12, err.Error())
	}

	resp, err := httpCl.PostForm(cl.Addr+"search-table/"+cl.ProjName, urlValues)
	if err != nil {
		return nil, retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		rowsStr := make([]map[string]string, 0)
		json.Unmarshal(body, &rowsStr)
		ret := make([]map[string]any, 0)
		stmtStruct, err := internal.ParseSearchStmt(stmt)
		if err != nil {
			return nil, err
		}

		for _, rowStr := range rowsStr {
			versionInt, _ := strconv.ParseInt(rowStr["_version"], 10, 64)
			versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
			if err != nil {
				return nil, retError(10, err.Error())
			}
			row, err := cl.ParseRow(rowStr, versionedTableStruct)
			if err != nil {
				return nil, retError(10, err.Error())
			}
			ret = append(ret, row)
		}

		return &ret, nil
	} else {
		return nil, retError(11, string(body))
	}
}

func (cl Client) SearchForOne(stmt string) (*map[string]any, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)
	urlValues.Set("query-one", "t")

	stmtStruct, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return nil, retError(12, err.Error())
	}

	resp, err := httpCl.PostForm(cl.Addr+"search-table/"+cl.ProjName, urlValues)
	if err != nil {
		return nil, retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		rowStr := make(map[string]string)
		json.Unmarshal(body, &rowStr)

		versionInt, _ := strconv.ParseInt(rowStr["_version"], 10, 64)
		versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
		if err != nil {
			return nil, retError(10, err.Error())
		}

		row, err := cl.ParseRow(rowStr, versionedTableStruct)
		if err != nil {
			return nil, retError(10, err.Error())
		}
		return &row, nil
	} else {
		return nil, retError(11, string(body))
	}
}

func (cl Client) DeleteRows(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	_, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return retError(12, err.Error())
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%sdelete-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return retError(11, string(body))
	}
}

func (cl Client) CountRows(stmt string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)
	urlValues.Set("stmt", stmt)

	_, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return -1, retError(12, err.Error())
	}

	resp, err := httpCl.PostForm(fmt.Sprintf("%scount-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return 0, retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, _ := strconv.ParseInt(r, 10, 64)
		return trueR, nil
	} else {
		return 0, retError(11, string(body))
	}
}

func (cl Client) AllRowsCount(tableName string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sall-rows-count/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return 0, retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, _ := strconv.ParseInt(r, 10, 64)
		return trueR, nil
	} else {
		return 0, retError(11, string(body))
	}
}

// Sums the fields of a row and returns int64
func (cl Client) SumRows(stmt string) (any, error) {
	urlValues := url.Values{}
	urlValues.Add("stmt", stmt)
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%ssum-rows/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return 0, retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		r := string(body)
		trueR, _ := strconv.ParseInt(r, 10, 64)
		return trueR, nil

	} else {
		return 0, retError(11, string(body))
	}
}

func (cl Client) UpdateRowsStr(stmt string, updateDataStr map[string]string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	_, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return retError(12, err.Error())
	}

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
		return retError(10, err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return retError(11, string(body))
	}
}

func (cl Client) UpdateRowsAny(stmt string, updateData map[string]any) error {
	stmtStruct, err := internal.ParseSearchStmt(stmt)
	if err != nil {
		return retError(12, err.Error())
	}
	updateDataStr, err := cl.ConvertInterfaceMapToStringMap(stmtStruct.TableName, updateData)
	if err != nil {
		return retError(20, err.Error())
	}

	return cl.UpdateRowsStr(stmt, updateDataStr)
}
