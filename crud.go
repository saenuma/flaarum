package flaarum

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"time"
	"fmt"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"encoding/json"

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


// ParseRow given a TableStruct would convert a map of strings to a map of interfaces.
func (cl *Client) ParseRow (rowStr map[string]string, tableStruct flaarum_shared.TableStruct) (map[string]interface{}, error) {
  fTypeMap := make(map[string]string)
  for _, fd := range tableStruct.Fields {
    fTypeMap[fd.FieldName] = fd.FieldType
  }

  for _, fkd := range tableStruct.ForeignKeys {
    if _, ok := rowStr[fkd.FieldName + "._version"]; ! ok {
      continue
    }
    
    versionInt, err := strconv.ParseInt(rowStr[fkd.FieldName + "_version"], 10, 64)
  	if err != nil {
  		return nil, errors.Wrap(err, "strconv error")
  	}
    otherTableStruct, err := cl.GetTableStructureParsed(tableStruct.TableName, versionInt)
    if err != nil {
      return nil, err
    }

    for _, fd2 := range otherTableStruct.Fields {
      fTypeMap[fkd.FieldName + "." + fd2.FieldName] = fd2.FieldType
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
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'int'", v, k))
        }
        tmpRow[k] = vInt
      } else if fieldType == "float" {
        vFloat, err := strconv.ParseFloat(v, 64)
        if err != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'float'", v, k))
        }
        tmpRow[k] = vFloat
      } else if fieldType == "bool" {
        if v == "t" {
          tmpRow[k] = true
        } else {
          tmpRow[k] = false
        }
      } else if fieldType == "date" {
        vTime1, err1 := time.Parse(flaarum_shared.BROWSER_DATE_FORMAT, v)
        if err1 != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in date format.", v, k))
        }
        tmpRow[k] = vTime1
      } else if fieldType == "datetime" {
        vTime1, err1 := time.Parse(flaarum_shared.BROWSER_DATETIME_FORMAT, v)
        if err1 != nil {
          return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not in datetime format.", v, k))
        }
        tmpRow[k] = vTime1
      }

    }
  }

  if _, ok := rowStr["id"]; ok {
    vInt, err := strconv.ParseInt(rowStr["id"], 10, 64)
    if err != nil {
      return nil, errors.New(fmt.Sprintf("The value '%s' to field '%s' is not of type 'int'", rowStr["id"], "id"))
    }
    tmpRow["id"] = vInt
  }
  if _, ok := rowStr["_version"]; ok {
    tmpRow["_version"], _ = strconv.ParseInt(rowStr["_version"], 10, 64)
  }
  for _, fkd := range tableStruct.ForeignKeys {
    if _, ok := rowStr[fkd.FieldName + "._version"]; ok {
      tmpRow[fkd.FieldName + "._version"], _ = strconv.ParseInt(rowStr[fkd.FieldName + "._version"], 10, 64)
    }
  }

  return tmpRow, nil
}


func (cl *Client) Search (stmt string) (*[]map[string]interface{}, error) {
  urlValues := url.Values{}
  urlValues.Set("keyStr", cl.KeyStr)
  urlValues.Set("stmt", stmt)

  resp, err := httpCl.PostForm(cl.Addr + "search-table/" + cl.ProjName, urlValues)
  if err != nil {
    return nil, errors.Wrap(err, "server read failed.")
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil, errors.Wrap(err, "ioutil read failed.")
  }

  if resp.StatusCode == 200 {
    rowsStr := make([]map[string]string, 0)
    err = json.Unmarshal(body, &rowsStr)
    if err != nil {
      return nil, errors.Wrap(err, "json error.")
    }

    ret := make([]map[string]interface{}, 0)
    stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
    if err != nil {
      return nil, err
    }

    for _, rowStr := range rowsStr {
    	versionInt, err := strconv.ParseInt(rowStr["_version"], 10, 64)
    	if err != nil {
    		return nil, errors.Wrap(err, "strconv error")
    	}
      versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
      if err != nil {
        return nil, err
      }

      row, err := cl.ParseRow(rowStr, versionedTableStruct)
      if err != nil {
        return nil, err
      }
      ret = append(ret, row)
    }

    return &ret, nil
  } else {
    return nil, errors.New(string(body))
  }
}


func (cl Client) SearchForOne(stmt string) (*map[string]interface{}, error) {
  urlValues := url.Values{}
  urlValues.Set("keyStr", cl.KeyStr)
  urlValues.Set("stmt", stmt)
  urlValues.Set("query-one", "t")

  resp, err := httpCl.PostForm(cl.Addr + "search-table/" + cl.ProjName, urlValues)
  if err != nil {
    return nil, errors.Wrap(err, "server read failed.")
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil, errors.Wrap(err, "ioutil read failed.")
  }

  if resp.StatusCode == 200 {
    rowStr := make(map[string]string)
    err = json.Unmarshal(body, &rowStr)
    if err != nil {
      return nil, errors.Wrap(err, "json error")
    }

    stmtStruct, err := flaarum_shared.ParseSearchStmt(stmt)
    if err != nil {
      return nil, err
    }

  	versionInt, err := strconv.ParseInt(rowStr["_version"], 10, 64)
  	if err != nil {
  		return nil, errors.Wrap(err, "strconv error")
  	}
    versionedTableStruct, err := cl.GetTableStructureParsed(stmtStruct.TableName, versionInt)
    if err != nil {
      return nil, err
    }

    row, err := cl.ParseRow(rowStr, versionedTableStruct)
    if err != nil {
      return nil, err
    }
    return &row, nil
  } else {
    return nil, errors.New(string(body))
  }
}


func (cl Client) DeleteRows(stmt string) error {
  urlValues := url.Values{}
  urlValues.Add("keyStr", cl.KeyStr)
  urlValues.Add("stmt", stmt)

  resp, err := httpCl.PostForm(fmt.Sprintf("%sdelete-rows/%s", cl.Addr, cl.ProjName), urlValues)
  if err != nil {
    return errors.Wrap(err, "server read failed.")
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return errors.Wrap(err, "ioutil read failed.")
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return errors.New(string(body))
  }
}


func (cl Client) DeleteFields(stmt string, toDeleteFields []string) error {
  urlValues := url.Values{}
  urlValues.Add("keyStr", cl.KeyStr)
  urlValues.Add("stmt", stmt)
  for i, f := range toDeleteFields {
    urlValues.Add(fmt.Sprintf("to_delete_field%d", i+1), f)
  }

  resp, err := httpCl.PostForm(fmt.Sprintf("%sdelete-fields/%s", cl.Addr, cl.ProjName), urlValues)
  if err != nil {
    return errors.Wrap(err, "server read failed.")
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return errors.Wrap(err, "ioutil read failed.")
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return errors.New(string(body))
  }
}