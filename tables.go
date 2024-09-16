package flaarum

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/saenuma/flaarum/internal"
)

func (cl *Client) CreateTable(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm(cl.Addr+"create-table/"+cl.ProjName, urlValues)
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

func (cl *Client) UpdateTableStructure(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm(cl.Addr+"update-table-structure/"+cl.ProjName, urlValues)
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

func (cl *Client) CreateOrUpdateTable(stmt string) error {
	tables, err := cl.ListTables()
	if err != nil {
		return err
	}

	tableStruct, err := internal.ParseTableStructureStmt(stmt)
	if err != nil {
		return err
	}
	if internal.FindIn(tables, tableStruct.TableName) == -1 {
		// table doesn't exist
		err = cl.CreateTable(stmt)
		if err != nil {
			return err
		}
	} else {
		// table exists check if it needs update
		currentVersionNum, err := cl.GetCurrentTableVersionNum(tableStruct.TableName)
		if err != nil {
			return err
		}

		oldStmt, err := cl.GetTableStructure(tableStruct.TableName, currentVersionNum)
		if err != nil {
			return err
		}

		if oldStmt != internal.FormatTableStruct(tableStruct) {
			err = cl.UpdateTableStructure(stmt)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (cl *Client) GetCurrentTableVersionNum(tableName string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sget-current-version-num/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
	if err != nil {
		return -1, retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		retId, _ := strconv.ParseInt(string(body), 10, 64)
		return retId, nil
	} else {
		return -1, retError(11, string(body))
	}
}

func (cl *Client) GetTableStructure(tableName string, versionNum int64) (string, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sget-table-structure/%s/%s/%d", cl.Addr, cl.ProjName, tableName, versionNum),
		urlValues)
	if err != nil {
		return "", retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		return string(body), nil
	} else {
		return "", retError(11, string(body))
	}
}

func (cl *Client) GetTableStructureParsed(tableName string, versionNum int64) (internal.TableStruct, error) {
	stmt, err := cl.GetTableStructure(tableName, versionNum)
	if err != nil {
		return internal.TableStruct{}, err
	}
	return internal.ParseTableStructureStmt(stmt)
}

func (cl *Client) GetCurrentTableStructureParsed(tableName string) (internal.TableStruct, error) {
	currentVersionNum, err := cl.GetCurrentTableVersionNum(tableName)
	if err != nil {
		return internal.TableStruct{}, err
	}
	stmt, err := cl.GetTableStructure(tableName, currentVersionNum)
	if err != nil {
		return internal.TableStruct{}, err
	}
	return internal.ParseTableStructureStmt(stmt)
}

func (cl Client) ListTables() ([]string, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%slist-tables/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return nil, retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		tables := make([]string, 0)
		json.Unmarshal(body, &tables)
		return tables, nil
	} else {
		return nil, retError(11, string(body))
	}

}

func (cl *Client) DeleteTable(tableName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"delete-table/"+cl.ProjName+"/"+tableName, urlValues)
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
