package flaarum

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/saenuma/flaarum/flaarum_shared"
)

func (cl *Client) CreateTable(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm(cl.Addr+"create-table/"+cl.ProjName, urlValues)
	if err != nil {
		return ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ConnError{"ioutil error\n" + err.Error()}
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return ServerError{string(body)}
	}
}

func (cl *Client) UpdateTableStructure(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm(cl.Addr+"update-table-structure/"+cl.ProjName, urlValues)
	if err != nil {
		return ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ConnError{"ioutil error\n" + err.Error()}
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return ServerError{string(body)}
	}
}

func (cl *Client) CreateOrUpdateTable(stmt string) error {
	tables, err := cl.ListTables()
	if err != nil {
		return err
	}

	tableStruct, err := flaarum_shared.ParseTableStructureStmt(stmt)
	if err != nil {
		return err
	}
	if flaarum_shared.FindIn(tables, tableStruct.TableName) == -1 {
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

		if oldStmt != flaarum_shared.FormatTableStruct(tableStruct) {
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
		return -1, ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		retId, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return -1, ConnError{"strconv error\n" + err.Error()}
		}
		return retId, nil
	} else {
		return -1, ServerError{string(body)}
	}
}

func (cl *Client) GetTableStructure(tableName string, versionNum int64) (string, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sget-table-structure/%s/%s/%d", cl.Addr, cl.ProjName, tableName, versionNum),
		urlValues)
	if err != nil {
		return "", ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		return string(body), nil
	} else {
		return "", ServerError{string(body)}
	}
}

func (cl *Client) GetTableStructureParsed(tableName string, versionNum int64) (flaarum_shared.TableStruct, error) {
	stmt, err := cl.GetTableStructure(tableName, versionNum)
	if err != nil {
		return flaarum_shared.TableStruct{}, err
	}
	return flaarum_shared.ParseTableStructureStmt(stmt)
}

func (cl *Client) GetCurrentTableStructureParsed(tableName string) (flaarum_shared.TableStruct, error) {
	currentVersionNum, err := cl.GetCurrentTableVersionNum(tableName)
	if err != nil {
		return flaarum_shared.TableStruct{}, err
	}
	stmt, err := cl.GetTableStructure(tableName, currentVersionNum)
	if err != nil {
		return flaarum_shared.TableStruct{}, err
	}
	return flaarum_shared.ParseTableStructureStmt(stmt)
}

func (cl Client) ListTables() ([]string, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%slist-tables/%s", cl.Addr, cl.ProjName), urlValues)
	if err != nil {
		return nil, ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ConnError{err.Error()}
	}

	if resp.StatusCode == 200 {
		tables := make([]string, 0)
		err = json.Unmarshal(body, &tables)
		if err != nil {
			return nil, ConnError{"json error\n" + err.Error()}
		}
		return tables, nil
	} else {
		return nil, ServerError{string(body)}
	}

}

func (cl *Client) DeleteTable(tableName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"delete-table/"+cl.ProjName+"/"+tableName, urlValues)
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

// TrimTable is needed because deletion and update of rows are not deep but fast.
// Should be ran after a while on a table that have used lots of deletions and updates
// TrimTable could also be called FullDelete and FullUpdate
func (cl *Client) TrimTable(tableName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"trim-table/"+cl.ProjName+"/"+tableName, urlValues)
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
