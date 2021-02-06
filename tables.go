package flaarum

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"net/url"
	"fmt"
	"strconv"
	"github.com/bankole7782/flaarum/flaarum_shared"
	"encoding/json"
)


func (cl *Client) CreateTable(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm( cl.Addr + "create-table/" + cl.ProjName, urlValues)
	if err != nil {
		return errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "ioutil error)")
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return errors.New(string(body))
	}
}


func (cl *Client) UpdateTableStructure(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)
	urlValues.Add("stmt", stmt)

	resp, err := httpCl.PostForm( cl.Addr + "update-table-structure/" + cl.ProjName, urlValues)
	if err != nil {
		return errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "ioutil error)")
	}

	if resp.StatusCode == 200 {
		return nil
	} else {
		return errors.New(string(body))
	}
}


func (cl *Client) GetCurrentTableVersionNum(tableName string) (int64, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sget-current-version-num/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
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


func (cl *Client) GetTableStructure(tableName string, versionNum int64) (string, error) {
	urlValues := url.Values{}
	urlValues.Add("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(fmt.Sprintf("%sget-table-structure/%s/%s/%d", cl.Addr, cl.ProjName, tableName, versionNum),
		urlValues)
	if err != nil {
		return "", errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "ioutil error")
	}

	if resp.StatusCode == 200 {
		return string(body), nil
	} else {
		return "", errors.New(string(body))
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


func (cl *Client) EmptyTable(tableName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

  resp, err := httpCl.PostForm(cl.Addr + "empty-table/" + cl.ProjName + "/" + tableName,
    urlValues)
  if err != nil {
    return errors.Wrap(err, "error contacting site")
  }
  defer resp.Body.Close()
  body, err :=  ioutil.ReadAll(resp.Body)
  if err != nil {
    return errors.Wrap(err, "ioutil error")
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return errors.New(string(body))
  }
}


func (cl Client) ListTables() ([]string, error) {
  urlValues := url.Values{}
  urlValues.Add("key-str", cl.KeyStr)

  resp, err := httpCl.PostForm(fmt.Sprintf("%slist-tables/%s", cl.Addr, cl.ProjName), urlValues)
  if err != nil {
    return nil, errors.Wrap(err, "error contacting site")
  }
  defer resp.Body.Close()
  body, err :=  ioutil.ReadAll(resp.Body)
  if err != nil {
    return nil, errors.Wrap(err, "ioutil error")
  }

  if resp.StatusCode == 200 {
    tables := make([]string, 0)
    err = json.Unmarshal(body, &tables)
    if err != nil {
      return nil, errors.Wrap(err, "json error.")
    }
    return tables, nil
  } else {
    return nil, errors.New(string(body))
  }

}


func (cl *Client) RenameTable(tableName, newTableName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

  resp, err := httpCl.PostForm(cl.Addr + "rename-table/" + cl.ProjName + "/" + tableName + "/" + newTableName,
    urlValues)
  if err != nil {
    return errors.Wrap(err, "error contacting site")
  }
  defer resp.Body.Close()
  body, err :=  ioutil.ReadAll(resp.Body)
  if err != nil {
    return errors.Wrap(err, "ioutil error")
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return errors.New(string(body))
  }
}


func (cl *Client) DeleteTable(tableName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

  resp, err := httpCl.PostForm(cl.Addr + "delete-table/" + cl.ProjName + "/" + tableName , urlValues)
  if err != nil {
    return errors.Wrap(err, "error contacting site")
  }
  defer resp.Body.Close()
  body, err :=  ioutil.ReadAll(resp.Body)
  if err != nil {
    return errors.Wrap(err, "ioutil error")
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return errors.New(string(body))
  }
}
