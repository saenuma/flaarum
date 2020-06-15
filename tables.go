package flaarum

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"net/url"
	"fmt"
	"strconv"
	"github.com/bankole7782/flaarum/flaarum_shared"
)


func (cl *Client) CreateTable(stmt string) error {
	urlValues := url.Values{}
	urlValues.Add("keyStr", cl.KeyStr)
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
	urlValues.Add("keyStr", cl.KeyStr)
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
	urlValues.Add("keyStr", cl.KeyStr)
	
	resp, err := httpCl.PostForm(fmt.Sprintf("%s/get-current-version-num/%s/%s", cl.Addr, cl.ProjName, tableName), urlValues)
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
	urlValues.Add("keyStr", cl.KeyStr)
	
	resp, err := httpCl.PostForm(fmt.Sprintf("%s/get-table-structure/%s/%s/%d", cl.Addr, cl.ProjName, tableName, versionNum), 
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