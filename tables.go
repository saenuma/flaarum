package flaarum

import (
	"io/ioutil"
	"github.com/pkg/errors"
	"net/url"
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
