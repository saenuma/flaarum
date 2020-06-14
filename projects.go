package flaarum

import (
	"github.com/pkg/errors"
	"io/ioutil"
)


func (cl *Client) CreateProject(projName string) error {
	resp, err := httpCl.Get( cl.Addr + "create-project/" + projName)
	if err != nil {
		return errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		return nil
	} else {
		return errors.New(string(body))
	}
}

