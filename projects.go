package flaarum

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"encoding/json"
	"net/url"
)


func (cl *Client) CreateProject(projName string) error {
	resp, err := httpCl.Get( cl.Addr + "create-project/" + projName)
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


func (cl *Client) DeleteProject(projName string) error {
	resp, err := httpCl.Get( cl.Addr + "delete-project/" + projName)
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


func (cl *Client) ListProjects() ([]string, error) {
	resp, err := httpCl.Get( cl.Addr + "list-projects")
	if err != nil {
		return []string{}, errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{}, errors.Wrap(err, "ioutil error)")
	}

	if resp.StatusCode == 200 {
		ret := make([]string, 0)
		err := json.Unmarshal(body, &ret)
		if err != nil {
			return []string{}, errors.Wrap(err, "json error")
		}
		return ret, nil
	} else {
		return []string{}, errors.New(string(body))
	}	
}


func (cl *Client) RenameProject(projName, newProjName string) error {
  urlValues := url.Values{}
  urlValues.Set("keyStr", cl.KeyStr)

  resp, err := httpCl.PostForm(cl.Addr + "rename-project/" + projName + "/" + newProjName,
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
