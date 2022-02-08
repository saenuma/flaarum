package flaarum

import (
	"io"
	"encoding/json"
	"net/url"
)


func (cl *Client) CreateProject(projName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm( cl.Addr + "create-project/" + projName, urlValues)
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


func (cl *Client) DeleteProject(projName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm( cl.Addr + "delete-project/" + projName, urlValues)
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


func (cl *Client) ListProjects() ([]string, error) {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm( cl.Addr + "list-projects", urlValues)
	if err != nil {
		return []string{}, ConnError{err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return []string{}, ConnError{"ioutil error\n" + err.Error()}
	}

	if resp.StatusCode == 200 {
		ret := make([]string, 0)
		err := json.Unmarshal(body, &ret)
		if err != nil {
			return []string{}, ConnError{"json error\n" + err.Error()}
		}
		return ret, nil
	} else {
		return []string{}, ServerError{string(body)}
	}
}


func (cl *Client) RenameProject(projName, newProjName string) error {
  urlValues := url.Values{}
  urlValues.Set("key-str", cl.KeyStr)

  resp, err := httpCl.PostForm(cl.Addr + "rename-project/" + projName + "/" + newProjName,
    urlValues)
  if err != nil {
    return ConnError{err.Error()}
  }
  defer resp.Body.Close()
  body, err :=  io.ReadAll(resp.Body)
  if err != nil {
    return ConnError{err.Error()}
  }

  if resp.StatusCode == 200 {
    return nil
  } else {
    return ServerError{string(body)}
  }
}
