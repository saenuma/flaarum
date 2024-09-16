package flaarum

import (
	"encoding/json"
	"io"
	"net/url"
)

func (cl *Client) CreateProject(projName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"create-project/"+projName, urlValues)
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

func (cl *Client) DeleteProject(projName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"delete-project/"+projName, urlValues)
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

func (cl *Client) ListProjects() ([]string, error) {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"list-projects", urlValues)
	if err != nil {
		return []string{}, retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return []string{}, retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		ret := make([]string, 0)
		json.Unmarshal(body, &ret)
		return ret, nil
	} else {
		return []string{}, retError(11, string(body))
	}
}

func (cl *Client) RenameProject(projName, newProjName string) error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"rename-project/"+projName+"/"+newProjName,
		urlValues)
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
