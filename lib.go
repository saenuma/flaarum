// this package 'flaarum' is the golang library for communicating with the flaarum server.
package flaarum

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

var httpCl *http.Client

func init() {
	config := &tls.Config{InsecureSkipVerify: true}
	tr := &http.Transport{TLSClientConfig: config}

	httpCl = &http.Client{Transport: tr}
}

type Client struct {
	Addr     string
	KeyStr   string
	ProjName string
}

func NewClient(ip, keyStr, projName string) Client {
	return Client{"https://" + ip + ":22318/", keyStr, projName}
}

// Used whenever you changed the default port
func NewClientCustomPort(ip, keyStr, projName string, port int) Client {
	return Client{"https://" + ip + fmt.Sprintf(":%d/", port), keyStr, projName}
}

func (cl *Client) Ping() error {
	urlValues := url.Values{}
	urlValues.Set("key-str", cl.KeyStr)

	resp, err := httpCl.PostForm(cl.Addr+"is-flaarum", urlValues)
	if err != nil {
		return retError(10, err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return retError(10, err.Error())
	}

	if resp.StatusCode == 200 {
		if string(body) == "yeah-flaarum" {
			return nil
		} else {
			return retError(10, "Unexpected Error in confirming that the server is a flaarum store.")
		}
	} else {
		return retError(10, string(body))
	}
}
