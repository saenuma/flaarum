// this package 'flaarum' is the golang library for communicating with the flaarum server.
package flaarum

import (
	"net/http"
	"crypto/tls"
	"github.com/pkg/errors"
	"strings"
	"io/ioutil"
)


var httpCl *http.Client

func init() {
	config := &tls.Config { InsecureSkipVerify: true}
	tr := &http.Transport{TLSClientConfig: config}

	httpCl = &http.Client{Transport: tr}
}

type Client struct {
	Addr string
	KeyStr string
	ProjName string
}

func NewClient(addr, keyStr, projName string) Client {
	if ! strings.HasSuffix(addr, "/") {
		addr += "/"
	}

	return Client{addr, keyStr, projName}
}


func (cl *Client) Ping() error {
	resp, err := httpCl.Get(cl.Addr + "is-flaarum")
	if err != nil {
		return errors.Wrap(err, "http error")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if resp.StatusCode == 200 {
		if string(body) == "yeah-flaarum" {
			return nil
		} else {
			return errors.New("Unexpected Error in confirming that the server is a flaarum store.")
		}
	} else {
		return errors.New(string(body))
	}
}
