package main

import (
	"os/exec"
	"path/filepath"

	"github.com/saenuma/flaarum/flaarum_shared"
)

func main() {
	rootPath, _ := flaarum_shared.GetRootPath()
	keyPath := filepath.Join(rootPath, "https-server.key")
	crtPath := filepath.Join(rootPath, "https-server.crt")

	exec.Command("openssl", "req", "-x509", "-newkey", "rsa:4096", "-keyout", keyPath,
		"-out", crtPath, "-sha256", "-days", "3650", "-nodes", "-subj",
		"/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname").Run()
}
