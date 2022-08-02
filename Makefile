
build:
	rm -rf bin/

	go build -o bin/flcli ./cli
	go build -o bin/flprod ./prod
	go build -o bin/flstore ./store

	cp store/https-server.crt bin/https-server.crt
	cp store/https-server.key bin/https-server.key
