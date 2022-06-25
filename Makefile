
build:
	rm -rf bin/
	go build -o bin/flcli ./cli
	go build -o bin/fldaemon ./daemon
	go build -o bin/flgcpasr ./gcpasr
	go build -o bin/flprogs ./progs
	go build -o bin/fllgcp ./lgcp
	go build -o bin/flprod ./prod
	go build -o bin/flstore ./store
	go build -o bin/flstatsr ./statsr

	cp store/https-server.crt bin/https-server.crt
	cp store/https-server.key bin/https-server.key
	cp english-stopwords.json bin/english-stopwords.json

	cp services/* bin/
