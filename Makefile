
build:
	rm -rf bin/
	rm -rf flaarum.tar.xz
	
	go build -o bin/flcli ./cli
	go build -o bin/fldaemon ./daemon
	go build -o bin/flprogs ./progs
	go build -o bin/flprod ./prod
	go build -o bin/flstore ./store

	cp store/https-server.crt bin/https-server.crt
	cp store/https-server.key bin/https-server.key

	cp services/* bin/

	tar -cJf flaarum.tar.xz bin/*
