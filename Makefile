
build:
	rm -rf artifacts/flaarum.tar.xz
	go build -o artifacts/store ./flaarum_store
	go build -o artifacts/mkpass ./flaarum_mkpass
	cp flaarum.json artifacts/flaarum.json
	cp flaarum_store/https-server.crt artifacts/https-server.crt
	cp flaarum_store/https-server.key artifacts/https-server.key
