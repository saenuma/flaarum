
build:
	rm -rf artifacts/flaarum.tar.xz
	go build -o artifacts/store ./flaarum_store
	go build -o artifacts/mkpass ./flaarum_mkpass
	go build -o artifacts/cli ./flaarum_cli

	cp flaarum.json artifacts/flaarum.json
	cp flaarum_store/https-server.crt artifacts/https-server.crt
	cp flaarum_store/https-server.key artifacts/https-server.key

create-snap:
	snapcraft clean all-needed-files
	snapcraft --debug