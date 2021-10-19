
build:
	rm -rf artifacts/flaarum.tar.xz
	go build -o artifacts/astest ./astest
	go build -o artifacts/cli ./cli
	go build -o artifacts/gcpasr ./gcpasr
	go build -o artifacts/inout ./inout
	go build -o artifacts/lgcp ./lgcp
	go build -o artifacts/prod ./prod
	go build -o artifacts/store ./store
	go build -o artifacts/statsr ./statsr
	go build -o artifacts/tindexer ./tindexer

	cp store/https-server.crt artifacts/https-server.crt
	cp store/https-server.key artifacts/https-server.key
	cp english-stopwords.json artifacts/english-stopwords.json

pkg:
	snapcraft clean all-needed-files
	snapcraft --debug
