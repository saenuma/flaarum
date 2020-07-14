
build:
	rm -rf artifacts/flaarum.tar.xz
	go build -o artifacts/store ./store
	go build -o artifacts/prod ./prod
	go build -o artifacts/cli ./cli
	go build -o artifacts/tindexer ./tindexer

	cp flaarum.json artifacts/flaarum.json
	cp store/https-server.crt artifacts/https-server.crt
	cp store/https-server.key artifacts/https-server.key
	cp english-stopwords.json artifacts/english-stopwords.json

create-snap:
	snapcraft clean all-needed-files
	snapcraft --debug