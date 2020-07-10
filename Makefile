
build:
	rm -rf artifacts/flaarum.tar.xz
	go build -o artifacts/store ./flaarum_store
	go build -o artifacts/prod ./flaarum_prod
	go build -o artifacts/cli ./flaarum_cli
	go build -o artifacts/tindexer ./flaarum_tindexer
	go build -o artifacts/launcher ./flaarum_launcher	

	cp flaarum.json artifacts/flaarum.json
	cp flaarum_store/https-server.crt artifacts/https-server.crt
	cp flaarum_store/https-server.key artifacts/https-server.key
	cp english-stopwords.json artifacts/english-stopwords.json
	cp startup_script.sh artifacts/startup_script.sh

create-snap:
	snapcraft clean all-needed-files
	snapcraft --debug