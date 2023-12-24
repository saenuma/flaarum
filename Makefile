
build:
	rm -rf bin/

	go build -o bin/flcli ./cli
	go build -o bin/flprod ./prod
	go build -o bin/flstore ./store
	go build -o bin/fltasks ./tasks