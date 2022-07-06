@echo on

go build -o wbin\flcli.exe ./cli
go build -o wbin\flprod.exe ./prod
go build -ldflags="-H windowsgui" -o wbin\flstore.exe ./store
