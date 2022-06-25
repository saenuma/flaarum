# flaarum

![Flaarum Logo](https://github.com/saenuma/flaarum/raw/master/flaarum-logo.png "Flaarum logo")

[![GoDoc](https://godoc.org/github.com/saenuma/flaarum?status.svg)](https://godoc.org/github.com/saenuma/flaarum)

A database that enforces structures and features its own query language.


## Why a new database.

1.	Every data stored in this database is **automatically indexed**. Text fields are indexed with full text search indexes.

2.	**Table Expansion** (a more comfortable form of joins)

3.	Can make any **changes to any table's structure** even though you have data in the table.


## Regular Features

1.  Supports its own query language.


## Technologies Used.

* Golang
* Ubuntu
* HTTPS
* JSON


## Documentation

The tutorial pages is located at [saenuma.com](https://sae.ng/flaarumtuts/intro).

API documentation can be found on [godoc](https://pkg.go.dev/github.com/saenuma/flaarum)


## Development Environment Installation.

1.	Install wsl on windows. Search on google for instructions

1.  Launch wsl and run `wget https://sae.ng/install_flaarum.sh`

1.  Execute the downloaded script by running `sudo ./install_flaarum.sh`

1.	You don't need a key to connect with the database when not in production.


### Production Environment Installation

1. 	Launch a server and make sure it has a static internal address.

1.  Download install script by running `wget https://sae.ng/install_flaarum.sh`

1.  Execute the downloaded script by running `sudo ./install_flaarum.sh`

1.  Make production ready by running `sudo flprod mpr`

1.	SSH into the server and run `sudo flprod r` to get your key string. Needed in your program to connect to your flaarum server.


## CLI

You can use the cli `flcli` to administer the database from the terminal.

Run it with help `flcli help` to view available options.

You must SSH into the server to use the `flcli` command.

## License

Released with the MIT License
