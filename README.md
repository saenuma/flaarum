# flaarum

![Flaarum Logo](https://github.com/bankole7782/flaarum/raw/master/flaarum-logo.png "Flaarum logo")

[![GoDoc](https://godoc.org/github.com/bankole7782/flaarum?status.svg)](https://godoc.org/github.com/bankole7782/flaarum)

A database that enforces structures and features its own query language.


## Why a new database.

1.	Every data stored in this database is **automatically indexed**. Text fields are indexed with full text search indexes.

2.	**Table Expansion** (a more comfortable form of joins)

3.	Can make any **changes to any table's structure** even though you have data in the table.


## Regular Features

1.  Supports its own query language.

2.	Full text Search.


## Technologies Used.

* Golang
* Ubuntu
* HTTPS
* JSON


## Documentation

The tutorial pages is located at [pandolee.com](https://pandolee.com/flaarumtuts/intro).

API documentation can be found on [godoc](https://godoc.org/github.com/bankole7782/flaarum)


## Install

1.	Install from snapstore using `sudo snap install flaarum`

2.	Start the project with the command `sudo snap start flaarum.store`

3.	Start the text indexer with the command `sudo snap start flaarum.tindexer`

4.	You don't need a key to connect with the database when not in production.


### Production Setup

1.	Edit the config file found in `/var/snap/flaarum/current/flaarum.json` and set `in_production` to true

2.	Mount the persistent disk in the path `/var/snap/flaarum/current/data`

3.	Run `sudo flaarum.keyp c` to get the key that would be supplied in any database request.

4.	Use the key when creating `flaarum.Client` in your programs.


## CLI

You can use the cli `flaarum.cli` to administer the database from the terminal. 

Run it with help `flaarum.cli help` to view available options.


## License

Released with the MIT License