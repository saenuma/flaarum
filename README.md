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

2.	Full text Search.


## Technologies Used.

* Golang
* Ubuntu
* HTTPS
* JSON


## Documentation

The tutorial pages is located at [saenuma.com](https://saenuma.com/flaarumtuts/intro).

API documentation can be found on [godoc](https://godoc.org/github.com/saenuma/flaarum)


## Install

1.	Install from snapstore using `sudo snap install flaarum`

1.	You don't need a key to connect with the database when not in production.


### Basic Production Setup (Google Cloud)

1.	Create a launch file with `flaarum.lgcp initb` and edit it to your specifications

2.	Create a service account and store the downloaded json in your flaarum folder (gotten from `flaarum.cli pwd`)

3.	If you have a project that you've not launched an instance in before. Enable compute api with this command
		`gcloud services enable compute.googleapis.com --project {project}`.Remember to replace the project with you project name.

4.	Launch the service with `flaarum.lgcp lb launchfile serviceaccountfile`
    where launchfile is the name of the file created in step 1 excluding the path and serviceaccountfile is the json in step 2
    excluding the path.

5.	SSH into the server and run `flaarum.prod r` to get your key string. Needed in your program to connect to your flaarum server.


### AutoScaling Production Setup (Google Cloud)

1.	Create a launch file with `flaarum.lgcp initas` and edit it to your specifications

2.	Create a service account and store the downloaded json in your flaarum folder (gotten from `flaarum.cli pwd`)

3.	If you have a project that you've not launched an instance in before. Enable compute api with this command
		`gcloud services enable compute.googleapis.com --project {project}`.Remember to replace the project with you project name.

4.	Launch the service with `flaarum.lgcp las launchfile serviceaccountfile`
    where launchfile is the name of the file created in step 1 excluding the path and serviceaccountfile is the json in step 2
    excluding the path.

5.	SSH into the server (not the control server) and run `flaarum.prod r` to get your key string. Needed in your program to connect to your flaarum server.


## CLI

You can use the cli `flaarum.cli` to administer the database from the terminal.

Run it with help `flaarum.cli help` to view available options.


## License

Released with the MIT License
