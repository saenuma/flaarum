package main

import (
  _ "embed"
)

//go:embed version.txt
var currentVersionStr string
