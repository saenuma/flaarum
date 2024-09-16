package flaarum

import "fmt"

/*
Common error codes

10: Connection Error
11: Error happended on server
12: Statements Error
20: Validation error
21: Errors relating to unique constraints
22: Errors relating to required constraints
23: Errors relating to foreign keys
*/
type FlError struct {
	Code int
	msg  string
}

func (e FlError) Error() string {
	return fmt.Sprintf("Error Code: %d\n%s", e.Code, e.msg)
}

func retError(code int, msg string) FlError {
	return FlError{Code: code, msg: msg}
}
