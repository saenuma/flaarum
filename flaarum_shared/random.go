package flaarum_shared

import (
  "math/rand"
  "time"
  crand "crypto/rand"
  "math/big"
  "fmt"
)



const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func UntestedRandomString(length int) string {
  var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

  b := make([]byte, length)
  for i := range b {
    b[i] = letters[seededRand.Intn(len(letters))]
  }
  return string(b)
}


func innerGenerateSecureRandomString(n int) (string, error) {
  ret := make([]byte, n)
  for i := 0; i < n; i++ {
    num, err := crand.Int(crand.Reader, big.NewInt(int64(len(letters))))
    if err != nil {
      return "", err
    }
    ret[i] = letters[num.Int64()]
  }

  return string(ret), nil
}


func GenerateSecureRandomString(n int) string {
  ret, err := innerGenerateSecureRandomString(n)
  if err != nil {
    fmt.Println(err)
    fmt.Println("Switching to less secure method to generate keystr (a random string)")
    ret = UntestedRandomString(n)
  }
  return ret
}
