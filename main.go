package main

import (
	"fmt"
	rcj "github.com/davefinster/rcj-go/api"
)

func main() {
	s := rcj.Server{}
	s.PostgresString = "host=cockroach port=26257 user=rcjgo dbname=rcj sslmode=disable"
	err := s.Start()
	fmt.Println(err)
}
