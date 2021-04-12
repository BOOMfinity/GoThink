package database

import (
	"errors"
	"flag"
	"fmt"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
	"io"
	"log"
	"os"
	"strings"
)

var (
	Host = flag.String("host", "localhost", "RethinkDB address")
	Port = flag.Uint("port", 28015, "RethinkDB client port")
	PasswordFilePath = flag.String("password-file", "", "Path to the file with password")
	Password = flag.String("password", "", "Enter your password to the RethinkDB (admin user)")
)

type Connection struct {
	DB *rethinkdb.Session
}

func NewConnection() (conn *Connection, err error) {
	log.Println("Checking config...")
	if Host == nil || *Host == "" {
		return nil, errors.New("Host address cannot be empty!")
	}
	if Port == nil || *Port == 0 {
		return nil, errors.New("RethinkDB client port cannot be empty!")
	}
	if PasswordFilePath != nil && *PasswordFilePath != "" {
		if PasswordFile, err := os.Open(*PasswordFilePath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.New("Password file not found!")
			} else {
				return nil, err
			}
		} else {
			data, err := io.ReadAll(PasswordFile)
			if err != nil {
				return nil, err
			}
			*Password = string(data)
		}
	}
	println()
	log.Printf("Host: %v", *Host)
	log.Printf("Port: %v", *Port)
	if strings.Count(*Password, "")-1 > 0 {
		log.Printf("Password: %v", strings.Repeat("*", strings.Count(*Password, "")-1))
	} else {
		log.Printf("Password: (not set)")
	}
	println()
	log.Println("Connecting...")
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts {
		Password: *Password,
		Address: fmt.Sprintf("%v:%v", *Host, *Port),
		Username: "admin",
	})
	if err != nil {
		return
	}
	log.Println("Successfully connected to RethinkDB")
	conn = new(Connection)
	conn.DB = session
	return
}
