package database

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/BOOMfinity-Developers/GoThink"
	"github.com/jessevdk/go-flags"
	"gopkg.in/rethinkdb/rethinkdb-go.v6"
)

var (
	//Host = flag.String("host", "localhost", "RethinkDB address")
	//Port = flag.Uint("port", 28015, "RethinkDB client port")
	//PasswordFilePath = flag.String("password-file", "", "Path to the file with password")
	//Password = flag.String("password", "", "Enter your password to the RethinkDB (admin user)")
	options GoThink.DatabaseFlags
)

type Connection struct {
	DB *rethinkdb.Session
}

func AddFlags(parser *flags.Parser) {
	_, err := parser.AddGroup("Database", "", &options)
	if err != nil {
		panic(err)
	}
}

func NewConnection() (conn *Connection, err error) {
	log.Println("Checking config...")
	if options.Host == "" {
		return nil, errors.New("Host address cannot be empty!")
	}
	if options.Port == 0 {
		return nil, errors.New("RethinkDB client port cannot be empty!")
	}
	if options.PasswordFile != "" {
		if PasswordFile, err := os.Open(options.PasswordFile); err != nil {
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
			options.Password = string(data)
		}
	}
	println()
	log.Printf("Host: %v", options.Host)
	log.Printf("Port: %v", options.Port)
	if strings.Count(options.Password, "")-1 > 0 {
		log.Printf("Password: %v", strings.Repeat("*", strings.Count(options.Password, "")-1))
	} else {
		log.Printf("Password: (not set)")
	}
	println()
	log.Println("Connecting...")
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Password: options.Password,
		Address:  fmt.Sprintf("%v:%v", options.Host, options.Port),
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
