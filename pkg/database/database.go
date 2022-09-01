package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/VenomPCPL/rethinkdb-go"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"strings"
)

func CLIMiddleware(ctx *cli.Context, run cli.ActionFunc) error {
	sess, err := CreateConnection(ctx.String("host"), ctx.String("password"), ctx.String("password-file"), ctx.Uint("port"))
	if err != nil {
		return err
	}
	ctx.Context = context.WithValue(ctx.Context, "database", sess)
	return run(ctx)
}

func CreateConnection(host, password, passwordFile string, port uint) (*rethinkdb.Session, error) {
	log.Println("Checking config...")
	if host == "" {
		return nil, errors.New("host address cannot be empty")
	}
	if port == 0 {
		return nil, errors.New("client port cannot be empty")
	}
	if passwordFile != "" {
		if PasswordFile, err := os.Open(passwordFile); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, errors.New("password file not found")
			} else {
				return nil, err
			}
		} else {
			data, err := io.ReadAll(PasswordFile)
			if err != nil {
				return nil, err
			}
			password = string(data)
		}
	}
	println()
	log.Printf("Host: %v", host)
	log.Printf("Port: %v", port)
	if strings.Count(password, "")-1 > 0 {
		log.Printf("Password: %v", strings.Repeat("*", strings.Count(password, "")-1))
	} else {
		log.Printf("Password: (not set)")
	}
	println()
	log.Println("Connecting...")
	session, err := rethinkdb.Connect(rethinkdb.ConnectOpts{
		Password:      password,
		Address:       fmt.Sprintf("%v:%v", host, port),
		UseJSONNumber: true,
		Username:      "admin",
	})
	if err != nil {
		return nil, err
	}
	log.Println("Successfully connected to RethinkDB")
	return session, nil
}
