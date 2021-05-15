package main

import (
	"fmt"
	"log"
	"os"

	"github.com/BOOMfinity-Developers/GoThink"
	"github.com/BOOMfinity-Developers/GoThink/pkg/database"
	"github.com/BOOMfinity-Developers/GoThink/pkg/export"
	_import "github.com/BOOMfinity-Developers/GoThink/pkg/import"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:      "GoThink",
		Usage:     "Fast and simple RethinkDB backup tool",
		UsageText: "gothink <command> <options...> - run 'gothink <command> --help' for more details about a command",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Value: "localhost",
				Usage: "Your RethinkDB server address",
			},
			&cli.StringFlag{
				Name:    "password",
				Aliases: []string{"pass", "p"},
				Usage:   "Your RethinkDB server password",
			},
			&cli.UintFlag{
				Name:  "port",
				Value: 28015,
				Usage: "Your RethinkDB client port",
			},
			&cli.StringFlag{
				Name:    "password-file",
				Aliases: []string{"pf"},
				Usage:   "Path to the file containing the password for your database",
			},
		},
		Commands: []*cli.Command{
			{
				Name:        "version",
				Description: "Just shows a GoThink version",
				Aliases:     []string{"v", "ver"},
				Action: func(context *cli.Context) error {
					fmt.Printf("GoThink: v%v. Supports backups from GoThink %v\n", GoThink.Version, GoThink.Supported.String())
					return nil
				},
			},
			{
				Name:        "export",
				Aliases:     []string{"e"},
				Usage:       "Exports documents from RethinkDB",
				Description: "It allows you to dump all or selected data from RethinkDB",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "export-path",
						Aliases: []string{"export", "e"},
						Usage:   "What will be exported. Use database.table syntax",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"file", "f"},
						Usage:   "Just output file name / path",
						Value:   "backup.tar.gz",
					},
				},
				Action: func(context *cli.Context) error {
					return database.CLIMiddleware(context, export.RunFromCLI)
				},
			},
			{
				Name:        "import",
				Aliases:     []string{"i"},
				Usage:       "Restores data from backup",
				Description: "It allows you to restore a backup. At the moment, GoThink only supports its own backups. BACKUPS FROM PYTHON DRIVER ARE NOT YET SUPPORTED!\nAlso, this version of GoThink only supports backups from GoThink " + GoThink.Supported.String(),
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "import-path",
						Aliases: []string{"import", "i"},
						Usage:   "What will be imported. Use database.table syntax",
					},
					&cli.StringFlag{
						Name:    "import-file",
						Aliases: []string{"file", "f"},
						Usage:   "Path to the backup file",
						Value:   "backup.tar.gz",
					},
				},
				Action: func(context *cli.Context) error {
					return database.CLIMiddleware(context, _import.RunFromCLI)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
