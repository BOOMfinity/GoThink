package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/BOOMfinity-Developers/GoThink"
	"github.com/BOOMfinity-Developers/GoThink/pkg/check"
	"github.com/BOOMfinity-Developers/GoThink/pkg/database"
	"github.com/BOOMfinity-Developers/GoThink/pkg/export"
	_import "github.com/BOOMfinity-Developers/GoThink/pkg/import"
	"github.com/urfave/cli/v2"
)

func main() {
	globalFlags := []cli.Flag{
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
			Usage: "Your RethinkDB client driver port",
		},
		&cli.StringFlag{
			Name:    "password-file",
			Aliases: []string{"pf"},
			Usage:   "Path to the file containing the password for your database",
		},
	}
	app := &cli.App{
		Name:      "GoThink",
		Usage:     "Fast and simple RethinkDB backup tool",
		UsageText: "gothink <command> <options...> - run 'gothink <command> --help' for more details",
		Commands: []*cli.Command{
			{
				Name:        "version",
				Description: "Shows GoThink version",
				Aliases:     []string{"v", "ver"},
				Action: func(context *cli.Context) error {
					fmt.Printf("GoThink: v%v. Supports backups from GoThink %v\n", GoThink.Version, GoThink.Supported.String())
					return nil
				},
			},
			{
				Name:        "export",
				Aliases:     []string{"e", "dump"},
				Usage:       "Exports documents from RethinkDB",
				Description: "It allows you to dump all or selected data",
				Flags: append(globalFlags, []cli.Flag{
					&cli.StringFlag{
						Name:    "export-path",
						Aliases: []string{"export", "e"},
						Usage:   "Choose what will be exported. Use 'database.table' syntax",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"file", "f"},
						Usage:   "Output filename or path",
						Value:   "backup.tar.gz",
					},
				}...),
				Action: func(context *cli.Context) error {
					return database.CLIMiddleware(context, export.RunFromCLI)
				},
			},
			{
				Name:        "import",
				Aliases:     []string{"i", "restore"},
				Usage:       "Restores data from backup",
				Description: "It allows you to restore a backup. At the moment, GoThink only supports its own backups. BACKUPS FROM OTHER DRIVERS ARE NOT YET SUPPORTED!\nNote: This version of GoThink only supports backups from GoThink " + GoThink.Supported.String(),
				Flags: append(globalFlags, []cli.Flag{
					&cli.StringFlag{
						Name:    "import-path",
						Aliases: []string{"import", "i"},
						Usage:   "What will be imported. Use 'database.table' syntax",
					},
					&cli.StringFlag{
						Name:    "import-file",
						Aliases: []string{"file", "f"},
						Usage:   "Path to the backup file",
						Value:   "backup.tar.gz",
					},
					&cli.Uint64Flag{
						Name:  "replicas",
						Usage: "Number of replicas to setup on created table",
						Value: 1,
					},
					&cli.Uint64Flag{
						Name:  "shards",
						Usage: "Number of shards to setup on created table",
						Value: 1,
					},
				}...),
				Action: func(context *cli.Context) error {
					return database.CLIMiddleware(context, _import.RunFromCLI)
				},
			},
			{
				Name:        "check",
				Usage:       "Compares two databases. Useful, if you want to check if backup was restored correctly",
				Description: "Compares two databases. Useful, if you want to check if backup was restored correctly.\n\nIMPORTANT: This command may take a long time because it compares ALL documents and their size.",
				Flags: append(globalFlags, []cli.Flag{
					&cli.StringFlag{
						Name:     "host-b",
						Required: true,
						Usage:    "Target RethinkDB server address",
					},
					&cli.StringFlag{
						Name:    "password-b",
						Aliases: []string{"pass-b", "p-b"},
						Usage:   "Target RethinkDB server password",
					},
					&cli.UintFlag{
						Name:  "port-b",
						Value: 28015,
						Usage: "Target RethinkDB client driver port",
					},
					&cli.StringFlag{
						Name:    "password-file-b",
						Aliases: []string{"pf-b"},
						Usage:   "Path to the file containing the password for your database",
					},
					&cli.StringFlag{
						Name:    "database",
						Aliases: []string{"db"},
						Usage:   "Database name to check",
					},
				}...),
				Action: func(ctx *cli.Context) error {
					return database.CLIMiddleware(ctx, func(ctx2 *cli.Context) error {
						println()
						println("Connecting to the target server...")
						println()
						db, err := database.CreateConnection(ctx2.String("host-b"), ctx2.String("password-b"), ctx2.String("password-file-b"), ctx2.Uint("port-b"))
						if err != nil {
							return err
						}
						ctx2.Context = context.WithValue(ctx2.Context, "database-b", db)
						return check.RunFromCLI(ctx2)
					})
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
