# About
GoThink is a RethinkDB backup tool. It allows you to backup all or selected data and restore them.

We focused on speed, friendly use and functionality.

In the future we would like to add support for backups from the python driver, server migration / copy command and 2-3 compression methods to choose from.

# Installation / Build

There is one way for now:

- Method 1 (build from src)
  - Download the latest Golang version
  - Run `go get github.com/BOOMfinity-Developers/GoThink/cmd/gothink` command. 

The binary will be added automatically if Go bin dir is in the PATH.

If it isn't, add `export PATH=$PATH:$HOME/go/bin` to `$HOME/.bashrc` (for bash). 

**Available soon!**
- Method 2 (download binary from release page)
  - Click on the latest release on GoThink repo and download binary for the wanted os and arch.

~~For now, precompiled binaries are available for: Windows (amd64), Linux (amd64, arm) and Darwin (amd64)~~

# Usage

Run `gothink -h` for more information about available commands.

# Benchmarks

SOON™️

### I have a problem / feature request

Just create the issue and describe your request. We will look at it.

### Can I create a PR?

Of course, you can. We will check it and decide what to do.
