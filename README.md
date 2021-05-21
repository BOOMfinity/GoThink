# About GoThink
GoThink is a RethinkDB backup tool, whick allows you to back up all or preselected data and restore them.

This application was built with a focus on speed, friendliness and functionality.

In the future we would like to add support for backups from the python driver, server migration (copy command) and 2-3 compression methods to choose from.

# Installation

Currently, there is only one way to go:

- Method 1 - build from source
  - Download the latest Golang version
  - Run `go get github.com/BOOMfinity-Developers/GoThink/cmd/gothink` command. 

The binary will be installed automatically, if Go bin directory is in the PATH.
If it is not, add `export PATH=$PATH:$HOME/go/bin` to `$HOME/.bashrc` (for bash). If you are on Windows, well... ¯\_(ツ)_/¯. 

**Available soon!**
- Method 2 - download binary from release page
  - Download binary from "releases" page. Choose from multiple OSs and archs.

~~For now, precompiled binaries are available for Windows (amd64), Linux (amd64, arm) and Darwin (amd64).~~

# Usage

Run `gothink -h` for more information about available commands. It's pretty straightforward.

# Benchmarks

SOON™️

# FAQ

### I have a problem! I want to request feature!

Just create the issue and describe your request. We will look at it.

### Can I create a PR?

Of course, you can. We will check it and decide what to do.
