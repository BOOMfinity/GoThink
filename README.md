# About GoThink
GoThink is a RethinkDB backup tool, which allows you to back up all or preselected data and restore them.

This application was built with a focus on speed, friendliness and functionality.

In the future we want to add support for importing backups created with python driver, server migration (copy command) and more compression methods to choose from.

# Installation

- Method 1 - build from source
  - Download the latest Golang version
  - Run `go get github.com/BOOMfinity-Developers/GoThink/cmd/gothink` command. 

The binary will be installed automatically, if Go bin directory is in the PATH.
If it is not, add `export PATH=$PATH:$HOME/go/bin` to `$HOME/.bashrc` (for bash). If you are on Windows, well... ¯\_(ツ)_/¯. 

- Method 2 - download binary from release page
  - Download binary from "releases" page. Choose from multiple OSs and archs.

For now, precompiled binaries are available for Windows (amd64) and Linux (amd64).

# Usage

Run `gothink -h` for more information about available commands. It's pretty straightforward.

# Benchmarks

![image](https://i.imgur.com/UV5xIF8.png)

    ~796k documents

    GoThink: v1.0.1
    CPU: 4 × Intel® Core™ i3-3217U CPU @ 1.80GHz
    Operating System: Manjaro Linux
    KDE Plasma Version: 5.21.4
    KDE Frameworks Version: 5.81.0
    Qt Version: 5.15.2
    Kernel Version: 5.10.34-1-MANJARO
    OS Type: 64-bit

# FAQ

### I have a problem! I want to request feature!

Just create the issue and describe your request. We will look at it.

### Can I create a PR?

Of course, you can. We will check it and decide what to do. However if you're going to do something big, create an issue first to make sure no one else is already doing same thing.