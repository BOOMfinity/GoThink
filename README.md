[![Stand With Ukraine](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/badges/StandWithUkraine.svg)](https://stand-with-ukraine.pp.ua)
# About GoThink
GoThink is a RethinkDB backup tool, that allows you to back up all or preselected data and restore it.

This application was built with a focus on speed, friendliness, and functionality.

In the future, we want to add support for importing backups created with python driver, server migration (copy command), and more compression methods to choose from.

# Information for people using python driver
GoThink only implements the DUMP and RESTORE commands from the official python driver. It just does it faster and more efficiently. 

There is one thing GoThink doesn't have: TLS certificates support. We plan to add it in the future.

We will be happy if you use our tool instead of the official python driver :)

# Installation

### Method 1 - download prebuilt binary
  - Download the latest binary from [releases page](https://github.com/BOOMfinity/GoThink/releases). Choose from multiple OSs and architectures.
  
### Method 2 - build from source
  - Download the latest Golang version
  - Run `go install github.com/BOOMfinity/GoThink/cmd/gothink@latest` command

The binary will be installed automatically if Go bin directory is in the PATH.
If it is not, add `export PATH=$PATH:$HOME/go/bin` to `$HOME/.bashrc` (for bash). If you are on Windows, well... ¯\_(ツ)_/¯.

# Usage

Run `gothink help` for information about available commands. It's pretty straightforward.

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

Of course, you can. We will check it and decide what to do. However, if you're going to do something big, create an issue first to make sure no one else is already working on the same thing.