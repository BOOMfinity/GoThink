all:
	make windows
	make linux

windows:
	GOOS=windows go build -ldflags="-s -w" -o="build/gothink-export.exe" ./cmd/gothink-export/export.go
	GOOS=windows go build -ldflags="-s -w" -o="build/gothink-import.exe" ./cmd/gothink-import/import.go ./cmd/gothink-import/table.go ./cmd/gothink-import/workers.go

linux:
	GOOS=linux go build -ldflags="-s -w" -o="build/gothink-export" ./cmd/gothink-export/export.go
	GOOS=linux go build -ldflags="-s -w" -o="build/gothink-import" ./cmd/gothink-import/import.go ./cmd/gothink-import/table.go ./cmd/gothink-import/workers.go

LOGS=/dev/null

benchmark-linux:
	make linux
	@-killall rethinkdb &> $(LOGS)
	@-rm -rf .rdata &> $(LOGS)
	@-killall rethinkdb &> $(LOGS)
	sleep 5
	@{ rethinkdb -d .rdata &> $(LOGS) &}
	sleep 10
	# Tests
	/usr/bin/time -v rethinkdb restore import.tar.gz &> python-import.bench.txt
	@-killall rethinkdb &> $(LOGS)
	sleep 5
	@{ rethinkdb -d .rdata &> $(LOGS) &}
	sleep 10
	/usr/bin/time -v rethinkdb dump -f py-dump.tar.gz &> python-export.bench.txt
	@-killall rethinkdb &> $(LOGS)
	sleep 5
	@{ rethinkdb -d .rdata &> $(LOGS) &}
	sleep 10
	/usr/bin/time -v ./gothink-export &> gothink-export.bench.txt
	@-killall rethinkdb &> $(LOGS)
	sleep 5
	@-rm -rf .rdata &> $(LOGS)
	@{ rethinkdb -d .rdata &> $(LOGS) &}
	sleep 10
	/usr/bin/time -v ./gothink-import --file backup.tar.gz &> gothink-import.bench.txt
	sleep 5
	@killall rethinkdb &> $(LOGS)
	@rm -rf .rdata
