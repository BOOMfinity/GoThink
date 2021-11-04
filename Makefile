all:
	rm -rf ./build/*
	make windows
	make linux
	make osx

upx:
	make all
	upx ./build/gothink
	upx ./build/gothink.exe
	upx ./build/gothink.osx

windows:
	GOOS=windows go build -ldflags="-s -w" -o="build/gothink.exe" ./cmd/gothink/gothink.go

linux:
	GOOS=linux go build -ldflags="-s -w" -o="build/gothink" ./cmd/gothink/gothink.go

osx:
	GOOS=darwin go build -ldflags="-s -w" -o="build/gothink.osx" ./cmd/gothink/gothink.go

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
	/usr/bin/time -v ./build/gothink export &> gothink-export.bench.txt
	@-killall rethinkdb &> $(LOGS)
	sleep 5
	@-rm -rf .rdata &> $(LOGS)
	@{ rethinkdb -d .rdata &> $(LOGS) &}
	sleep 10
	/usr/bin/time -v ./build/gothink import &> gothink-import.bench.txt
	sleep 5
	@killall rethinkdb &> $(LOGS)
	@rm -rf .rdata
