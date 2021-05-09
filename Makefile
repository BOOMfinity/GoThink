windows:
	GOOS=windows go build -ldflags="-s -w" -o="gothink-export.exe" ./cmd/export/export.go
	GOOS=windows go build -ldflags="-s -w" -o="gothink-import.exe" ./cmd/import/import.go ./cmd/import/table.go ./cmd/import/workers.go

linux:
	GOOS=linux go build -ldflags="-s -w" -o="gothink-export" ./cmd/export/export.go
	GOOS=linux go build -ldflags="-s -w" -o="gothink-import" ./cmd/import/import.go ./cmd/import/table.go ./cmd/import/workers.go
