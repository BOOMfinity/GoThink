@echo off
echo Building GoThink-export...
cmd /c "go build -o=gothink-export.exe .\cmd\export\export.go"
echo GoThink-export was built
echo.
echo Building GoThink-import...
cmd /c "go build -o=gothink-import.exe .\cmd\import\import.go .\cmd\import\table.go .\cmd\import\workers.go"
echo GoThink-import was built
