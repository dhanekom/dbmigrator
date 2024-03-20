set GOARCH=386
go install -ldflags="-s -w" -buildvcs=false .\cmd\dbmigrator\.
copy %USERPROFILE%\go\bin\windows_386\dbmigrator.exe c:\grppos