set GOARCH=386
go install -ldflags="-s -w" .\cmd\dbmigrator\.
copy C:\Users\dewaldh\go\bin\windows_386\dbmigrator.exe c:\grppos