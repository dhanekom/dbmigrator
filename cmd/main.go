package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dhanekom/dbmigrator/pkg/config"
	"github.com/dhanekom/dbmigrator/pkg/dbrepo"
	"github.com/dhanekom/dbmigrator/pkg/migrator"
	"github.com/joho/godotenv"
)

var app config.AppConfig
var infoLog *log.Logger
var errorLog *log.Logger

func main() {
	infoLog = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
	app.Infolog = infoLog

	errorLog = log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	app.Errorlog = errorLog

	// get arguments
	var (
		dbDrivername *string = new(string)
		dbHost *string = new(string)
		dbPort *string = new(string)
		dbName *string = new(string)
		dbUser *string = new(string)
		dbPassword *string = new(string)
		dbSSL *string = new(string)
		migrationDir *string = new(string)
		allowFix bool
		verbose *bool = new(bool)
		// fix *bool = new(bool)
	)
	
	*verbose = false
	// *fix = false
	*dbSSL = "disable"

	dbDrivername = flag.String("dbdriver", dbrepo.DBDRIVER_POSTGRES, fmt.Sprintf("Database driver (%s)", dbrepo.DBDRIVER_POSTGRES))
	dbHost = flag.String("host", "", "Database host IP or URL")
	dbPort = flag.String("port", "", "Database port")
	dbName = flag.String("dbname", "", "Database name")
	dbUser = flag.String("user", "", "Database username")
	dbPassword = flag.String("password", "", "Database password")
	dbSSL = flag.String("dbssl", "disable", "Database sslsettings (disable, prefer, require)")
	migrationDir = flag.String("path", "./migrations", "Path of migration files")
	verbose = flag.Bool("verbose", false, "More detailed logging")

	_, err := os.Stat(".env")
	if err == nil {
		app.LogVerboseLn("reading configs from .env file")

		err := godotenv.Load()
		if err != nil {
			fmt.Printf("Error loading .env file - %s", err)
			os.Exit(1)
		}				

		tmpDrivername := os.Getenv("DBMIGRATOR_DB_DRIVER")
		if *dbDrivername == "" && tmpDrivername != "" {
			*dbDrivername	= tmpDrivername		
		}
		if *dbHost == "" {
			*dbHost = os.Getenv("DBMIGRATOR_DB_HOST")
		}
		if *dbPort == "" {
			*dbPort = os.Getenv("DBMIGRATOR_DB_PORT")	
		}
		if *dbName == "" {
			*dbName = os.Getenv("DBMIGRATOR_DB_NAME")
		}		
		if *dbUser == "" {
			*dbUser = os.Getenv("DBMIGRATOR_DB_USERNAME")
		}		
		if *dbPassword == "" {
			*dbPassword = os.Getenv("DBMIGRATOR_DB_PASSWORD")
		}		
		tmpDBSSL := os.Getenv("DBMIGRATOR_DB_SSL")
		if tmpDBSSL == "" && tmpDBSSL != "" {
			*dbSSL = tmpDBSSL
		}
		tmpMigrationDir := os.Getenv("DBMIGRATOR_PATH")
		if *migrationDir != "" && tmpMigrationDir != "" {
			*migrationDir = tmpMigrationDir
		}		
		tmpAllowFix := os.Getenv("DBMIGRATOR_ALLOW_FIX")
		allowFix, _ = strconv.ParseBool(tmpAllowFix)
	}
	// fix = flag.Bool("fix", false, "More details logging")

	flag.Parse()

	app.Verbose = *verbose

	missingParams := make([]string, 0)
	checkAndAddMissingParams := func(desc string, value string) {
		if value == "" {
			missingParams = append(missingParams, "* " + desc)
		}
	}

	checkAndAddMissingParams("dbdriver", *dbDrivername)
	checkAndAddMissingParams("host", *dbHost)
	checkAndAddMissingParams("port", *dbPort)
	checkAndAddMissingParams("dbName", *dbName)
	checkAndAddMissingParams("user", *dbUser)
	checkAndAddMissingParams("password", *dbPassword)
	checkAndAddMissingParams("path", *migrationDir)

	if len(missingParams) > 0 {
		var tmpErrStr string
		for _, v := range missingParams {
			tmpErrStr = tmpErrStr + fmt.Sprintln(v)
		}

		fmt.Printf("The following required parameters are missing:\n%sPlease run the application with the -h parameter for more information", tmpErrStr)
		os.Exit(1)
	}

	app.AllowFix = allowFix
 
	// if !(*dbSSL == "disable") && !(*dbSSL == "prefer") && !(*dbSSL == "require") {
	// 	*dbSSL = "disable"
	// }	

	// if !allowFix && *fix {
	// 	fmt.Prinln("fix option will only be allowed it the DBMIGRATOR_ALLOW_FIX is set to true in a .env file")
	// }

	var command, commandAttr string
	if len(flag.Args()) -1 > 2 {
		fmt.Printf("a max of 2 trailing attributes (a command and an optional command attribute) is allow. %d arguments found - %v", len(flag.Args()),  strings.Join(flag.Args(), ","))
		os.Exit(1)
	}
	for i, arg := range flag.Args() {
		switch i {
		case 0: command = arg
		case 1: commandAttr = arg
		default:
			fmt.Println("too many trailing attributes found")
			os.Exit(1)
		}
	}

	myDBRepo, err := dbrepo.NewDBRepo(
		*dbDrivername,
		dbrepo.DBConnectionData{
			DBHost: *dbHost, 
		  DBPort: *dbPort, 
			DBName: *dbName, 
			DBUser: *dbUser, 
			DBPassword: *dbPassword, 
			DBSSL: *dbSSL,
		},
		&app,
	)

	// Create Migrator
	myMigrator, err := migrator.NewMigrator(*migrationDir, myDBRepo, &app)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Execute migrator command
	err = myMigrator.Execute(command, commandAttr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}