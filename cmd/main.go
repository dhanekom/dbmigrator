package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dhanekom/dbmigrator/pkg/dbrepo"
	"github.com/dhanekom/dbmigrator/pkg/migrator"
	"github.com/joho/godotenv"
)

func main() {
	// get arguments
	var dbDrivername *string
	var dbHost *string
	var dbPort *string
	var dbName *string
	var dbUser *string
	var dbPassword *string
	var dbSSL *string
	var path *string
	var allowFix bool
	var verbose *bool = new(bool)
	var fix *bool = new(bool)
	
	*verbose = false
	*fix = false

	_, err := os.Stat(".env")
	if os.IsExist(err) {
		// fmt.Println("Reading params from .env")
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file")
		}

		*dbDrivername = os.Getenv("DBMIGRATOR_DB_DRIVER")
		*dbHost = os.Getenv("DBMIGRATOR_DB_HOST")
		*dbPort = os.Getenv("DBMIGRATOR_DB_PORT")
		*dbName = os.Getenv("DBMIGRATOR_DB_NAME")
		*dbUser = os.Getenv("DBMIGRATOR_DB_USERNAME")
		*dbPassword = os.Getenv("DBMIGRATOR_DB_PASSWORD")
		*dbSSL = os.Getenv("DBMIGRATOR_DB_SSL")
		*path = os.Getenv("DBMIGRATOR_PATH")
		tmpAllowFix := os.Getenv("DBMIGRATOR_ALLOW_FIX")
		allowFix, _ = strconv.ParseBool(tmpAllowFix)

		if *dbDrivername == "" {
			*dbDrivername = dbrepo.DBDRIVER_POSTGRES
		}

		if *path == "" {
			*path = "./migrations"
		}
	} else {
		// fmt.Println("Reading params from command line arguments")
		dbDrivername = flag.String("dbdriver", dbrepo.DBDRIVER_POSTGRES, fmt.Sprintf("Database driver (%s)", dbrepo.DBDRIVER_POSTGRES))
		dbHost = flag.String("host", "", "Database host IP or URL")
		dbPort = flag.String("port", "", "Database port")
		dbName = flag.String("dbname", "", "Database name")
		dbUser = flag.String("user", "", "Database username")
		dbPassword = flag.String("password", "", "Database password")
		dbSSL = flag.String("dbssl", "disable", "Database sslsettings (disable, prefer, require)")
		path = flag.String("path", "./migrations", "Path of migration files")
	}

	verbose = flag.Bool("verbose", false, "More detailed logging")
	fix = flag.Bool("fix", false, "More details logging")

	flag.Parse()

	if *dbDrivername == "" ||
	   *dbHost == "" ||
		 *dbPort == "" ||
		 *dbName == "" ||
		 *dbUser == "" ||
		 *dbPassword == "" ||
		 *path == "" {
			 log.Fatal("There are missing required parameters. Please run the application with the -h parameter for more information")
		 }

	if !(*dbSSL == "disable") && !(*dbSSL == "prefer") && !(*dbSSL == "require") {
		*dbSSL = "disable"
	}	

	if !allowFix && *fix {
		log.Fatal("Fix option will only be allowed it the DBMIGRATOR_ALLOW_FIX is set to true in a .env file")
	}

	var command, commandAttr string
	if len(flag.Args()) -1 > 2 {
		log.Default().Fatalf("a max of 2 trailing attributes (a command and an optional command attribute) is allow. %d arguments found - %v", strings.Join(flag.Args(), ","))
	}
	for i, arg := range flag.Args() {
		switch i {
		case 0: command = arg
		case 1: commandAttr = arg
		default:
			log.Fatal("too many trailing attributes found")
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
	)

	// Create Migrator
	myMigrator, err := migrator.NewMigrator(*path, myDBRepo, *verbose, true)
	if err != nil {
		log.Fatal(err)
	}

	// Execute migrator command
	err = myMigrator.Execute(command, commandAttr)
	if err != nil {
		log.Fatal(err)
	}
}