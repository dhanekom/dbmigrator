package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dhanekom/dbmigrator/config"
	"github.com/dhanekom/dbmigrator/dbrepo"
	"github.com/dhanekom/dbmigrator/migrator"
	"github.com/joho/godotenv"
)

var app config.AppConfig
var infoLog *log.Logger
var errorLog *log.Logger

func main() {
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
		logDir string		
		// fix *bool = new(bool)
	)
	
	*dbSSL = "disable"

	dbDrivername = flag.String("dbdriver", dbrepo.DBDRIVER_POSTGRES, fmt.Sprintf("Database driver (%s)", dbrepo.DBDRIVER_POSTGRES))
	dbHost = flag.String("host", "", "Database host IP or URL")
	dbPort = flag.String("port", "", "Database port")
	dbName = flag.String("dbname", "", "Database name")
	dbUser = flag.String("user", "", "Database username")
	dbPassword = flag.String("password", "", "Database password")
	dbSSL = flag.String("dbssl", "disable", "Database sslsettings (disable, prefer, require)")
	migrationDir = flag.String("path", "./migrations", "Path of migration files")

	_, err := os.Stat(".env")
	if err == nil {
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
		logDir = os.Getenv("DBMIGRATOR_LOG_DIR")
	}
	// fix = flag.Bool("fix", false, "More details logging")

	flag.Parse()

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

	exPath, err := os.Executable()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	exPath = path.Dir(exPath)

	if logDir == "" {
		logDir = exPath
	}

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err = os.MkdirAll(logDir, 0666)
		if err != nil {
			log.Fatal(err)
		}
	}	

	appFilename := os.Args[0]
	logFilename := appFilename[:len(appFilename) - len(filepath.Ext(appFilename))] + ".log"
	logFile, err := os.OpenFile(filepath.Join(logDir, logFilename), os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer logFile.Close()

	infoLog = log.New(logFile, "INFO\t", log.Ldate|log.Ltime)
	app.Infolog = infoLog

	errorLog = log.New(logFile, "ERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	app.Errorlog = errorLog	

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

	myMigrator, err := migrator.NewMigrator(*migrationDir, myDBRepo, &app)
	if err != nil {
		errorLog.Println(err)
		fmt.Println(err)
		os.Exit(1)
	}

	err = execute(myMigrator, command, commandAttr)
	if err != nil {
		errorLog.Println(err)
		fmt.Println(err)
		os.Exit(1)
	}
}

func execute(m *migrator.Migrator, command, commandAttr string) error {
	command = strings.ToLower(command)

	m.App.Infolog.Printf("executed command %q with attributes %q", command, commandAttr)

	switch command {
  case migrator.COMMAND_CREATE:
		return m.Create(commandAttr)
	case migrator.COMMAND_UP:
		return m.Up(commandAttr)
	case migrator.COMMAND_DOWN:
		return m.Down(commandAttr)
	case migrator.COMMAND_FORCE:
		return m.Force(commandAttr)
	case migrator.COMMAND_LIST:
		return listMigrationInfo(m)
	case migrator.COMMAND_FIX:
		return fixMigrations(m)
	case migrator.COMMAND_VERSION:
		return listCurrentVersion(m)			
	default:
		return fmt.Errorf("%q is not a valid command. Valid commands are %q, %q, %q, %q", command, migrator.COMMAND_CREATE, migrator.COMMAND_UP, migrator.COMMAND_DOWN, migrator.COMMAND_FORCE)
	}
}

func listMigrationInfo(m *migrator.Migrator) error {
	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("list - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("closing DB")
		m.DBRepository.CloseDB()
	}()

	m.App.Infolog.Println("successfully connected to DB")		

	err = m.DBRepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf("list - %s", err)
	}	

	mvs, err := m.GetMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf("list - %s", err)
	}

	getBoolStr := func(value bool, TrueStr, FalseStr string) string {
		if value {
			return TrueStr
		} else {
			return FalseStr
		}
	}

	fmt.Printf("%-15s | %-30s | %-8s | %-9s | %-11s\n", "Version", "Description", "Migrated", "Up Exists", "Down Exists")
	fmt.Printf("%-15s | %-30s | %-8s | %-9s | %-11s\n", "-------", "-----------", "--------", "---------", "-----------")
	for _, mv := range mvs {
		fmt.Printf("%-15s | %-30s | %-8s | %-9s | %-11s\n", mv.Version, mv.Desc, getBoolStr(mv.ExistsInDB, "Y", " "), getBoolStr(mv.UpFileExists, "Y", " "), getBoolStr(mv.DownFileExists, "Y", " "))
	}

	return nil
}

func fixMigrations(m *migrator.Migrator) error {
	var msg string
	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("fixMigrations - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("closing DB")
		m.DBRepository.CloseDB()
	}()

	m.App.Infolog.Println("successfully connected to DB")		

	err = m.DBRepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf("fixMigrations - %s", err)
	}	

	mvs, err := m.GetMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf("fixMigrations - %s", err)
	}

	// Check if there are migrations that are older that the current migration version that have not been run
	currentVersion, err := m.CurrentVersion()
	if err != nil {
		return fmt.Errorf("fixMigrations - %s", err)
	}

	lastVersionBeforeGap := ""
	hasGap := false
	for _, mv := range mvs {
		if !mv.ExistsInDB {
			msg = "migrations gaps found"
			fmt.Println(msg)
			m.App.Infolog.Println("fixMigrations - " + msg)		
			msg = fmt.Sprintf("oldest migration version not yet executed: %s", mv.Version)
			fmt.Println(msg)
			m.App.Infolog.Println("fixMigrations - " + msg)
			hasGap = true
			break
		}
		lastVersionBeforeGap = mv.Version
	}

	if hasGap {
		msg = fmt.Sprintf("migrating down to version %s", lastVersionBeforeGap)
		fmt.Println(msg)
		m.App.Infolog.Println("fixMigrations - " + msg)
		err = m.Migrate(migrator.DIRECTION_DOWN, lastVersionBeforeGap)
		if err != nil {
			return fmt.Errorf("fixMigrations - %s", err)
		}

		msg = fmt.Sprintf("migrating up to previous current version %s", currentVersion)
		fmt.Println(msg)
		m.App.Infolog.Println("fixMigrations - " + msg)		
		err = m.Migrate(migrator.DIRECTION_UP, currentVersion)
		if err != nil {
			return fmt.Errorf("fixMigrations - %s", err)
		}
	}

	return nil
}

func listCurrentVersion(m *migrator.Migrator) error {
	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("listCurrentVersion - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("closing DB")
		m.DBRepository.CloseDB()
	}()

	currentVersion, err := m.CurrentVersion()
	if err != nil {
		return fmt.Errorf("listCurrentVersion - %s", err)
	}

	msg := fmt.Sprintf("current version: %s", currentVersion)
	fmt.Println(msg)
	m.App.Infolog.Println("listCurrentVersion - " + msg)		

	return nil
}