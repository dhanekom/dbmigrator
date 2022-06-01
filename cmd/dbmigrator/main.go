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

type Command struct {
	command string
	commandDesc string
	usage string
}

var commands = []Command {
	{migrator.COMMAND_CREATE, "create V", "Create up and down migration files with a timestamp and a description (V)"},
	{migrator.COMMAND_UP, "up [V]", "Applies all up migrations or migrates up to version V"},
	{migrator.COMMAND_DOWN, "down [V]", "Applies all down migrations or migrates down to version V"},
	{migrator.COMMAND_GOTO, "goto V", "Migrates up or down to version V"},
	{migrator.COMMAND_LIST, "list", "Lists migration details"},
	{migrator.COMMAND_VERSION, "version", "Lists the current migration version"},
	{migrator.COMMAND_FIX, "fix", "Finds older migrations that have not been executed and attempts to run them in a safe way"},
}

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
		silentMode *bool
		allowFix bool
		logpath *string		
	)

	allowFix = false
	
	dbDrivername = flag.String("dbdriver", "", fmt.Sprintf("database driver"))
	dbHost = flag.String("host", "", "database host IP or URL")
	dbPort = flag.String("port", "", "database port")
	dbName = flag.String("dbname", "", "database name")
	dbUser = flag.String("user", "", "database username")
	dbPassword = flag.String("password", "", "database password")
	dbSSL = flag.String("dbssl", "disable", "database sslsettings (disable, prefer, require)")
	migrationDir = flag.String("migration_dir", "", "directory of migration files")
	logpath = flag.String("log_path", "", "full path of log file")
	silentMode = flag.Bool("s", false, "allow command to run without any confirmation prompts")

	_, err := os.Stat(".env")
	if err == nil {
		err := godotenv.Load()
		if err != nil {
			fmt.Printf("error loading .env file - %s", err)
			os.Exit(1)
		}
		
		loadParam := func(value *string, envParamName string, useEnvValueIfProvided bool) {
			envParamValue := os.Getenv(envParamName)
			if (*value == "" && envParamValue != "") || (envParamValue != "" && useEnvValueIfProvided) {
				*value	= envParamValue		
			}			
		}

		loadParam(dbDrivername, "DBMIGRATOR_DB_DRIVER", false)
		loadParam(dbHost, "DBMIGRATOR_DB_HOST", false)
		loadParam(dbPort, "DBMIGRATOR_DB_PORT", false)
		loadParam(dbName, "DBMIGRATOR_DB_NAME", false)
		loadParam(dbUser, "DBMIGRATOR_DB_USERNAME", false)
		loadParam(dbPassword, "DBMIGRATOR_DB_PASSWORD", false)
		loadParam(dbSSL, "DBMIGRATOR_DB_SSL", true)
		loadParam(migrationDir, "DBMIGRATOR_MIGRATION_DIR", false)
		loadParam(logpath, "DBMIGRATOR_LOG_PATH", false)
		tmpAllowFix := os.Getenv("DBMIGRATOR_ALLOW_FIX")
		allowFix, _ = strconv.ParseBool(tmpAllowFix)
	}

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: dbmigrator FLAGS COMMAND [arg...]\n\n")
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()		
		fmt.Fprintln(w, "\nCommands:")
		var b strings.Builder
		for _, cmd := range commands {
			b.WriteString(fmt.Sprintf("  %-15s%s\n", cmd.commandDesc, cmd.usage))
		}
		fmt.Fprint(w, b.String(), "\n")
	}	

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
	checkAndAddMissingParams("migration_dir", *migrationDir)

	if len(missingParams) > 0 {
		var tmpErrStr string
		for _, v := range missingParams {
			tmpErrStr = tmpErrStr + fmt.Sprintln(v)
		}

		fmt.Printf("The following required parameters are missing:\n%sPlease run the application with the -h parameter for more information", tmpErrStr)
		os.Exit(1)
	}

	app.AllowFix = allowFix
	app.SilentMode = *silentMode

	appFilename := os.Args[0]
	appFilenameExclExt := appFilename[:len(appFilename) - len(filepath.Ext(appFilename))]

	exPath, err := os.Executable()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	exPath = path.Dir(exPath)

	if *logpath == "" {
		*logpath = filepath.Join(exPath, "logs", appFilenameExclExt + ".log")
	}

	if _, err := os.Stat(filepath.Dir(*logpath)); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(*logpath), 0666)
		if err != nil {
			log.Fatal(err)
		}
	}

	// logFilename := appFilenameExclExt + ".log"
	logFile, err := os.OpenFile(*logpath, os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer logFile.Close()

	if _, err := os.Stat(*migrationDir); os.IsNotExist(err) {
		err = os.MkdirAll(*migrationDir, 0666)
		if err != nil {
			log.Fatal(err)
		}
	}			

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

	if err != nil {
		errorLog.Println(err)
		fmt.Println(err)
		os.Exit(1)
	}	

	myMigrator, err := migrator.NewMigrator(*migrationDir, myDBRepo, &app)
	if err != nil {
		errorLog.Println(err)
		fmt.Println(err)
		os.Exit(1)
	}

	err = run(myMigrator, command, commandAttr)
	if err != nil {
		errorLog.Println(err)
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(m *migrator.Migrator, command, commandAttr string) error {
	command = strings.ToLower(command)

	m.App.Infolog.Printf("executed command %q with attributes %q", command, commandAttr)

	switch command {
	case migrator.COMMAND_CREATE:
		return m.Create(commandAttr)
	case migrator.COMMAND_UP:
		return m.Up(commandAttr)
	case migrator.COMMAND_DOWN:
		return m.Down(commandAttr)
	case migrator.COMMAND_GOTO:
		return m.Goto(commandAttr)
	case migrator.COMMAND_LIST:
		return listMigrationInfo(m)
	case migrator.COMMAND_FIX:
		return fixMigrations(m)
	case migrator.COMMAND_VERSION:
		return listCurrentVersion(m)			
	default:
		return fmt.Errorf("%q is not a valid command. Please run the application with the -h parameter for more information", command)
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

	lineFormat := "%-15s | %-30s | %-8s | %-9s | %-11s\n"

	fmt.Printf(lineFormat, "Version", "Description", "Migrated", "Up Exists", "Down Exists")
	fmt.Printf(lineFormat, "-------", "-----------", "--------", "---------", "-----------")
	for _, mv := range mvs {
		fmt.Printf(lineFormat, mv.Version, mv.Desc, getBoolStr(mv.ExistsInDB, "Y", " "), getBoolStr(mv.UpFileExists, "Y", " "), getBoolStr(mv.DownFileExists, "Y", " "))
	}

	return nil
}

func fixMigrations(m *migrator.Migrator) error {
	funcPrefix := "fixMigrations"
	if !m.App.AllowFix {
		return fmt.Errorf("fix option has been disabled")
	}

	var msg string
	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("closing DB")
		m.DBRepository.CloseDB()
	}()

	m.App.Infolog.Println("successfully connected to DB")		

	err = m.DBRepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}	

	mvs, err := m.GetMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	currentVersion, err := m.CurrentVersion()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	// Check if there are migrations that are older that the current migration version that have not been run	
	migrationGaps, lastValidVersion := m.FindMigrationGaps(mvs, currentVersion)

	if len(migrationGaps) == 0 {
		fmt.Println("no migration gaps found. Nothing to fix")
		return nil
	}

	err = m.GetConfirmation(fmt.Sprintf(`Fix is about to migrate down to version %s and back up to the current version.
Please type 'yes' to continue with the fix or 'no' to cancel`, lastValidVersion), []string{"yes"})
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}	

	msg = fmt.Sprintf("migrating down to version %s", lastValidVersion)
	fmt.Println(msg)
	m.App.Infolog.Println(funcPrefix + " - " + msg)
	err = m.Migrate(migrator.DIRECTION_DOWN, lastValidVersion)
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	msg = fmt.Sprintf("migrating up to previous current version %s", currentVersion)
	fmt.Println(msg)
	m.App.Infolog.Println(funcPrefix + " - " + msg)
	err = m.Migrate(migrator.DIRECTION_UP, currentVersion)
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	return nil
}

func listCurrentVersion(m *migrator.Migrator) error {
	funcPrefix := "listCurrentVersion"
	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("closing DB")
		m.DBRepository.CloseDB()
	}()

	currentVersion, err := m.CurrentVersion()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	if currentVersion == "" {
		msg := "no migration have been run yet"
		fmt.Println(msg)
		m.App.Infolog.Println(funcPrefix + " - " + msg)
		return nil		
	}

	msg := currentVersion
	fmt.Println(msg)
	m.App.Infolog.Println(funcPrefix + " - " + msg)		

	return nil
}