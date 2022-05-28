package migrator

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/dhanekom/dbmigrator/pkg/config"
	"github.com/dhanekom/dbmigrator/pkg/dbrepo"
	"github.com/dhanekom/dbmigrator/pkg/models"
)

type Migrator struct {
	path     string
	dbrepository *dbrepo.DBRepo
	app *config.AppConfig
}

const (
	COMMAND_CREATE = "create"
	COMMAND_UP = "up"
	COMMAND_DOWN = "down"
	COMMAND_FORCE = "force"
	COMMAND_LIST = "list"
	DIRECTION_UP = "up"
	DIRECTION_DOWN = "down"
)

var (
	re = regexp.MustCompile(`^(\d{8}_\d{6})_(\w+)\.(down|up)\.sql$`)
)

func NewMigrator(path string, db *dbrepo.DBRepo, a *config.AppConfig) (*Migrator, error) {
	result := Migrator{
		path: path,
		dbrepository:  db,
		app: a,
	}

	return &result, nil
}

func (m Migrator) Execute(command, commandAttr string) error {
	command = strings.ToLower(command)
	switch command {
  case COMMAND_CREATE:
		return m.Create(commandAttr)
	case COMMAND_UP:
		return m.Up(commandAttr)
	case COMMAND_DOWN:
		return m.Down(commandAttr)
	case COMMAND_FORCE:
		return m.Force(commandAttr)
	case COMMAND_LIST:
		return m.List()		
	default:
		return fmt.Errorf("%q is not a valid command. Valid commands are %q, %q, %q, %q", command, COMMAND_CREATE, COMMAND_UP, COMMAND_DOWN, COMMAND_FORCE)
	}
}

// Create creates an up and down migration file in the configured migration directory
func (m Migrator) Create(version string) error {
	// Check if Path exists and create dir if it does not exist
	_, err := os.Stat(m.path)
	if os.IsNotExist(err) {
		os.MkdirAll(m.path, 0666)
	}

	// Generate up and down file names in the following format yyyymmddhhnnss_descriptions
	version = strings.ToLower(version)
	var sb strings.Builder
	addUnderscore := false
	for _, r := range version {
		if unicode.IsNumber(r) || unicode.IsLetter(r) || r == '_' {
			if addUnderscore {
				sb.WriteRune('_')
			}
			addUnderscore = false
			sb.WriteRune(r)
		} else {
			addUnderscore = true
		}
	}

	version = sb.String()

	if strings.Trim(version, " ") == "" {
		return errors.New("create - migration name only contains invalid characters")
	}

	t := time.Now()
	version = t.Format("20060102_150405_") + version
	tmpFilepath := filepath.Join(m.path, version + ".up.sql")
	fmt.Printf("creating %s\n", tmpFilepath)
	file, err := os.Create(tmpFilepath)
	if err != nil {
		return fmt.Errorf("create - %s", err)
	}
	file.Close()

	tmpFilepath = filepath.Join(m.path, version + ".down.sql")
	fmt.Printf("creating %s\n", tmpFilepath)
	file, err = os.Create(tmpFilepath)
	if err != nil {
		return fmt.Errorf("create - %s", err)
	}
	file.Close()

	return nil
}

func (m Migrator) getMigrationVersionInfo() ([]models.MigrationVersion, error) {
	m.app.LogVerboseLn("gathering migration version info")
	result := make([]models.MigrationVersion, 0)
	mvs := make(map[string]*models.MigrationVersion, 0)
	files, err := ioutil.ReadDir(m.path)
	if err != nil {
		return nil, fmt.Errorf("getMigrationVersionInfo - %s", err)
	}

	for _, file := range files {
		matches := re.FindAllStringSubmatch(file.Name(), -1)

		if matches == nil {
			m.app.LogVerbosef("unable to parse seperate parts of filename %s\n", file.Name())
			continue
		}

		var mv *models.MigrationVersion
		version := matches[0][1]
		direction := matches[0][3]
		mv, ok := mvs[version]
		if !ok {
			mv = &models.MigrationVersion{
				Version: version,
				Desc: matches[0][2],
			}

			mvs[version] = mv
		}

		if direction == DIRECTION_UP {
			mv.UpFileExists = true
		} else if direction == DIRECTION_DOWN {
			mv.DownFileExists = true
		}
	}

	migratedVersions, err := m.dbrepository.MigratedVersions()
	if err != nil {
		return nil, fmt.Errorf("getMigrationVersionInfo - %s", err)
	}	

	for _, migratedVersion := range migratedVersions {
		mv, ok := mvs[migratedVersion]
		if !ok {
			mv = &models.MigrationVersion{
				Version: migratedVersion,
			}

			mvs[migratedVersion] = mv
		} else {
			mv.ExistsInDB = true
		}
	}

	for _, v := range mvs {
		result = append(result, *v)
	}

	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})

	return result, nil
}

func (m Migrator) getMigrationsToRun(mvs []models.MigrationVersion, currentVersion, toVersion, migrationDirection string) ([]models.MigrationVersion, error) {
	m.app.LogVerboseLn("determining migrations to run")
	result := make([]models.MigrationVersion, 0)

	if migrationDirection == DIRECTION_UP && currentVersion >= toVersion {
		return result, fmt.Errorf("getMigrationsToRun - to version must be higher than the current version")
	} else if migrationDirection == DIRECTION_DOWN && toVersion >= currentVersion {
		return result, fmt.Errorf("getMigrationsToRun - to version must be lower than the current version")
	}

	for _, mv := range mvs {
		if (migrationDirection == DIRECTION_UP && mv.Version > currentVersion && mv.Version <= toVersion) ||
		   (migrationDirection == DIRECTION_DOWN && mv.Version <= currentVersion && mv.Version > toVersion) {
			result = append(result, mv)
		}
	}

	return result, nil
}

func (m Migrator) migrate(command, toVersion string) error {
	if command != COMMAND_UP && command != COMMAND_DOWN && command != COMMAND_FORCE {
		return errors.New(fmt.Sprintf("migrate - %q is not a valid migration command", command))
	}

	if command == COMMAND_FORCE && toVersion == "" {
		return errors.New("migrate - force migrations require a to version to be specified")
	}

	m.app.LogVerboseLn("connecting to DB")
	err := m.dbrepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	defer func(){
		m.app.LogVerboseLn("closing DB")
		m.dbrepository.CloseDB()
	}()

	m.app.LogVerboseLn("successfully connected to DB")

	err = m.dbrepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}		
	
	// Get current version from db
	currentVersion, err := m.dbrepository.CurrentVersion()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	mvs, err := m.getMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	if toVersion == "" && command == COMMAND_UP {
		for i := len(mvs) -1; i >= 0; i-- {
			if mvs[i].FileExists(command) {
				toVersion = mvs[i].Version				
				m.app.LogVerbosef("migration version: %s\n", toVersion)
				break
			}
		}
	}	
	
	var migrationDirection string	
	if toVersion > currentVersion {
		migrationDirection = DIRECTION_UP
	} else {
		migrationDirection = DIRECTION_DOWN
	}	

	if toVersion == currentVersion {
		fmt.Printf("db already migrated to the newest version")
		return nil
	}
	// Determine direction. If e.g. version > current version then an up is required

	if command != COMMAND_FORCE && command != migrationDirection {
		if command == COMMAND_UP {
			return fmt.Errorf("migrate - up migration now allowed because the current db version is higher than %s", toVersion)
		} else {
			return fmt.Errorf("migrate - down migration now allowed because the current db version is lower than %s", toVersion)
		}
	}

	// Check if migration sql file exists

	// pattern := filepath.Join(m.path, fmt.Sprintf("%s_*.%s.sql", toVersion, tmpMigrationDirection))
	// matches, err := filepath.Glob(pattern)
	// if err != nil {
	// 	return fmt.Errorf("migrate - find files with pattern %s - %s", pattern, toVersion)
	// }

	// if matches == nil {
	// 	return fmt.Errorf("migrate - unable to find a %s migration file for version %s", tmpMigrationDirection, toVersion)
	// }

	// if len(matches) > 1 {
	// 	return fmt.Errorf("migrate - multiple %s migration files found for version %s", tmpMigrationDirection, toVersion)
	// }

	// Find all migration files between the current version (excluded) and the new version (included)
	migrationsToRun, err := m.getMigrationsToRun(mvs, currentVersion, toVersion, migrationDirection)
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	for _, mv := range migrationsToRun {
		data, err := ioutil.ReadFile(filepath.Join(m.path, mv.Filename(migrationDirection)))
		if err != nil {
			return fmt.Errorf("migrate - %s", err)
		}
	
		fmt.Printf("running migration %s", mv.Filename(migrationDirection))
		err = m.dbrepository.MigrateData(mv.Version, string(data), migrationDirection)
		if err != nil {
			fmt.Println("")
			return fmt.Errorf("migrate - %s", err)
		}	
		fmt.Println(" - success")
		// fmt.Printf("successfully migrated to version %s\n", mv.Version)		
	}

	return nil
}

// Up migrates a DB up to the version if the version is higher than the current version
func (m *Migrator) Up(version string) error {
	return m.migrate(COMMAND_UP, version)
}

// Down migrates a DB down to the version if the version is lower than the current version
func (m Migrator) Down(version string) error {
	return m.migrate(COMMAND_DOWN, version)
}

func (m Migrator) List() error {
	m.app.LogVerboseLn("connecting to DB")
	err := m.dbrepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	defer func(){
		m.app.LogVerboseLn("closing DB")
		m.dbrepository.CloseDB()
	}()

	m.app.LogVerboseLn("successfully connected to DB")		

	err = m.dbrepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}	

	mvs, err := m.getMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
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

// Force migrates a DB to the migration specified by version
func (m Migrator) Force(toVersion string) error {
	return m.migrate(COMMAND_FORCE, toVersion)
}

// // Fix determines whether there are gaps inmigrations (up migrations that are older than the currently active migration and were not previously run).
// // Fix will run down migrations to revert the DB version to the point before the oldest gap
// func (m Migrator) Fix() error {

// }