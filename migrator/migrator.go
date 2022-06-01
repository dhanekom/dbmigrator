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

	"github.com/dhanekom/dbmigrator/config"
	"github.com/dhanekom/dbmigrator/dbrepo"
	"github.com/dhanekom/dbmigrator/models"
)

type Migrator struct {
	path     string
	DBRepository *dbrepo.DBRepo
	App *config.AppConfig
	confirmationProvided bool
}

const (
	COMMAND_CREATE = "create"
	COMMAND_UP = "up"
	COMMAND_DOWN = "down"
	COMMAND_GOTO = "goto"
	COMMAND_LIST = "list"
	COMMAND_VERSION = "version"
	COMMAND_FIX = "fix"

	DIRECTION_UP = "up"
	DIRECTION_DOWN = "down"
)

var (
	re = regexp.MustCompile(`^(\d{8}_\d{6})_(\w+)\.(down|up)\.sql$`)
)

// NewMigrator creates a *Migrator that can migrate a DB to different migration versions
func NewMigrator(path string, db *dbrepo.DBRepo, a *config.AppConfig) (*Migrator, error) {
	result := Migrator{
		path: path,
		DBRepository:  db,
		App: a,
		confirmationProvided: false,
	}

	return &result, nil
}

// GetConfirmation prompts users to confirm whether they want to continue with a command
func (m *Migrator) GetConfirmation(promptMsg string, trueValues []string) (error) {
	if m.confirmationProvided || m.App.SilentMode {
		m.confirmationProvided = true
		return nil
	}

  var answer string
	fmt.Printf("%s: ", promptMsg)
	_, err := fmt.Scanf("%s", &answer)
	if err != nil || answer != "yes" {
		return errors.New("command cancelled")
	}

	m.confirmationProvided = true
	return nil
}

// Create creates an up and down migration file in the configured migration directory
func (m Migrator) Create(desc string) error {
	funcPrefix := "create"

	desc = strings.ToLower(desc)
	if desc == "" {
		return errors.New(funcPrefix + " - a description is required")
	}

	// Check if Path exists and create dir if it does not exist
	_, err := os.Stat(m.path)
	if os.IsNotExist(err) {
		os.MkdirAll(m.path, 0666)
	}

	// Generate up and down file names in the following format yyyymmddhhnnss_descriptions
	var sb strings.Builder
	addUnderscore := false
	for _, r := range desc {
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

	desc = sb.String()

	if strings.Trim(desc, " ") == "" {
		return errors.New(funcPrefix + " - migration name only contains invalid characters")
	}

	t := time.Now()
	desc = t.Format("20060102_150405_") + desc

  mvs, err := m.GetMigrationVersionInfoMap()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	for _, mv := range mvs {
		if desc == mv.Version {
			return fmt.Errorf(funcPrefix + " - migration files with prefix %q already exist", desc)
		}
	}

	tmpFilepath := filepath.Join(m.path, desc + ".up.sql")
	fmt.Printf("creating %s\n", tmpFilepath)
	file, err := os.Create(tmpFilepath)
	if err != nil {
		return fmt.Errorf("create - %s", err)
	}
	file.Close()

	tmpFilepath = filepath.Join(m.path, desc + ".down.sql")
	fmt.Printf("creating %s\n", tmpFilepath)
	file, err = os.Create(tmpFilepath)
	if err != nil {
		return fmt.Errorf("create - %s", err)
	}
	file.Close()

	return nil
}

// GetMigrationVersionInfoMap reads all files in the migration directory and parses the filenames to determine
// all the migration vesions and descriptions. There details are return in a map of models.MigrationVersion items
func (m Migrator) GetMigrationVersionInfoMap() (map[string]*models.MigrationVersion, error) {
	funcPrefix := "GetMigrationVersionInfoMap"	

	mvs := make(map[string]*models.MigrationVersion, 0)
	files, err := ioutil.ReadDir(m.path)
	if err != nil {
		return nil, fmt.Errorf(funcPrefix + " - getting migration filenames - %s", err)
	}

	for _, file := range files {
		matches := re.FindAllStringSubmatch(file.Name(), -1)

		if matches == nil {
			m.App.Infolog.Printf(funcPrefix + " - unable to parse seperate parts of filename %s\n", file.Name())
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
			if mv.UpFileExists {
				return nil, fmt.Errorf(funcPrefix + "more than one up migration file found for migration version %s", mv.Version)
			}
			mv.UpFileExists = true
		} else if direction == DIRECTION_DOWN {
			if mv.DownFileExists {
				return nil, fmt.Errorf(funcPrefix + "more than one down migration file found for migration version %s", mv.Version)
			}
			mv.DownFileExists = true
		}
	}

	return mvs, nil
}

// GetMigrationVersionInfo gathers details of all migrated versions and migrations files
func (m Migrator) GetMigrationVersionInfo() ([]models.MigrationVersion, error) {
	funcPrefix := "getMigrationVersionInfo"

	m.App.Infolog.Println("gathering migration version info")
	result := make([]models.MigrationVersion, 0)

  mvs, err := m.GetMigrationVersionInfoMap()
	if err != nil {
		return nil, fmt.Errorf(funcPrefix + " - %s", err)
	}		

	migratedVersions, err := m.DBRepository.MigratedVersions()
	if err != nil {
		return nil, fmt.Errorf(funcPrefix + " - %s", err)
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

// GetMigrationsToRun determines which migrations must be run and returns the result as a slice of models.MigrationVersion
func (m Migrator) GetMigrationsToRun(mvs []models.MigrationVersion, currentVersion, toVersion, migrationDirection string) ([]models.MigrationVersion, error) {
	funcPrefix := "getMigrationsToRun"
	m.App.Infolog.Println("determining migrations to run")
	result := make([]models.MigrationVersion, 0)

	if migrationDirection == DIRECTION_UP && currentVersion >= toVersion {
		return result, fmt.Errorf(funcPrefix + " - to version must be higher than the current version")
	} else if migrationDirection == DIRECTION_DOWN && toVersion >= currentVersion {
		return result, fmt.Errorf(funcPrefix + " - to version must be lower than the current version")
	}

	if migrationDirection == DIRECTION_UP {
		for i := 0; i <= len(mvs) - 1; i++ {
			mv := mvs[i]
			if (mv.Version > currentVersion && mv.Version <= toVersion) {
				result = append(result, mv)
			}
		}
	} else if migrationDirection == DIRECTION_DOWN {
		for i := len(mvs) -1; i >= 0; i-- {
			mv := mvs[i]
			if (mv.Version <= currentVersion && mv.Version > toVersion && mv.ExistsInDB) {
				result = append(result, mv)
			}
		}
	}

	return result, nil
}

// FindMigrationGaps finds all migrations that are older than the current migration version and have not yet been run
// and returns them as a slice of models.MigrationVersion. The last migration version (lastValidVersion) that was migrated
// before the oldest migration gap version is also returned as this is usefull for the fix command
func (m *Migrator) FindMigrationGaps(mvs []models.MigrationVersion, currentVersion string) (migrationGaps []models.MigrationVersion, lastValidVersion string) {
	migrationGaps = make([]models.MigrationVersion, 0)
	lastValidVersion = ""
	for _, mv := range mvs {
		if mv.Version >= currentVersion {
			break
		}

		if !mv.ExistsInDB {
			migrationGaps = append(migrationGaps, mv)
		}
		if len(migrationGaps) == 0 {
			lastValidVersion = mv.Version
		}
	}

	return
}

// Migrate migrates a db from the current version to the specified toVersion
func (m *Migrator) Migrate(command, toVersion string) error {
	funcPrefix := "migrate"
	var msg string
	if command != COMMAND_UP && command != COMMAND_DOWN && command != COMMAND_GOTO {
		return fmt.Errorf(funcPrefix + " - %q is not a valid migration command", command)
	}

	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	defer func(){
		m.App.Infolog.Println(funcPrefix + " - closing DB")
		m.DBRepository.CloseDB()
	}()

	m.App.Infolog.Println(funcPrefix + " - successfully connected to DB")

	err = m.DBRepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}		
	
	mvs, err := m.GetMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	// Get current version from db
	currentVersion, err := m.DBRepository.CurrentVersion()
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	if toVersion == "" && command == COMMAND_UP {
		for i := len(mvs) -1; i >= 0; i-- {
			if mvs[i].FileExists(command) {
				toVersion = mvs[i].Version
				msg = fmt.Sprintf("migrating up to version %s", toVersion)
				fmt.Println(msg)			
				m.App.Infolog.Println(funcPrefix + " - " +msg)
				break
			}
		}
	}

	// Check if toVersion exists
	found := command == COMMAND_DOWN && toVersion == ""
	if !found {
		for _, mv := range mvs {
			if mv.Version == toVersion {
				found = true
				break
			}
		}
	}

	if !found {
		return fmt.Errorf(funcPrefix + " - migration version %s not found", toVersion)
	}	
	
	var migrationDirection string	
	if toVersion > currentVersion {
		migrationDirection = DIRECTION_UP
	} else {
		migrationDirection = DIRECTION_DOWN
	}	

	if toVersion == currentVersion {
		msg = "db already migrated to the newest version"
		fmt.Println(msg)
		m.App.Infolog.Println(funcPrefix + " - " + msg)
		return nil
	}
	// Determine migration direction. If e.g. version > current version then an up is required
	if command != COMMAND_GOTO && command != migrationDirection {
		if command == COMMAND_UP {
			return fmt.Errorf(funcPrefix + " - up migration now allowed because the current db version is higher than %s", toVersion)
		} else {
			return fmt.Errorf(funcPrefix + " - down migration now allowed because the current db version is lower than %s", toVersion)
		}
	}

	if command == COMMAND_UP {
		migrationGaps, _ := m.FindMigrationGaps(mvs, currentVersion)
		if len(migrationGaps) > 0 {
			return fmt.Errorf(funcPrefix + " - up migrations not allowed when all older migrations have not been run")
		}
	}

	// Find all migration files between the current version (excluded) and the new version (included)
	migrationsToRun, err := m.GetMigrationsToRun(mvs, currentVersion, toVersion, migrationDirection)
	if err != nil {
		return fmt.Errorf(funcPrefix + " - %s", err)
	}

	if len(migrationsToRun) > 0 && migrationDirection == DIRECTION_DOWN && !m.confirmationProvided {
		err := m.GetConfirmation(`please type 'yes' to continue or 'no' to cancel`, []string{"yes"})
		if err != nil {
			return fmt.Errorf(funcPrefix + " - %s", err)
		}
	}

	for _, mv := range migrationsToRun {
		data, err := ioutil.ReadFile(filepath.Join(m.path, mv.Filename(migrationDirection)))
		if err != nil {
			return fmt.Errorf(funcPrefix + " - %s", err)
		}
	
		msg = fmt.Sprintf("running %s migration %s", migrationDirection, mv.Filename(migrationDirection))
		fmt.Print(msg)
		err = m.DBRepository.MigrateData(mv.Version, string(data), migrationDirection)
		if err != nil {
			return fmt.Errorf(funcPrefix + " - %s", err)
		}	
		fmt.Println(" - success")
		msg = funcPrefix + " - " + msg + " - success"
		m.App.Infolog.Println(msg)
	}

	return nil
}

// Up migrates a DB up to the version if the version is higher than the current version
func (m Migrator) Up(toVersion string) error {
	return m.Migrate(COMMAND_UP, toVersion)
}

// Down migrates a DB down to the version if the version is lower than the current version
func (m Migrator) Down(toVersion string) error {
	return m.Migrate(COMMAND_DOWN, toVersion)
}

// Goto migrates a DB to the migration specified by version
func (m Migrator) Goto(toVersion string) error {
	if toVersion == "" {
		return errors.New("goto - goto migrations require a to version to be specified")
	}

	return m.Migrate(COMMAND_GOTO, toVersion)
}

// CurrentVersion returns the current db migration version
func (m Migrator) CurrentVersion() (string, error) {
	version, err := m.DBRepository.CurrentVersion()
	if err != nil {
		return "", fmt.Errorf("currentVersion - %s", err)
	}

	return version, nil
}