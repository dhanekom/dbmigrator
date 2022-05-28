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
}

const (
	COMMAND_CREATE = "create"
	COMMAND_UP = "up"
	COMMAND_DOWN = "down"
	COMMAND_FORCE = "force"
	COMMAND_LIST = "list"
	COMMAND_VERSION = "version"
	COMMAND_FIX = "fix"
	DIRECTION_UP = "up"
	DIRECTION_DOWN = "down"
)

var (
	re = regexp.MustCompile(`^(\d{8}_\d{6})_(\w+)\.(down|up)\.sql$`)
)

func NewMigrator(path string, db *dbrepo.DBRepo, a *config.AppConfig) (*Migrator, error) {
	result := Migrator{
		path: path,
		DBRepository:  db,
		App: a,
	}

	return &result, nil
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

func (m Migrator) GetMigrationVersionInfo() ([]models.MigrationVersion, error) {
	m.App.Infolog.Println("gathering migration version info")
	result := make([]models.MigrationVersion, 0)
	mvs := make(map[string]*models.MigrationVersion, 0)
	files, err := ioutil.ReadDir(m.path)
	if err != nil {
		return nil, fmt.Errorf("getMigrationVersionInfo - getting migration filenames - %s", err)
	}

	for _, file := range files {
		matches := re.FindAllStringSubmatch(file.Name(), -1)

		if matches == nil {
			m.App.Infolog.Printf("getMigrationVersionInfo - unable to parse seperate parts of filename %s\n", file.Name())
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
				return nil, fmt.Errorf("more than one up migration file found for migration version %s", mv.Version)
			}
			mv.UpFileExists = true
		} else if direction == DIRECTION_DOWN {
			if mv.DownFileExists {
				return nil, fmt.Errorf("more than one down migration file found for migration version %s", mv.Version)
			}
			mv.DownFileExists = true
		}
	}

	migratedVersions, err := m.DBRepository.MigratedVersions()
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

func (m Migrator) GetMigrationsToRun(mvs []models.MigrationVersion, currentVersion, toVersion, migrationDirection string) ([]models.MigrationVersion, error) {
	m.App.Infolog.Println("determining migrations to run")
	result := make([]models.MigrationVersion, 0)

	if migrationDirection == DIRECTION_UP && currentVersion >= toVersion {
		return result, fmt.Errorf("getMigrationsToRun - to version must be higher than the current version")
	} else if migrationDirection == DIRECTION_DOWN && toVersion >= currentVersion {
		return result, fmt.Errorf("getMigrationsToRun - to version must be lower than the current version")
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

func (m *Migrator) Migrate(command, toVersion string) error {
	var msg string
	if command != COMMAND_UP && command != COMMAND_DOWN && command != COMMAND_FORCE {
		return fmt.Errorf("migrate - %q is not a valid migration command", command)
	}

	if command == COMMAND_FORCE && toVersion == "" {
		return errors.New("migrate - force migrations require a to version to be specified")
	}

	m.App.Infolog.Println("connecting to DB")
	err := m.DBRepository.ConnectToDB()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	defer func(){
		m.App.Infolog.Println("migrate - closing DB")
		m.DBRepository.CloseDB()
	}()

	m.App.Infolog.Println("Migrate - successfully connected to DB")

	err = m.DBRepository.SetupMigrationTable()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}		
	
	// Get current version from db
	currentVersion, err := m.DBRepository.CurrentVersion()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	mvs, err := m.GetMigrationVersionInfo()
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	if toVersion == "" && command == COMMAND_UP {
		for i := len(mvs) -1; i >= 0; i-- {
			if mvs[i].FileExists(command) {
				toVersion = mvs[i].Version
				msg = fmt.Sprintf("migrating up to version %s", toVersion)
				fmt.Println(msg)			
				m.App.Infolog.Println("migrate - " +msg)
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
		msg = "db already migrated to the newest version"
		fmt.Println(msg)
		m.App.Infolog.Println("migrate - " + msg)
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

	// Find all migration files between the current version (excluded) and the new version (included)
	migrationsToRun, err := m.GetMigrationsToRun(mvs, currentVersion, toVersion, migrationDirection)
	if err != nil {
		return fmt.Errorf("migrate - %s", err)
	}

	for _, mv := range migrationsToRun {
		data, err := ioutil.ReadFile(filepath.Join(m.path, mv.Filename(migrationDirection)))
		if err != nil {
			return fmt.Errorf("migrate - %s", err)
		}
	
		msg = fmt.Sprintf("running migration %s", mv.Filename(migrationDirection))
		fmt.Print(msg)
		err = m.DBRepository.MigrateData(mv.Version, string(data), migrationDirection)
		if err != nil {
			return fmt.Errorf("migrate - %s", err)
		}	
		fmt.Println(" - success")
		msg = "migrate - " + msg + " - success"
		m.App.Infolog.Println(msg)
		// fmt.Printf("successfully migrated to version %s\n", mv.Version)		
	}

	return nil
}

// Up migrates a DB up to the version if the version is higher than the current version
func (m Migrator) Up(version string) error {
	return m.Migrate(COMMAND_UP, version)
}

// Down migrates a DB down to the version if the version is lower than the current version
func (m Migrator) Down(version string) error {
	return m.Migrate(COMMAND_DOWN, version)
}

// Force migrates a DB to the migration specified by version
func (m Migrator) Force(toVersion string) error {
	return m.Migrate(COMMAND_FORCE, toVersion)
}

// // Fix determines whether there are gaps inmigrations (up migrations that are older than the currently active migration and were not previously run).
// // Fix will run down migrations to revert the DB version to the point before the oldest gap
// func (m Migrator) Fix() error {

// }

func (m Migrator) CurrentVersion() (string, error) {
	version, err := m.DBRepository.CurrentVersion()
	if err != nil {
		return "", fmt.Errorf("currentVersion - %s", err)
	}

	return version, nil
}