package dbrepo

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dhanekom/dbmigrator/config"
)

const (
	DBDRIVER_POSTGRES = "POSTGRES"
)

type DBConnectionData struct {
	DBHost string
	DBPort string
	DBName string
	DBUser string
	DBPassword string
	DBSSL string
}

type DBDriver interface {
	Open(dbConnData DBConnectionData) (*sql.DB, error)
	SetupMigrationTableSQL() string
	MigrateDBSQL(migrationDirection string) (string, error)
	CurrentVersionSQL() string
	MigratedVersionsSQL() string
}

type DBRepo struct {
	app *config.AppConfig
	driver DBDriver
	connectionData DBConnectionData
	db *sql.DB
}

func NewDBRepo(dbdrivername string, connData DBConnectionData, a *config.AppConfig) (*DBRepo, error) {
	dbdrivername = strings.ToUpper(dbdrivername)
	dbrepo := DBRepo{
		app: a,
	}
	switch dbdrivername {
	case DBDRIVER_POSTGRES:
		dbrepo.driver = &PostgresDBDriver{}
		dbrepo.connectionData = connData
	default:
		return nil, fmt.Errorf("ConnectToDB - %q is not a valid DB Driver Name. Value must be one of the following (%s)", dbdrivername, DBDRIVER_POSTGRES)
	}	

	return &dbrepo, nil
}

func (r *DBRepo) ConnectToDB() (error) {
	myDB, err := r.driver.Open(r.connectionData)
	if err != nil {
		return fmt.Errorf("ConnectToDB - %s", err)
	}

	myDB.SetMaxOpenConns(10)
	myDB.SetConnMaxIdleTime(5)
	myDB.SetConnMaxLifetime(5 & time.Minute)

	err = myDB.Ping()
	if err != nil {
		return fmt.Errorf("ConnectToDB - %s", err)
	}

	r.db = myDB
	return nil
}

func (r *DBRepo) CloseDB() error {
	return r.db.Close()
}

func (r DBRepo) SetupMigrationTable() error {
	_, err := r.db.Exec(r.driver.SetupMigrationTableSQL())
	if err != nil {
		return fmt.Errorf("SetupMigrationTable - %s", err)
	}

	return nil
}

func (r DBRepo) migrateDB(toVersion, migrationDirection string) error {
	stmt, err := r.driver.MigrateDBSQL(migrationDirection)
	if err != nil {
		return fmt.Errorf("migrateDB - %s", err)
	}	

	_, err = r.db.Exec(stmt, toVersion)
	if err != nil {
		return fmt.Errorf("migrateDB - %s", err)
	}

	return nil	
}

func (r DBRepo) MigrateData(toVersion, script, migrationDirection string) error {
	migrationDirection = strings.ToLower(migrationDirection)

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("migrateData - %s", err)
	}
	defer tx.Rollback()

	_, err = r.db.Exec(script)
	if err != nil {
		return fmt.Errorf("migrateData - version %s - %s", toVersion, err)
	}

	err = r.migrateDB(toVersion, migrationDirection)
	if err != nil {
		return fmt.Errorf("migrateData - version %s - Admin script - %s", toVersion, err)
	}

  if err = tx.Commit(); err != nil {
		return fmt.Errorf("migrateData - Commit - %s", err)
	}

	return nil	
}

func (r DBRepo) CurrentVersion() (string, error) {
	var version string
	row := r.db.QueryRow(r.driver.CurrentVersionSQL())
	if row.Err() == sql.ErrNoRows {
		return "", nil
	}

	err := row.Scan(&version)
	if err != nil {
		return "", fmt.Errorf("CurrentVersion - %s", err)
	}

	return version, nil
}

func (r DBRepo) MigratedVersions() ([]string, error) {
	var result []string
	rows, err := r.db.Query(r.driver.MigratedVersionsSQL())
	if err != nil {
		return result, fmt.Errorf("MigratedVersions - %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return result, fmt.Errorf("MigratedVersions - %s", err)
		}

		result = append(result, version)
	}

	return result, nil
}
