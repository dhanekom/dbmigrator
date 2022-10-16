package dbrepo

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDBDriver struct {

}

func (d *MySQLDBDriver) Open(dbConnData DBConnectionData) (*sql.DB, error){
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?autocommit=true&loc=Local&multiStatements=true", dbConnData.DBUser, dbConnData.DBPassword, dbConnData.DBHost, dbConnData.DBPort, dbConnData.DBName)
	myDB, err := sql.Open("mysql", dataSourceName)
	if err != nil {
	  return nil, err
	}

	return myDB, nil
}

func (d *MySQLDBDriver) SetupMigrationTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS schema_migration (
		version varchar(15) NOT null,
		UNIQUE INDEX schema_migration_version_idx (version)
	);`
}

func (d *MySQLDBDriver) MigrateDBSQL(migrationDirection string) (string, error) {
  switch strings.ToLower(migrationDirection) {
	case "up":
		return `insert into schema_migration (version) values (?)`, nil
	case "down":
		return `delete from schema_migration where version = ?`, nil
	default:
		return "", errors.New("migrationDirection  - migration direction must be up or down")
	}
}

func (d *MySQLDBDriver) CurrentVersionSQL() string {
	return `select coalesce(max(version), '') as version from schema_migration`
}

func (d *MySQLDBDriver) MigratedVersionsSQL() string {
	return `select version from schema_migration order by version`
}