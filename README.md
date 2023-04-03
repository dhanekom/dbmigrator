# dbmigrator

A Go based application that helps you migrate database structures.

![](/docs/images/demo.gif)

## Supported databases

- PostgreSQL
- MySQL

## Features

- Create up and down sql migration files
- Migrate DB structure up or down by running SQL files
- Assist users with fixing migration gaps (E.g. if migrations no 1 and 3 have been executed and after you do a git pull you find a migration no 2 that was added by another user)
- List migration details in a table format
- Configs can be specified by either command line arguments (flags) or by a .env file that is in the same directory as the executable. If configs are specified by both the command line arguments and the .env file then the command line arguments take preference

## Building the cli application

- Download and install Go (Golang): https://go.dev/dl/
- Clone this git repo
- Open terminal and navigate to the root directory of the repository
- Run <code>go mod tidy</code> to pull all dependencies
- Run <code>go build -o [directory or application full path] .\cmd\dbmigrator\\.</code>

## Terminology

| Term            | Description                                                                                                                                                                             |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| version         | A migration version uniquely identifies a migration (a pair of up and down sql migration files). A version refers to a timestamp in the format YYYYMMDD_HHNNSS (e.g. "20220601_124512") |
| current version | The newers (highest) migration version that has been run. This can be found in the schema_migration table                                                                               |
| migration gaps  | Migrations that are older than the current version and have not yet been run. The "fix" command should mostly be able to fix migration gaps                                             |

## Usage

<code>dbmigrator FLAGS COMMAND [arg...]</code>

## Commands

| Command  | Description                                                                               |
| -------- | ----------------------------------------------------------------------------------------- |
| create V | Create up and down migration files with a timestamp and a description (V)                 |
| up [V]   | Applies all up migrations or migrates up to version V                                     |
| down [V] | Applies all down migrations or migrates down to version V                                 |
| goto V   | Migrates up or down to version V                                                          |
| list [N] | Lists all migration details or the last N migrations                                      |
| version  | Lists the current migration version                                                       |
| fix      | Finds older migrations that have not been executed and attempts to run them in a safe way |
| force V  | Sets the current migration version without running any migrations                         |

## Command line flags

| Flag            | ENV file param            | Default       | Description                                                         |
| --------------- | ------------------------- | ------------- | ------------------------------------------------------------------- |
| -dbdriver       | DBMIGRATOR_DB_DRIVER      |               | database driver                                                     |
| -dbname         | DBMIGRATOR_DB_NAME        |               | database name                                                       |
| -dbssl          | DBMIGRATOR_DB_SSL         |               | database sslsettings (disable, prefer, require) (default "disable") |
| -host           | DBMIGRATOR_DB_HOST        |               | database host IP or URL                                             |
| -log_path       | DBMIGRATOR_LOG_PATH       | [appname].log | full path of log file                                               |
| -migration_path | DBMIGRATOR_MIGRATION_PATH |               | directory containing migration files                                |
| -password       | DBMIGRATOR_DB_PASSWORD    |               | database password                                                   |
| -port           | DBMIGRATOR_DB_PORT        |               | database port                                                       |
| -s              | N/A                       | false         | allow command to run without any confirmation prompts               |
| -user           | DBMIGRATOR_DB_USERNAME    |               | database username                                                   |
| N/A             | DBMIGRATOR_ALLOW_FIX      | false         | database username                                                   |

## Command line exit codes

| Code | Description       |
| ---- | ----------------- |
| 0    | Success           |
| 1    | System Error      |
| 2    | Invalid Configs   |
| 3    | Invalid Params    |
| 4    | DB error          |
| 5    | User cancellation |

## Tipical workflows

### Developer workflow

- Add a .env file to the path from which the dbmigrator app will be executed. See the "Command line flags" section below (specifically the "ENV file param" column). Configure these values in the .env file.
  <br><br>
  Developer A - Wants to add a city table to the database
  <br>
- <code>dbmigrator create add_city</code>
- Developer A - Adds e.g. a CREATE TABLE SQL statement to the up migration file and a DROP TABLE SQL statement to the down migration file.
- Developer A - Runs all up migrations that have to yet been run
  <br><code>dbmigrator up</code>
- Developer A - List migrations to check. The "list" command lists all migration files and the "Migrated" column indicates whether a migration has been applied to the database.
  <br><code>dbmigrator list</code>
- Developer A - Commits the new migration files (in the migration_path) to version control and pushed to the remote version control host.
  <br><br>
- Developer B - Pulls from the remote version control (the add_city up and down migration files are pulled)
- Developer B - Upgrades local database (the add_city migration is run and the city table is created)
  <br><code>dbmigrator up</code>
- Developer B - wants to add a country table to the database
  <br><code>dbmigrator create add_country</code>
- Developer B - Adds a CREATE TABLE SQL statement to the up migration file and a DROP TABLE SQL statement to the down migration file.
- Developer B - Runs all up migrations that have to yet been run. The country table will be created.
  <br><code>dbmigrator up</code>

### Production workflow

- Add a .env file to the path from which the dbmigrator app will be executed. See the "Command line flags" section below (specifically the "ENV file param" column). Configure these values in the .env file. Note that all parameters can be configured via command line flags (see below). Also note that command line flags take precedence over .env file parameters.
- Send the up and down migration files to the migration_path (It is import to keep this folder up to date to ensure that migrations are executed in the correct order)
- Upgrade the database structure by running the up migrations.
  <br><code>dbmigrator up</code><br>
  Note: You might want to send migration files that must not yet be run (e.g. if you want to send up and down migration files before updating the application(s) that uses your database). In this case you could upgrade to a specified migration version by running "dbmigrator up [version timestamp]".
  <br>E.g. <code>dbmigrator up 20221015_183738<></code><br>

## Examples

### passing in all configs using flags:

<code>dbmigrator -dbdriver=postgres -host=127.0.0.1 -port=5432 -dbname=testdb -user=testuser -password=testpassword -log_path=c:\temp\log -migration_path=c:\my_app_dir\migrations create</code>

<code>dbmigrator -dbdriver=postgres -host=127.0.0.1 -port=5432 -dbname=testdb -user=testuser -password=testpassword -log_path=c:\temp\log -migration_path=c:\my_app_dir\migrations up 20220601_124512</code>

### create:

<code>dbmigrator create some_description</code> This command will create an up and down sql migration file with names similar to the file names below:

- 20220601_124512_some_description.up.sql
- 20220601_124512_some_description.down.sql

### up:

<code>dbmigrator up</code> runs all migrations with a version that is higher than the current version

<code>dbmigrator up 20220601_124512</code> runs all up migrations with a version that is higher than the current version but only up to the specified version (20220601_124512 in this case)

### down:

<code>dbmigrator down</code> runs all down migrations with a version number that is lower than the current version

<code>dbmigrator down 20220501_120000</code> runs all down migrations with a version number that is lower than the current version but only down to and including the specified version (20220501_120000 in this case)

### goto:

<code>dbmigrator goto 20220501_120000</code> runs up or down migrations to get the db to the specified migration version

runs all down migrations with a version number that is lower than the current version but only down to and including the specified version (20220501_120000 in this case)

### list:

<code>dbmigrator list</code> displays a table that provides an overview of all migration versions including their descriptions, whether they have been run and whether their up and down sql migration files could be found

### version:

<code>dbmigrator version</code> displays the current version

### fix:

<code>dbmigrator fix</code> determines whether there are any migration gaps. If gaps are found all down migrations will be run to get the db on the version before the oldest gap and then all up migrations are run to get the db back the the current version (the current version before the fix command was run)
