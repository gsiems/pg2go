package meta

import (
	"github.com/jmoiron/sqlx"
)

// DB contains an sqlx database connection
type DB struct {
	*sqlx.DB
}

// OpenDB opens a database connection and returns a DB reference.
func OpenDB(dsn string) (*DB, error) {
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db}, db.Ping()
}

// CloseDB closes a DB reference.
func (db *DB) CloseDB() error {
	return db.DB.Close()
}
