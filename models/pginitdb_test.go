package models

import (
	"database/sql"
	"log"
	"testing"
)

const testConstring = "host=127.0.0.1 user=testuser password=example dbname=vadym sslmode=disable"

func TestDBShouldConnect(t *testing.T) {
	db, err := sql.Open("postgres", testConstring)
	if err != nil {
		log.Panic(err)
		t.Error(err)
	}

	if err = db.Ping(); err != nil {
		t.Error(err)
	}
}

func TestInitWrongShouldNotConnect(t *testing.T) {
	_, err := sql.Open("postgres", testConstring)
	if err != nil {
		t.Error("this should not connect")
	}
}

func TestInitDBfuncRuns(t *testing.T) {
	db := initDB(testConstring)
	if err := db.Ping(); err != nil {
		t.Error(err)
	}
}

func TestReconnectRuns(t *testing.T) {
	db := initDB(testConstring)
	reconnect(db, testConstring)
}
