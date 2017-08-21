package models

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// PGLogicalStream provides functionality to get stream
// of logical replication data from Postgresql
type PGLogicalStream struct {
	db                      *sql.DB
	constr                  string
	replicationSlot         string
	requestIntervalMillisec int
}

// FactoryPGLogicalInputStream creates object of PGLogicalStream,
// that has channel of data from postgres logical replication stream
func FactoryPGLogicalInputStream(constr string, replicationSlot string, requestIntervalMillisec int) *PGLogicalStream {
	pg := PGLogicalStream{
		db:                      initDB(constr),
		constr:                  constr,
		replicationSlot:         replicationSlot,
		requestIntervalMillisec: requestIntervalMillisec,
	}
	pg.establishReplicationConnection()
	return &pg
}

// create replication slot, if slot does not exist
func (pg *PGLogicalStream) establishReplicationConnection() {
	slotAlreadyExists := false
	rows, err := pg.db.Query("SELECT true FROM pg_replication_slots WHERE slot_name = '" + pg.replicationSlot + "';")
	if err != nil {
		log.Fatal(err)
	}
	rows.Next()
	rows.Scan(&slotAlreadyExists)
	if !slotAlreadyExists {
		pg.db.Exec("SELECT * FROM pg_create_logical_replication_slot('" + pg.replicationSlot + "', 'wal2json');")
	}
}

// Drops the replication slot on postgres
func (pg *PGLogicalStream) GracefullyClose() {
	pg.db.Exec("SELECT pg_drop_replication_slot('" + pg.replicationSlot + "');")
}

/**
In:
@ db - pointer to the connection variable
Out:
@ wal2JsonStream *Message
Creates and returnes channel of (json) Messages from logical replication slot
*/
func (pg *PGLogicalStream) getLRDataAsStringToConsoleFromWal2JsonStream() {
	for {
		// SELECT * FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
		// SELECT * FROM pg_replication_slots;
		rows, _ := pg.db.Query("SELECT * FROM pg_logical_slot_get_changes('" + pg.replicationSlot + "', NULL, NULL)")
		loc, xid, data := "", "", ""
		for rows.Next() {
			if err := rows.Scan(&loc, &xid, &data); err == nil {
				fmt.Println("DEBUG: fetched row: ", data)
			} else {
				fmt.Println(err)
			}
		}
	}
}

// GetChannelInputStream returns a channel for the input stream
// of logical replication data
func (pg *PGLogicalStream) GetChannelInputStream() chan string {
	LRdata := make(chan string)
	loc, xid, data := "", "", ""

	go func() {
		for {
			rows, _ := pg.db.Query("SELECT * FROM pg_logical_slot_get_changes('" + pg.replicationSlot + "', NULL, NULL)")
			for rows.Next() {
				if err := rows.Scan(&loc, &xid, &data); err == nil {
					//fmt.Println("DEBUG: fetched row: ", data)
					LRdata <- data
				} else {
					fmt.Println(err)
				}
			}
			time.Sleep(time.Millisecond * time.Duration(pg.requestIntervalMillisec))
		}
	}()

	return LRdata
}
