package models

import (
	"fmt"
	"testing"
)

const (
	TestPglogicalConstring       = "host=127.0.0.1 user=testuser password=example dbname=vadym sslmode=disable"
	TestPglogicalReplicationSlot = "test_replication"
)

func TestPGLogicalStreamObjectIsConnected(t *testing.T) {
	plg := FactoryPGLogicalInputStream(TestPglogicalConstring, TestPglogicalReplicationSlot)
	if err := plg.db.Ping(); err != nil {
		t.Error(err)
	}
}

func TestGetLRDataAsStringToConsoleFromWal2JsonStreamRuns(t *testing.T) {
	plg := FactoryPGLogicalInputStream(TestPglogicalConstring, TestPglogicalReplicationSlot)
	plg.getLRDataAsStringToConsoleFromWal2JsonStream()
}

func TestChannelWithLogicalReplicationDataReceivesValues(t *testing.T) {
	plg := FactoryPGLogicalInputStream(TestPglogicalConstring, TestPglogicalReplicationSlot)
	lrDataChan := plg.GetChannelInputStream()

	for i := range lrDataChan {
		fmt.Println("RECEIVED VIA CHANNEL: ", i)
	}
}
