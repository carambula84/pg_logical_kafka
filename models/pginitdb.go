package models

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

//initDB Initializes connection
func initDB(constr string) *sql.DB {
	var err error
	db, err := sql.Open("postgres", constr)
	if err != nil {
		log.Panic(err)
	}

	if err := db.Ping(); err != nil {
		fmt.Println("Could not connect to Postgres")
		log.Panic(err)
	}
	fmt.Println("connected")

	return db
}

//reconnect tries to reconnect untill sucess
func reconnect(db *sql.DB, constr string) {
	go func() {
		for {
			//fmt.Println("....checking connection")
			if err := db.Ping(); err != nil {
				//fmt.Println("connection lost, reconnecting...")
				initDB(constr)
				fmt.Println("reconnected")
			}
		}
	}()
}
