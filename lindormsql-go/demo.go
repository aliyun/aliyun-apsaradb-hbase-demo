package main

import (
	"database/sql"
	"fmt"
	"log"
)

func main() {
db, err := sql.Open("avatica", "地址")
if err != nil {
		log.Fatalf("error connecting: %s", err.Error())
	}
db.Exec("CREATE TABLE if not exists tst (id INT, flt FLOAT, bool BOOLEAN, str VARCHAR, primary key(id))")
db.Exec("UPSERT INTO tst (id, flt, bool, str) VALUES (0, 0.0, false, 'xx')")
rows, err:= db.Query("SELECT * FROM tst")
defer rows.Close()
fmt.Println("Rows :", rows)
}
