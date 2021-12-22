package main

import (
	"database/sql"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
)

func main() {
	c := avatica.NewConnector("http://localhost:30060").(*avatica.Connector)
	c.Info = map[string]string{
		"user":     "sql",
		"password": "test",
		"database": "default",
	}

	db := sql.OpenDB(c)
	db.Exec("CREATE TABLE if not exists tst (id INT, flt FLOAT, bool BOOLEAN, str VARCHAR, primary key(id))")
	db.Exec("UPSERT INTO tst (id, flt, bool, str) VALUES (0, 0.0, false, 'xx')")
	rows, _ := db.Query("SELECT * FROM tst")
	defer rows.Close()
	fmt.Println("Rows :", rows)
}
