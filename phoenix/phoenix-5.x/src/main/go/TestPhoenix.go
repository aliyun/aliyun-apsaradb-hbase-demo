package main

import "database/sql"
import "fmt"
import _ "github.com/apache/calcite-avatica-go/v3"

// 运行程序前需要安装phoenix-go驱动，详见https://github.com/apache/calcite-avatica-go
func main() {
  // localhost修改为运行环境的QueryServer主机名或IP
  db, err := sql.Open("avatica", "http://localhost:8765")
  checkErr(err)
  db.Exec("drop table if exist go_test")
  db.Exec("create table go_test(id integer primary key, name varchar)")
  db.Exec("create index go_test_idx on go_test(name)")

  // 使用Prepare批量写入数据
  stm,_ := db.Prepare("upsert into go_test values(?, ?)")
  stm.Exec(111, "admin")
  stm.Exec(112, "root")

  rows, err := db.Query("select * from go_test")
  checkErr(err)
  for rows.Next() {
        var id int
        var name string
        err := rows.Scan(&id, &name)
        checkErr(err)
        fmt.Printf("id : %d\n", id)
        fmt.Printf("username : %s\n", name)
  }
  db.Close()
}

func checkErr(err error) {
  if err != nil {
        panic(err)
  }
}