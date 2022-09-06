package main

import (
	"database/sql"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
	"log"
	"time"
)

func checkPrepareErr(remark string, err error) {
	if err != nil {
		log.Println(remark + ":" + err.Error())
	}
}

func main() {
	databaseUrl := "http://localhost:30060" // 这里的链接地址与lindorm-cli的链接地址比，需要去掉http之前的字符串
	conn := avatica.NewConnector(databaseUrl).(*avatica.Connector)
	conn.Info = map[string]string{
		"user":     "sql",     // 数据库用户名
		"password": "test",    // 数据库密码
		"database": "default", // 初始化连接指定的默认database
	}
	db := sql.OpenDB(conn)
	// 连接最大空闲时间， 可以根据实际情况调整
	db.SetConnMaxIdleTime(8 * time.Minute)
	// 连接池中允许的最大连接数， 可以根据实际情况调整
	db.SetMaxOpenConns(20)
	// 连接池中允许的最大空闲连接数量, 可以根据实际情况调整
	db.SetMaxIdleConns(2)

	// 创建表
	stmt, err := db.Prepare("create table if not exists user_test(id int, name varchar,age int, primary key(id))")
	checkPrepareErr("prepare create", err)
	_, err = stmt.Exec()
	checkPrepareErr("create stmt exec", err)
	checkPrepareErr("create close statement", stmt.Close())

	// 添加数据
	stmt, err = db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
	checkPrepareErr("prepare upsert", err)
	_, err = stmt.Exec(1, "zhangsan", 17)
	_, err = stmt.Exec(2, "lisi", 18)
	_, err = stmt.Exec(3, "wanger", 19)
	checkPrepareErr("upsert stmt exec", err)
	checkPrepareErr("upsert close statement", stmt.Close())

	// 删除数据
	stmt, err = db.Prepare("delete from user_test where id=?")
	checkPrepareErr("prepare delete", err)
	_, err = stmt.Exec(2)
	checkPrepareErr("delete stmt exec", err)
	checkPrepareErr("delete close statement", stmt.Close())

	// 修改数据
	stmt, err = db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
	checkPrepareErr("prepare update", err)
	_, err = stmt.Exec(1, "zhangsan", 7)
	checkPrepareErr("update stmt exec", err)
	checkPrepareErr("update close statement", stmt.Close())

	// 获取一行数据
	stmt, err = db.Prepare("select * from user_test where id=?")
	checkPrepareErr("prepare select one", err)
	row := stmt.QueryRow(1)
	var id int
	var name string
	var age int
	err = row.Scan(&id, &name, &age)
	checkPrepareErr("scan", err)
	checkPrepareErr("select close statement", stmt.Close())
	fmt.Println("id:", id, "name:", name, "age:", age)

	// 查询数据
	querySql := "select * from user_test"
	stmt, err = db.Prepare(querySql)
	checkPrepareErr("prepare select", err)
	rows, err := stmt.Query()
	fmt.Println(querySql)
	checkPrepareErr("select", err)
	for rows.Next() {
		err = rows.Scan(&id, &name, &age)
		checkPrepareErr("scan", err)
		fmt.Println("id:", id, "name:", name, "age:", age)
	}
	checkPrepareErr("close rows", rows.Close())
	checkPrepareErr("select close statement", stmt.Close())
}
