package main

import (
	"database/sql"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
	"log"
	"sync"
	"time"
)

func checkErr(remark string, err error) {
	if err != nil {
		log.Println(remark + ":" + err.Error())
	}
}

var wg sync.WaitGroup

func operation(db *sql.DB) {
	_, err := db.Exec("create table if not exists user_test(id int, name varchar,age int, primary key(id))")
	checkErr("create table", err)

	// 添加数据
	_, err = db.Exec("upsert into user_test(id,name,age) values(?,?,?)", 1, "zhangsan", 17)
	_, err = db.Exec("upsert into user_test(id,name,age) values(?,?,?)", 2, "lisi", 18)
	_, err = db.Exec("upsert into user_test(id,name,age) values(?,?,?)", 3, "wanger", 19)
	checkErr("insert", err)

	var id int
	var name string
	var age int
	// 查询数据
	querySql := "select * from user_test"
	rows, err := db.Query(querySql)
	fmt.Println(querySql)
	checkErr("select", err)
	for rows.Next() {
		err = rows.Scan(&id, &name, &age)
		checkErr("scan", err)
		fmt.Println("id:", id, "name:", name, "age:", age)
	}
	checkErr("close rows", rows.Close())
	time.Sleep(3 * time.Minute)
	wg.Done()
}

func main() {
	databaseUrl := "http://localhost:30060" // 这里的链接地址与lindorm-cli的链接地址比，需要去掉http之前的字符串
	conn := avatica.NewConnector(databaseUrl).(*avatica.Connector)
	conn.Info = map[string]string{
		"user":     "test",    // 数据库用户名
		"password": "test",    // 数据库密码
		"database": "default", // 初始化连接指定的默认database
	}
	// sql.DB本身就是一个pool的设计，唯一的缺点是没有 连接探活机制
	db := sql.OpenDB(conn)
	db.SetMaxIdleConns(10) // 闲置连接数
	db.SetMaxOpenConns(20) // 最大连接数
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go operation(db)
	}
	// 数据库端连接数为20
	fmt.Println("开始阻塞")
	wg.Wait()
	fmt.Println("任务执行结束,解除阻塞")
	// 三分钟后所有连接被数据库丢弃，此时连接数为0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go operation(db)
	}
	// 数据库端连接数又为20，使用的是同一个sql.DB实例：db
	fmt.Println("开始阻塞_2")
	wg.Wait()
	fmt.Println("任务执行结束,解除阻塞_2")
}
