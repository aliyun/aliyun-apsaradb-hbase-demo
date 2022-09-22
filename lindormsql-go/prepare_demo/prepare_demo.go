package main

import (
    "database/sql"
    "fmt"
    avatica "github.com/apache/calcite-avatica-go/v5"
    "lindorm-sql-demo/util"
    "time"
)

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
    util.CheckErr("prepare_demo create", err)
    _, err = stmt.Exec()
    util.CheckErr("create stmt exec", err)
    util.CheckErr("create close statement", stmt.Close())

    // 添加数据
    stmt, err = db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
    util.CheckErr("prepare_demo upsert", err)
    _, err = stmt.Exec(1, "zhangsan", 17)
    _, err = stmt.Exec(2, "lisi", 18)
    _, err = stmt.Exec(3, "wanger", 19)
    util.CheckErr("upsert stmt exec", err)
    util.CheckErr("upsert close statement", stmt.Close())

    // 删除数据
    stmt, err = db.Prepare("delete from user_test where id=?")
    util.CheckErr("prepare_demo delete", err)
    _, err = stmt.Exec(2)
    util.CheckErr("delete stmt exec", err)
    util.CheckErr("delete close statement", stmt.Close())

    // 修改数据
    stmt, err = db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
    util.CheckErr("prepare_demo update", err)
    _, err = stmt.Exec(1, "zhangsan", 7)
    util.CheckErr("update stmt exec", err)
    util.CheckErr("update close statement", stmt.Close())

    // 获取一行数据
    stmt, err = db.Prepare("select * from user_test where id=?")
    util.CheckErr("prepare_demo select one", err)
    row := stmt.QueryRow(1)
    var id int
    var name string
    var age int
    err = row.Scan(&id, &name, &age)
    util.CheckErr("scan", err)
    util.CheckErr("select close statement", stmt.Close())
    fmt.Println("id:", id, "name:", name, "age:", age)

    // 查询数据
    querySql := "select * from user_test"
    stmt, err = db.Prepare(querySql)
    util.CheckErr("prepare_demo select", err)
    rows, err := stmt.Query()
    fmt.Println(querySql)
    defer rows.Close()
    util.CheckErr("select", err)
    for rows.Next() {
        err = rows.Scan(&id, &name, &age)
        util.CheckErr("scan", err)
        fmt.Println("id:", id, "name:", name, "age:", age)
    }
    util.CheckErr("select close statement", stmt.Close())
}
