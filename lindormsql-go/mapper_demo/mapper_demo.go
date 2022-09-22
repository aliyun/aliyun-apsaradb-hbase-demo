package main

import (
    "database/sql"
    "errors"
    "fmt"
    avatica "github.com/apache/calcite-avatica-go/v5"
    "lindorm-sql-demo/util"
    "log"
    "time"
)

type UserManager interface {
    Connect(string, string, string, string) error
    Add(int, string, int) error
    Delete(int) error
    Update(int, string, int) error
    Get(int) (int, string, int, error)
}
type UserManagerImpl struct {
    db *sql.DB
}

func NewUserManager() UserManager {
    return &UserManagerImpl{}
}

func (u *UserManagerImpl) Connect(url, user, password, database string) error {
    c := avatica.NewConnector(url).(*avatica.Connector)
    c.Info = map[string]string{
        "user":     user,
        "password": password,
        "database": database,
    }

    db := sql.OpenDB(c)
    // 连接最大空闲时间， 可以根据实际情况调整
    db.SetConnMaxIdleTime(8 * time.Minute)
    // 连接池中允许的最大连接数， 可以根据实际情况调整
    db.SetMaxOpenConns(20)
    // 连接池中允许的最大空闲连接数量, 可以根据实际情况调整
    db.SetMaxIdleConns(2)

    u.db = db
    _, err := u.db.Exec("create table if not exists user_test(id int, name varchar,age int, primary key(id))")
    if err != nil {
        log.Fatalf("error creating: %s", err.Error())
        return err
    }
    return nil
}

func (u *UserManagerImpl) Add(id int, name string, age int) error {
    _, err := u.db.Exec("upsert into user_test(id,name,age) values(?,?,?)", id, name, age)
    return err
}
func (u *UserManagerImpl) Delete(id int) error {
    _, err := u.db.Exec("delete from user_test where id=?", id)
    return err
}

func (u *UserManagerImpl) Update(id int, name string, age int) error {
    _, err := u.db.Exec("upsert into user_test(id,name,age) values(?,?,?)", id, name, age)
    return err
}

func (u *UserManagerImpl) Get(id int) (id2 int, name string, age int, err error) {
    rows, err := u.db.Query("select * from user_test where id=?", id)
    util.CheckErr("scan", err)
    defer rows.Close()
    if rows.Next() {
        err = rows.Scan(&id2, &name, &age)
        return id2, name, age, err
    }
    err = errors.New(fmt.Sprintf("empty result with id = %d", id))
    return
}

func main() {
    u := NewUserManager()
    err := u.Connect("http://localhost:30060", "sql", "test", "default")
    util.CheckErr("connect", err)
    err = u.Add(1, "zhangsan", 17)
    util.CheckErr("add", err)
    err = u.Add(2, "lisi", 18)
    util.CheckErr("add", err)
    err = u.Add(3, "wanger", 19)
    util.CheckErr("add", err)
    err = u.Delete(2)
    util.CheckErr("add", err)
    err = u.Update(1, "zhangsan2", 27)
    util.CheckErr("add", err)
    id, name, age, err := u.Get(1)
    util.CheckErr("get", err)
    fmt.Println(id, name, age)
}
