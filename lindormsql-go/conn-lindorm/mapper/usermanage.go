package mapper

import (
	"database/sql"
	"log"
)

type UserManager interface {
	Connect(string) error
	Add(int, string, int) error
	Delete(int) error
	Update(int, string, int) error
	Get(int) (int, string, int, error)
}
type UserManagerImpl struct {
	db *sql.DB
}

func (u *UserManagerImpl) Connect(url string) error {
	var err error
	u.db, err = sql.Open("avatica", url)
	if err != nil {
		log.Fatalf("error connecting: %s", err.Error())
		return err
	}
	_, err = u.db.Exec("create table if not exists user_test(id int, name varchar,age int, primary key(id))")
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
	rows.Next()
	err = rows.Scan(&id2, &name, &age)
	err = rows.Close()
	return id2, name, age, err
}
