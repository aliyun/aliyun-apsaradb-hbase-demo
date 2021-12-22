package main

import (
	"conn-lindorm/mapper"
	"fmt"
)

func main() {
	u := mapper.NewUserManager()
	err := u.Connect("http://localhost:30060", "sql", "test", "default")
	if err != nil {
		fmt.Println("connect error:", err.Error())
		return
	}
	_ = u.Add(1, "zhangsan", 17)
	_ = u.Add(2, "lisi", 18)
	_ = u.Add(3, "wanger", 19)
	_ = u.Delete(2)
	_ = u.Update(1, "zhangsan2", 27)
	fmt.Println(u.Get(1))
}
