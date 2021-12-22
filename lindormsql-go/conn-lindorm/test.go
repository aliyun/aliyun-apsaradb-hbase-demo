package main

import (
	"conn-lindorm/mapper"
	"fmt"
	_ "github.com/apache/calcite-avatica-go/v5"
)

func main() {
	var u mapper.UserManager = &mapper.UserManagerImpl{}
	err := u.Connect("http://localhost:30060/default?user=sql&password=test")
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
