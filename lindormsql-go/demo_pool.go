package main

import (
	"context"
	"database/sql"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// ProbeStmt 定时探活
type ProbeStmt struct {
	db       *sql.DB
	interval time.Duration // 探活时间间隔
	mu       sync.RWMutex  // 保护字段，并发情况下使用
	sql      string        // 探活执行的sql
	num      int           // 执行sql的并发数
	isOpen   bool
	stop     func() // 停止探活
}

func (as *ProbeStmt) SetInterval(d time.Duration) {
	if d < 0 {
		d = time.Minute
	}
	as.interval = d
}

func (as *ProbeStmt) SetSql(s string) {
	if s == "" {
		s = "show databases"
	}
	as.sql = s
}
func (as *ProbeStmt) SetDb(db *sql.DB) {
	as.db = db
}

func (as *ProbeStmt) SetNum(n int) {
	if n < 0 {
		n = 1
	}
	as.num = n
}

func (as *ProbeStmt) Open(ctx context.Context) error {
	if as.isOpen {
		return fmt.Errorf("probe is open")
	}
	if as.db == nil {
		return fmt.Errorf("db is null")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if as.interval == 0 {
		as.interval = time.Minute
	}
	if as.sql == "" {
		as.sql = "show databases"
	}
	if as.num == 0 {
		as.num = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	as.stop = cancel
	as.isOpen = true
	go as.aliveTask(ctx)
	return nil
}

func (as *ProbeStmt) aliveTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(as.interval):
			log.Println("________________________________探活执行 ", as.sql, as.interval)
			// 比如想保证n个连接空闲时间是活的，那就并发执行n个Exec
			for i := 0; i < as.num; i++ {
				go func() {
					_, err := as.db.Exec(as.sql)
					if err != nil {
						return
					}
				}()
			}
		}
	}
}

func (as *ProbeStmt) Close() {
	log.Println("________________________________探活关闭 ")
	as.stop()
	as.isOpen = false
}

func checkErr(remark string, err error) {
	if err != nil {
		log.Println(remark + ":" + err.Error())
	}
}

var wg sync.WaitGroup

// 推荐使用prepare方式
func operation(db *sql.DB, ts int) {
	_, err := db.Exec("create table if not exists user_test(id int, name varchar,age int, primary key(id))")
	checkErr("create table", err)

	// 添加数据
	stmt, err := db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
	checkErr("prepare upsert", err)
	_, err = stmt.Exec(1, "zhangsan", 17)
	_, err = stmt.Exec(2, "lisi", 18)
	_, err = stmt.Exec(3, "wanger", 19)
	checkErr("upsert stmt exec", err)
	checkErr("upsert close statement", stmt.Close())

	var id int
	var name string
	var age int
	// 查询数据， 并发性高，请改成prepare方式
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
	time.Sleep(time.Duration(ts) * time.Second)
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
	conn.Client = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			IdleConnTimeout:       10 * time.Minute, // 连接空闲丢弃时间,了解更多请参考结构体Transport的定义
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			/*
				注意这个很关键，测试发现这个值会限制连接数量，导致下面的设置无效，这里大一点关系不大，但不能不设置
				其它字段比如MaxIdleConns,不设置或者0值，意味着不会限制连接数量，那就不设置好了
			*/
			MaxIdleConnsPerHost: 10000,
		},
	}
	// sql.DB本身就是一个pool的设计，唯一的缺点是没有 连接探活机制
	db := sql.OpenDB(conn)

	db.SetMaxIdleConns(128) // 闲置连接数
	db.SetMaxOpenConns(128) // 最大连接数（不要太大，过多的连接服务端不会建立，不要太小，数据库机器cpu使用率会不高，数值可根据数据库机器配置来调节）

	// 定时探活并新建连接适合这样的场景：长时间空闲，有突发流量，希望降低突发流量的rt。其它场景下，即使连接失活也没关系，db在执行语句时如果连接不可用，会丢弃并创建新的连接
	// 在连接忙的时候，探活机制不应改参与竞争连接，这是下一步优化的方向，不过如果探活周期长，执行消耗小关系就不是很大
	as := &ProbeStmt{db: db, interval: 25 * time.Second, num: 20} // interval小于连接空闲丢弃时间
	err := as.Open(nil)
	checkErr("the probe mechanism", err)
	defer as.Close()

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go operation(db, 0)
	}
	fmt.Println("开始阻塞")
	wg.Wait()
	fmt.Println("任务执行结束,解除阻塞")

}
