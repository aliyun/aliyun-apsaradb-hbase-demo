/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
	"log"
	"strconv"
	"sync"
	"time"
)

type ConnPool struct {
	connList     []*sql.DB // 存放连接,和head、tail组成一个循环队列
	maxOpen      int       // 允许的最大连接数
	muNumOpen    sync.Mutex
	numOpen      int // 已有的连接数量，包括分配出去的
	muNumFree    sync.Mutex
	numFree      int                     // 空闲连接数量
	head         int                     // 头部拿连接
	tail         int                     // 尾部插入连接
	connRequests map[uint64]chan *sql.DB // 阻塞高效实现,和leftRequest、rightRequest共同保证先进先出
	muRequest    sync.Mutex
	leftRequest  uint64
	rightRequest uint64
	info         *ConnInfo

	// 以下为探活字段，探活不能并发开启、关闭
	interval time.Duration // 探活时间间隔
	sql      string        // 探活执行的sql
	isOpen   bool          // 探活是否开启
	stop     func()        // 停止探活
}
type ConnInfo struct {
	url      string
	user     string
	password string
	database string
}

func (cp *ConnPool) GetConn() (*sql.DB, error) {
	// 返回空闲连接
	cp.muNumFree.Lock()
	if cp.numFree > 0 {
		db := cp.connList[cp.head]
		cp.head = (cp.head + 1) % cp.maxOpen
		cp.numFree--
		cp.muNumFree.Unlock()
		return db, nil
	}
	cp.muNumFree.Unlock()
	// 创建新的连接
	cp.muNumOpen.Lock()
	if cp.numOpen < cp.maxOpen {
		cp.numOpen++
		cp.muNumOpen.Unlock()
		db := cp.getNewSqlDB()
		return db, nil
	}
	cp.muNumOpen.Unlock()

	// 等待
	cp.muRequest.Lock()
	// 再次检查空闲连接，防止出现一直等待的情况
	cp.muNumFree.Lock()
	if cp.numFree > 0 {
		db := cp.connList[cp.head]
		cp.head = (cp.head + 1) % cp.maxOpen
		cp.numFree--
		cp.muNumFree.Unlock()
		cp.muRequest.Unlock()
		return db, nil
	}
	cp.muNumFree.Unlock()
	req := make(chan *sql.DB, 1)
	cp.connRequests[cp.rightRequest] = req
	cp.rightRequest++ // uint64类型最大值+1会变成0
	cp.muRequest.Unlock()
	select {
	case ret, ok := <-req:
		if !ok {
			return nil, errors.New("req is not ok")
		}
		if ret == nil {
			return nil, errors.New("ret is null")
		}
		return ret, nil
	}
}

func (cp *ConnPool) ReleaseConn(db *sql.DB) {
	cp.muRequest.Lock()
	if cp.leftRequest != cp.rightRequest {
		req := cp.connRequests[cp.leftRequest]
		delete(cp.connRequests, cp.leftRequest) // Remove from pending requests.
		cp.leftRequest++
		cp.muRequest.Unlock()
		req <- db
		return
	}
	cp.muNumFree.Lock()
	cp.connList[cp.tail] = db
	cp.tail = (cp.tail + 1) % cp.maxOpen
	cp.numFree++
	cp.muNumFree.Unlock()
	cp.muRequest.Unlock()
}

func (cp *ConnPool) Close() error {
	for _, req := range cp.connRequests {
		close(req)
	}
	var err error
	for cp.head != cp.tail {
		err = cp.connList[cp.head].Close()
		cp.head = (cp.head + 1) % cp.maxOpen
	}
	return err
}

func (cp *ConnPool) getNewSqlDB() *sql.DB {
	conn := avatica.NewConnector(cp.info.url).(*avatica.Connector)
	conn.Info = map[string]string{
		"user":     cp.info.user,
		"password": cp.info.password,
		"database": cp.info.database,
	}
	return sql.OpenDB(conn)
}

func (cp *ConnPool) SetProbeInterval(d time.Duration) {
	if d < 0 {
		d = time.Minute
	}
	cp.interval = d
}

func (cp *ConnPool) SetProbeSql(s string) {
	if s == "" {
		s = "show databases"
	}
	cp.sql = s
}

func (cp *ConnPool) OpenProbe(ctx context.Context) error {
	if cp.isOpen {
		return fmt.Errorf("probe is open")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if cp.interval == 0 {
		cp.interval = time.Minute
	}
	if cp.sql == "" {
		cp.sql = "show databases"
	}
	ctx, cancel := context.WithCancel(ctx)
	cp.stop = cancel
	cp.isOpen = true
	go cp.aliveTask(ctx)
	return nil
}

func (cp *ConnPool) aliveTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(cp.interval):
			// 超过一半连接空闲时才去探活
			if cp.numFree > cp.numOpen/2 {
				log.Println("________________________________探活执行 ", cp.sql, cp.interval)
				for i := 0; i < cp.numFree; i++ {
					if cp.numFree > cp.numOpen/2 { // 每次循环再次判断，不影响主线任务，及时结束探活
						db, _ := cp.GetConn() // 注意这里只是给一个示例，生产环境下异常要cover
						stmt, _ := db.Prepare(cp.sql)
						_, _ = stmt.Exec()
						_ = stmt.Close()
						cp.ReleaseConn(db)
					} else { // 跳出当前for循环
						break
					}
				}
			}
		}
	}
}

func (cp *ConnPool) CloseProbe() {
	log.Println("________________________________探活关闭 ")
	cp.stop()
	cp.isOpen = false
}

func OpenConnPool(info *ConnInfo, maxOpen int) *ConnPool {
	if maxOpen < 1 {
		maxOpen = 1
	}
	return &ConnPool{
		connList:     make([]*sql.DB, maxOpen),
		maxOpen:      maxOpen,
		info:         info,
		connRequests: make(map[uint64]chan *sql.DB),
	}
}

func main() {
	databaseUrl := "http://localhost:30060" // 这里的链接地址与lindorm-cli的链接地址比，需要去掉http之前的字符串
	connInfo := &ConnInfo{
		url:      databaseUrl,
		user:     "test",    // 数据库用户名
		password: "test",    // 数据库密码
		database: "default", // 初始化连接指定的默认database
	}
	cp := OpenConnPool(connInfo, 100) // 100个连接的pool
	defer checkPrepareErr("close conn pool", cp.Close())

	//checkPrepareErr("open probe", cp.OpenProbe(nil)) // 开启探活
	//defer cp.CloseProbe()
	var wg sync.WaitGroup
	var totalTime int64
	var sumTime int64
	var mu sync.Mutex
	num := 10
	ti := time.Now().Unix()
	for n := 0; n < num; n++ {
		sumTime = 0
		for i := 0; i < 250; i++ {
			wg.Add(1)
			ii := i
			go func() {
				defer wg.Done()
				t_ns := time.Now().UnixNano()
				db, err := cp.GetConn()
				defer cp.ReleaseConn(db) // 一定要保证释放连接
				checkPrepareErr("get conn", err)
				stmt, err := db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
				checkPrepareErr("prepare upsert", err)
				for k := 0; k < 10; k++ {
					_, err = stmt.Exec(n*2500+ii*10+k, "name_"+strconv.Itoa(n*2500+ii*10+k), 18)
					checkPrepareErr("upsert stmt exec", err)
				}
				checkPrepareErr("upsert close statement", stmt.Close())
				mu.Lock()
				sumTime += (time.Now().UnixNano() - t_ns) / 1000000
				mu.Unlock()
			}()
		}
		fmt.Println("开始阻塞_", n)
		wg.Wait()
		fmt.Println("任务执行结束,解除阻塞_", n, "平均rt:", sumTime/2500, "ms")
		totalTime += sumTime / 2500

		//time.Sleep(2 * time.Second)
	}
	fmt.Println("_________________________________task take", time.Now().Unix()-ti, "s")
	fmt.Println("任务结束", "平均rt:", totalTime/int64(num), "ms")
}

func checkPrepareErr(remark string, err error) {
	if err != nil {
		log.Println(remark + ":" + err.Error())
	}
}
