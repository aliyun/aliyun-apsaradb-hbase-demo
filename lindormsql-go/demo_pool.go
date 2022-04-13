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
	"database/sql"
	"errors"
	"fmt"
	avatica "github.com/apache/calcite-avatica-go/v5"
	"log"
	"strconv"
	"sync"
	"time"
)

type connPool struct {
	connList     []*sql.DB // 存放连接
	maxOpen      int       // 允许的最大连接数
	muNumOpen    sync.Mutex
	numOpen      int // 已有的连接数量，包括分配出去的
	muNumFree    sync.Mutex
	numFree      int                     // 空闲连接数量
	head         int                     // 头部拿连接
	tail         int                     // 尾部插入连接
	connRequests map[uint64]chan *sql.DB // 阻塞
	muRequest    sync.Mutex
	nextRequest  uint64
	info         *ConnInfo
}
type ConnInfo struct {
	url      string
	user     string
	password string
	database string
}

func (cp *connPool) GetConn() (*sql.DB, error) {
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
	req := make(chan *sql.DB, 1)
	cp.muRequest.Lock()
	cp.connRequests[cp.nextRequest] = req
	cp.nextRequest++
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

func (cp *connPool) ReleaseConn(db *sql.DB) {
	cp.muRequest.Lock()
	if c := len(cp.connRequests); c > 0 {
		var req chan *sql.DB
		var reqKey uint64
		for reqKey, req = range cp.connRequests {
			break
		}
		delete(cp.connRequests, reqKey) // Remove from pending requests.
		cp.muRequest.Unlock()
		req <- db
		return

	}
	cp.muRequest.Unlock()
	cp.muNumFree.Lock()
	cp.connList[cp.tail] = db
	cp.tail = (cp.tail + 1) % cp.maxOpen
	cp.numFree++
	cp.muNumFree.Unlock()
}

func (cp *connPool) Close() error {
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

func (cp *connPool) getNewSqlDB() *sql.DB {
	conn := avatica.NewConnector(cp.info.url).(*avatica.Connector)
	conn.Info = map[string]string{
		"user":     cp.info.user,
		"password": cp.info.password,
		"database": cp.info.database,
	}
	return sql.OpenDB(conn)
}

func OpenConnPool(info *ConnInfo, maxOpen int) *connPool {
	if maxOpen < 1 {
		maxOpen = 1
	}
	return &connPool{
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
	defer func() {
		checkPrepareErr("close conn pool", cp.Close())
	}()
	var wg sync.WaitGroup
	var totalTime int64
	var sumTime int64
	var mu sync.Mutex
	num := 100
	ti := time.Now().Unix()
	for n := 0; n < num; n++ {
		sumTime = 0
		for i := 0; i < 250; i++ {
			wg.Add(1)
			ii := i
			go func() {
				t_ns := time.Now().UnixNano()
				db, err := cp.GetConn()
				checkPrepareErr("get conn", err)
				stmt, err := db.Prepare("upsert into user_test(id,name,age) values(?,?,?)")
				checkPrepareErr("prepare upsert", err)
				for k := 0; k < 10; k++ {
					_, err = stmt.Exec(n*2500+ii*10+k, "name_"+strconv.Itoa(n*2500+ii*10+k), 18)
					checkPrepareErr("upsert stmt exec", err)
				}
				checkPrepareErr("upsert close statement", stmt.Close())
				cp.ReleaseConn(db)
				mu.Lock()
				sumTime += (time.Now().UnixNano() - t_ns) / 1000000
				mu.Unlock()
				wg.Done()
			}()
		}
		fmt.Println("开始阻塞_", n)
		wg.Wait()
		fmt.Println("任务执行结束,解除阻塞_", n, "平均rt:", sumTime/2500, "ms")
		totalTime += sumTime / 2500

		//time.Sleep(2 * time.Second)
	}
	fmt.Println("_________________________________task take", (time.Now().Unix()-ti)/60, "min")
	fmt.Println("任务结束", "平均rt:", totalTime/int64(num), "ms")
	//checkPrepareErr("close conn pool", cp.Close())
}

func checkPrepareErr(remark string, err error) {
	if err != nil {
		log.Println(remark + ":" + err.Error())
	}
}
