package main
import (
    // hbase模块通过 thrift --gen go hbase.thrift 来生成
    "./gen-go/hbase"
    "context"
    "fmt"
    "github.com/apache/thrift/lib/go/thrift"
    "os"
)
const (
    HOST = "http://ld-bp1n9k5e2skw959z5-proxy-hbaseue-pub.hbaseue.rds.aliyuncs.com:9190"
    // 用户名
    USER = "root"
    // 密码
    PASSWORD = "root"
)
func main() {
    defaultCtx := context.Background()
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    trans, err := thrift.NewTHttpClient(HOST)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error resolving address:", err)
        os.Exit(1)
    }
    // 设置用户名密码
    httClient := trans.(*thrift.THttpClient)
    httClient.SetHeader("ACCESSKEYID", USER)
    httClient.SetHeader("ACCESSSIGNATURE", PASSWORD)
    client := hbase.NewTHBaseServiceClientFactory(trans, protocolFactory)
    if err := trans.Open(); err != nil {
        fmt.Fprintln(os.Stderr, "Error opening "+HOST, err)
        os.Exit(1)
    }
    // create Namespace
    err = client.CreateNamespace(defaultCtx, &hbase.TNamespaceDescriptor{Name: "ns"})
    if err != nil {
        fmt.Fprintln(os.Stderr, "error CreateNamespace:", err)
        os.Exit(1)
    }
    // create table
    tableName := hbase.TTableName{Ns: []byte("ns"), Qualifier: []byte("table1")}
    err = client.CreateTable(defaultCtx, &hbase.TTableDescriptor{TableName: &tableName, Columns: []*hbase.TColumnFamilyDescriptor{&hbase.TColumnFamilyDescriptor{Name: []byte("f")}}}, nil)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error CreateTable:", err)
        os.Exit(1)
    }
    //做DML操作时，表名参数为bytes，表名的规则是namespace + 冒号 + 表名
    tableInbytes := []byte("ns:table1")
    // 插入数据
    err = client.Put(defaultCtx, tableInbytes, &hbase.TPut{Row: []byte("row1"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
        Family:    []byte("f"),
        Qualifier: []byte("q1"),
        Value:     []byte("value1")}}})
    if err != nil {
        fmt.Fprintln(os.Stderr, "error Put:", err)
        os.Exit(1)
    }
    //批量插入数据
    puts := []*hbase.TPut{&hbase.TPut{Row: []byte("row2"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
        Family:    []byte("f"),
        Qualifier: []byte("q1"),
        Value:     []byte("value2")}}}, &hbase.TPut{Row: []byte("row3"), ColumnValues: []*hbase.TColumnValue{&hbase.TColumnValue{
        Family:    []byte("f"),
        Qualifier: []byte("q1"),
        Value:     []byte("value3")}}}}
    err = client.PutMultiple(defaultCtx, tableInbytes, puts)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error PutMultiple:", err)
        os.Exit(1)
    }
    // 单行查询数据
    result, err := client.Get(defaultCtx, tableInbytes, &hbase.TGet{Row: []byte("row1")})
    fmt.Println("Get result:")
    fmt.Println(result)
    // 批量单行查询
    gets := []*hbase.TGet{&hbase.TGet{Row: []byte("row2")}, &hbase.TGet{Row: []byte("row3")}}
    results, err := client.GetMultiple(defaultCtx, tableInbytes, gets)
    fmt.Println("GetMultiple result:")
    fmt.Println(results)
    //范围扫描
    //扫描时需要设置startRow和stopRow，否则会变成全表扫描
    startRow := []byte("row0")
    stopRow := []byte("row9")
    scan := &hbase.TScan{StartRow: startRow, StopRow: stopRow}
    //caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
    //根据每行的大小，caching的值一般设置为10到100之间
    caching := 2
    // 扫描的结果
    var scanResults []*hbase.TResult_
    for true {
        var lastResult *hbase.TResult_ = nil
        // getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
        currentResults, _ := client.GetScannerResults(defaultCtx, tableInbytes, scan, int32(caching))
        for _, tResult := range currentResults {
            lastResult = tResult
            scanResults = append(scanResults, tResult)
        }
        // 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result
        if lastResult == nil {
            break
        } else {
            // 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
            nextStartRow := createClosestRowAfter(lastResult.Row)
            scan = &hbase.TScan{StartRow: nextStartRow, StopRow: stopRow}
        }
    }
    fmt.Println("Scan result:")
    fmt.Println(scanResults)
    //disable table
    err = client.DisableTable(defaultCtx, &tableName)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error DisableTable:", err)
        os.Exit(1)
    }
    // delete table
    err = client.DeleteTable(defaultCtx, &tableName)
    if err != nil {
        fmt.Fprintln(os.Stderr, "error DisableTable:", err)
        os.Exit(1)
    }
    // delete namespace
    err = client.DeleteNamespace(defaultCtx, "ns")
    if err != nil {
        fmt.Fprintln(os.Stderr, "error DisableTable:", err)
        os.Exit(1)
    }
    //tableName, err := client.GetTableNamesByNamespace(defaultCtx, "default")
    if err != nil {
        fmt.Fprintln(os.Stderr, "error getting table:", err)
        os.Exit(1)
    }
    //s := string(tableName[0].Qualifier)
    //fmt.Println(s)
}
// 此函数可以找到比当前row大的最小row，方法是在当前row后加入一个0x00的byte
// 从比当前row大的最小row开始scan，可以保证中间不会漏扫描数据
func createClosestRowAfter(row []byte) []byte {
    var nextRow []byte
    var i int
    for i = 0; i < len(row); i++ {
        nextRow = append(nextRow, row[i])
    }
    nextRow = append(nextRow, 0x00)
    return nextRow
}
