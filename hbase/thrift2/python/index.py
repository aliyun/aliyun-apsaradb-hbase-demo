# -*- coding: utf-8  -*-
# 以下两个模块可以通过 pip install thrift 安装获得
from thrift.protocol import TBinaryProtocol
from thrift.transport import THttpClient
# 下面的模块通过 thrift --gen py hbase.thrift 来生成
from hbase import THBaseService
from hbase.ttypes import TColumnValue, TColumn, TTableName, TTableDescriptor, TColumnFamilyDescriptor, TNamespaceDescriptor, TGet, TPut, TScan
# 连接地址
url = "http://ld-bp1n9k5e2skw959z5-proxy-hbaseue-pub.hbaseue.rds.aliyuncs.com:9190"
transport = THttpClient.THttpClient(url)
headers = {}
# 用户名
headers["ACCESSKEYID"]="root";
# 密码
headers["ACCESSSIGNATURE"]="root"
transport.setCustomHeaders(headers)
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
client = THBaseService.Client(protocol)
transport.open()
# create Namespace
client.createNamespace(TNamespaceDescriptor(name="ns"))
# create table
tableName = TTableName(ns="ns", qualifier="table1")
client.createTable(TTableDescriptor(tableName=tableName, columns=[
    TColumnFamilyDescriptor(name="f")
]), None)
# 做DML操作时，表名参数为bytes，表名的规则是namespace + 冒号 + 表名
tableInbytes = "ns:table1".encode("utf8")
# 插入数据
client.put(table=tableInbytes, tput=TPut(row="row1".encode("utf8"), columnValues=[TColumnValue(family="f".encode("utf8"), qualifier="q1".encode("utf8"), value="value".encode("utf8"))]))
# 批量插入数据
puts = []
put1 = TPut(row="row2".encode("utf8"), columnValues=[TColumnValue(family="f".encode("utf8"), qualifier="q1".encode("utf8"), value="value2".encode("utf8"))])
put2 = TPut(row="row3".encode("utf8"), columnValues=[TColumnValue(family="f".encode("utf8"), qualifier="q1".encode("utf8"), value="value3".encode("utf8"))])
puts.append(put1)
puts.append(put2)
client.putMultiple(table=tableInbytes, tputs=puts)
# 单行查询数据
result = client.get(tableInbytes, TGet(row="row1".encode("utf8")))
print "Get result: ", result
# 批量单行查询
gets = []
get1 = TGet(row="row2".encode("utf8"))
get2 = TGet(row="row3".encode("utf8"))
gets.append(get1)
gets.append(get2)
results = client.getMultiple(tableInbytes, gets)
print "getMultiple results: ", results
# 范围扫描
# 扫描时需要设置startRow和stopRow，否则会变成全表扫描
startRow = "row0".encode("utf8")
stopRow = "row9".encode("utf8")
scan = TScan(startRow=startRow, stopRow=stopRow)
# caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
# 根据每行的大小，caching的值一般设置为10到100之间
caching = 2
# 扫描的结果
results = []
# 此函数可以找到比当前row大的最小row，方法是在当前row后加入一个0x00的byte
# 从比当前row大的最小row开始scan，可以保证中间不会漏扫描数据
def createClosestRowAfter(row):
    array = bytearray(row)
    array.append(0x00)
    return bytes(array)
while True:
    lastResult = None
    # getScannerResults会自动完成open,close 等scanner操作，HBase增强版必须使用此方法进行范围扫描
    currentResults = client.getScannerResults(tableInbytes, scan, caching)
    for result in currentResults:
        results.append(result)
        lastResult = result
    # 如果一行都没有扫描出来，说明扫描已经结束，我们已经获得startRow和stopRow之间所有的result    
    if lastResult is None:
        break
    # 如果此次扫描是有结果的，我们必须构造一个比当前最后一个result的行大的最小row，继续进行扫描，以便返回所有结果
    else:
        nextStartRow = createClosestRowAfter(lastResult.row)
        scan = TScan(startRow=nextStartRow, stopRow=stopRow)
print "Scan results: ", results
# disable 表
client.disableTable(TTableName(ns="ns", qualifier="table1"))
# 删除表
client.deleteTable(TTableName(ns="ns", qualifier="table1"))
# close 连接
transport.close()
