var HBaseService = require("./gen-nodejs/THBaseService.js");
var ttypes = require("./gen-nodejs/hbase_types.js");
var thrift = require("thrift");
var utf8 = require("utf8");

var options = {
  transport: thrift.TBufferedTransport,
  protocol: thrift.TBinaryProtocol,
  path: "/",
  headers: { ACCESSKEYID: "root", ACCESSSIGNATURE: "root" }
};

var connection = thrift.createHttpConnection(
  "ld-bp1k1a3tm048db5o6-proxy-hbaseue-pub.hbaseue.rds.aliyuncs.com",
  9190,
  options
);
var client = thrift.createHttpClient(HBaseService, connection);

// create Namespace
client.createNamespace(
  new ttypes.TNamespaceDescriptor({ name: "ns" }),
  function(err) {
    if (err) {
      console.log("create namespace error:", err);
    } else {
      console.log("create namespace success");
    }

    // create table
    var tableName = new ttypes.TTableName({ ns: "ns", qualifier: "table1" });
    client.createTable(
      new ttypes.TTableDescriptor({
        tableName: tableName,
        columns: [new ttypes.TColumnFamilyDescriptor({ name: "f" })]
      }),
      null,
      function(err) {
        if (err) {
          console.log("create table error:", err);
        } else {
          console.log("create table success");
        }
        // 做DML操作时，表名参数为bytes，表名的规则是namespace + 冒号 + 表名
        var tableInbytes = utf8.encode("ns:table1");

        // 插入数据
        put = new ttypes.TPut({
          row: utf8.encode("row1"),
          columnValues: [
            new ttypes.TColumnValue({
              family: utf8.encode("f"),
              qualifier: utf8.encode("q1"),
              value: utf8.encode("value")
            })
          ]
        });

        client.put(tableInbytes, put, function(err) {
          if (err) {
            console.log("Put error:", err);
          } else {
            console.log("Put success");
          }

          // 单行查询数据
          client.get(
            tableInbytes,
            new ttypes.TGet({ row: utf8.encode("row1") }),
            function(err, data) {
              if (err) {
                console.log("Get error:", err);
              } else {
                console.log("Get result: " + JSON.stringify(data));
              }
            }
          );
        });

        // 批量插入数据
        puts = [];
        put1 = new ttypes.TPut({
          row: utf8.encode("row2"),
          columnValues: [
            new ttypes.TColumnValue({
              family: utf8.encode("f"),
              qualifier: utf8.encode("q1"),
              value: utf8.encode("value2")
            })
          ]
        });
        put2 = new ttypes.TPut({
          row: utf8.encode("row3"),
          columnValues: [
            new ttypes.TColumnValue({
              family: utf8.encode("f"),
              qualifier: utf8.encode("q1"),
              value: utf8.encode("value3")
            })
          ]
        });
        puts.push(put1);
        puts.push(put2);

        client.putMultiple(tableInbytes, puts, function(err) {
          if (err) {
            console.log("PutMultiple error:", err);
          } else {
            console.log("PutMultiple success");
          }

          // 批量单行查询
          gets = [];
          get1 = new ttypes.TGet({ row: utf8.encode("row2") });
          get2 = new ttypes.TGet({ row: utf8.encode("row3") });
          gets.push(get1);
          gets.push(get2);
          client.getMultiple(tableInbytes, gets, function(err, data) {
            if (err) {
              console.log("GetMultiple error:", err);
            } else {
              console.log("GetMultiple result: " + JSON.stringify(data));
            }
          });
        });

        startRow = utf8.encode("row0");
        stopRow = utf8.encode("row9");
        scan = new ttypes.TScan({ startRow: startRow, stopRow: stopRow});
        // caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
        // 根据每行的大小，caching的值一般设置为10到100之间
        caching = 2;
        results = [];

        function tmp() {
          lastResult = null;
          client.getScannerResults(tableInbytes, scan, caching, function(err, data) {
            if (err) {
              console.log("scan error:", err);
            } else {
              for (var i = 0; i < data.length; i++) {
                results.push(data[i]);
                lastResult = data[i];
              }

              if (lastResult === null) {
                console.log("scan result: " + JSON.stringify(results));
              } else {
                nextStartRow = lastResult.row.toString("utf8");
                nextStartRow = nextStartRow + String.fromCharCode(0);
                nextStartRow = utf8.encode(nextStartRow);
                scan = new ttypes.TScan({startRow: nextStartRow, stopRow: stopRow});
                tmp();
              }
            }
          });
        }

        setTimeout(tmp, 1000);

        // disable 表 after 5 seconds
        setTimeout(function() {
          client.disableTable(
            new ttypes.TTableName({ ns: "ns", qualifier: "table1" }),
            function(err) {
              if (err) {
                console.log("DisableTable error:", err);
              } else {
                console.log("DisableTable success");
              }

              // 删除表
              client.deleteTable(
                new ttypes.TTableName({ ns: "ns", qualifier: "table1" }),
                function(err) {
                  if (err) {
                    console.log("DeleteTable error:", err);
                  } else {
                    console.log("DeleteTable success");
                  }

                  // 删除命名空间
                  client.deleteNamespace(
		    "ns",
                    function(err) {
                      if (err) {
                        console.log("DeleteNamespace error:", err);
                      } else {
                        console.log("DeleteNamespace success");
                      }
                    }
                  );
                }
              );
            }
          );
        }, 5000);
      }
    );
  }
);

