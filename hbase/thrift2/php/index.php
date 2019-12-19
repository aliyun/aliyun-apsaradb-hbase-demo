<?php 

require_once("./thrift-0.12.0/lib/php/lib/StringFunc/TStringFunc.php");
require_once("./thrift-0.12.0/lib/php/lib/StringFunc/Core.php");
require_once("./thrift-0.12.0/lib/php/lib/Factory/TStringFuncFactory.php");
require_once("./thrift-0.12.0/lib/php/lib/Protocol/TProtocol.php");
require_once("./thrift-0.12.0/lib/php/lib/Protocol/TBinaryProtocol.php");
require_once("./thrift-0.12.0/lib/php/lib/Transport/TTransport.php");
require_once("./thrift-0.12.0/lib/php/lib/Transport/TBufferedTransport.php");
require_once("./thrift-0.12.0/lib/php/lib/Transport/THttpClient.php");
require_once("./thrift-0.12.0/lib/php/lib/Type/TType.php");
require_once("./thrift-0.12.0/lib/php/lib/Type/TMessageType.php");
require_once("./thrift-0.12.0/lib/php/lib/Exception/TException.php");
require_once("./thrift-0.12.0/lib/php/lib/Exception/TTransportException.php");
require_once("./thrift-0.12.0/lib/php/lib/Exception/TApplicationException.php");
require_once("./thrift-0.12.0/lib/php/lib/Base/TBase.php");

require_once("./gen-php/THBaseServiceIf.php");
require_once("./gen-php/THBaseService_createNamespace_args.php");
require_once("./gen-php/TIOError.php");
require_once("./gen-php/THBaseService_createNamespace_result.php");
require_once("./gen-php/THBaseService_deleteNamespace_args.php");
require_once("./gen-php/THBaseService_deleteNamespace_result.php");
require_once("./gen-php/THBaseServiceClient.php");
require_once("./gen-php/TNamespaceDescriptor.php");
require_once("./gen-php/TTableName.php");
require_once("./gen-php/TTableDescriptor.php");
require_once("./gen-php/TColumnFamilyDescriptor.php");
require_once("./gen-php/THBaseService_createTable_result.php");
require_once("./gen-php/THBaseService_deleteTable_result.php");
require_once("./gen-php/THBaseService_createTable_args.php");
require_once("./gen-php/THBaseService_deleteTable_args.php");
require_once("./gen-php/THBaseService_disableTable_args.php");
require_once("./gen-php/THBaseService_disableTable_result.php");
require_once("./gen-php/THBaseService_get_args.php");
require_once("./gen-php/THBaseService_get_result.php");
require_once("./gen-php/THBaseService_put_args.php");
require_once("./gen-php/THBaseService_put_result.php");
require_once("./gen-php/THBaseService_putMultiple_args.php");
require_once("./gen-php/THBaseService_putMultiple_result.php");
require_once("./gen-php/THBaseService_getMultiple_args.php");
require_once("./gen-php/THBaseService_getMultiple_result.php");
require_once("./gen-php/THBaseService_getScannerResults_args.php");
require_once("./gen-php/THBaseService_getScannerResults_result.php");
require_once("./gen-php/TResult.php");
require_once("./gen-php/TPut.php");
require_once("./gen-php/TGet.php");
require_once("./gen-php/TColumnValue.php");
require_once("./gen-php/TScan.php");

use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\THttpClient;
use Thrift\Protocol\TBinaryProtocol;

$headers = array('ACCESSKEYID' => 'root','ACCESSSIGNATURE' => 'root');

// 新建HTTP协议的客户端
$socket = new THttpClient('ld-bp1k1a3tm048db5o6-proxy-hbaseue-pub.hbaseue.rds.aliyuncs.com', 9190);

//将header设置到http client中，访问时携带用户名密码
$socket->addHeaders($headers);

$transport = new TBufferedTransport($socket);
$protocol = new TBinaryProtocol($transport);
$client = new THBaseServiceClient($protocol);
$transport->open();

// create Namespace
$client->createNamespace(new TNamespaceDescriptor(array('name' => "ns")));
echo("create namespace success\n");

// create table
$tableName = new TTableName(array('ns' => "ns", 'qualifier' => "table1"));
$columns = [new TColumnFamilyDescriptor(array('name' => 'f'))];
$client->createTable(new TTableDescriptor(array('tableName' => $tableName, 'columns' => $columns)), array());
echo("create table success\n");

// 插入数据
$client->put("ns:table1", new TPut(array('row' => 'row1', 'columnValues' => [new TColumnValue(array('family' => 'f', 'qualifier' => 'q1', 'value' => 'value'))])));
echo("Put success\n");

// 单行查询数据
$result = $client->get("ns:table1", new TGet(array('row' => 'row1')));
echo("Get success:" . json_encode($result) . "\n");

// 批量插入数据
$client->putMultiple("ns:table1", array(
	new TPut(array('row' => 'row2', 'columnValues' => [new TColumnValue(array('family' => 'f', 'qualifier' => 'q1', 'value' => 'value2'))])),
	new TPut(array('row' => 'row3', 'columnValues' => [new TColumnValue(array('family' => 'f', 'qualifier' => 'q1', 'value' => 'value3'))]))
	)
);
echo("PutMultiple success\n");

// 批量单行查询
$result = $client->getMultiple("ns:table1", array(
	new TGet(array('row' => 'row2')),
	new TGet(array('row' => 'row3'))
	)
);
echo("GetMultiple success:" . json_encode($result) . "\n");

// 范围扫描
$startRow = "row0";
$stopRow = "row9";
$scan = new TScan(array('startRow' => $startRow, 'stopRow' => $stopRow));

// caching的大小为每次从服务器返回的行数，设置太大会导致服务器处理过久，太小会导致范围扫描与服务器做过多交互
// 根据每行的大小，caching的值一般设置为10到100之间
$caching = 2;

// 此函数可以找到比当前row大的最小row，方法是在当前row后加入一个0x00的byte
// 从比当前row大的最小row开始scan，可以保证中间不会漏扫描数据
function createClosestRowAfter($row) {
	return $row . chr(0);
};

$result = [];
while (true):
	$lastResult = null;
	$currentResult = $client->getScannerResults("ns:table1", $scan, $caching);
	foreach($currentResult as $item) {
		array_push($result, $item);
		$lastResult = $item;
	}

	if ($lastResult === null) {
		break;
	} else {
		$nextStartRow = createClosestRowAfter($lastResult->row);
		$scan = new TScan(array('startRow' => $nextStartRow, 'stopRow' => $stopRow));
	}
endwhile;

echo("Scan success:" . json_encode($result) . "\n");


// disable table
$client->disableTable($tableName);
echo("disable table success\n");

// delete table
$client->deleteTable($tableName);
echo("delete table success\n");

// delete Namespace
$client->deleteNamespace("ns");
echo("delete namespace success\n");
?>
