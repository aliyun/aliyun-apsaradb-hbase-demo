#include "THBaseService.h"
#include <config.h>
#include <vector>
#include "transport/TSocket.h"
#include <transport/TBufferTransports.h>
#include <THttpClient.h>
#include <protocol/TBinaryProtocol.h>
#include <iostream>
#include <sstream>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace com::alibaba::hbase::thrift2;

int main(int argc, char **argv) {
	if(argc != 3) {
		fprintf(stderr, "param  is :XX ip port\n");
		return -1;
	}

	int port = atoi(argv[2]);
	stdcxx::shared_ptr<THttpClient> http(new THttpClient(argv[1], port, "/"));
	http->setCustomHeader("ACCESSKEYID", "root");
	http->setCustomHeader("ACCESSSIGNATURE", "root");
	stdcxx::shared_ptr<TProtocol> protocol(new TBinaryProtocol(http));
	try {
		http->open();
		THBaseServiceClient client(protocol);
		printf("transport open\n");

		const std::string nnamespace("ns");
		TNamespaceDescriptor tNamespaceDescriptor;
		tNamespaceDescriptor.__set_name(nnamespace);
		client.createNamespace(tNamespaceDescriptor);
		printf("namespace created\n");

		const std::string table("ns:table1");
		TTableName tTableName;
		tTableName.__set_ns(nnamespace);
		tTableName.__set_qualifier("table1");

		TColumnFamilyDescriptor tColumnFamilyDescriptor;
		tColumnFamilyDescriptor.__set_name("f");

		std::vector<TColumnFamilyDescriptor> columns;
		columns.push_back(tColumnFamilyDescriptor);

		TTableDescriptor tTableDescriptor;
		tTableDescriptor.__set_tableName(tTableName);
		tTableDescriptor.__set_columns(columns);
		std::vector<std::string> splitKeys;

		client.createTable(tTableDescriptor, splitKeys);
		printf("table created\n");

		TColumnValue tColumnValue;
		tColumnValue.__set_family("f");
		tColumnValue.__set_qualifier("q1");
		tColumnValue.__set_value("value");

		std::vector<TColumnValue> columnValues;
		columnValues.push_back(tColumnValue);

		TPut tput;
		tput.__set_row("row1");
		tput.__set_columnValues(columnValues);

		client.put(table, tput);
		printf("put success\n");

		TColumnValue tColumnValue1(tColumnValue);
		tColumnValue1.__set_value("value2");

		TColumnValue tColumnValue2(tColumnValue);
		tColumnValue2.__set_value("value3");

		std::vector<TColumnValue> columnValues1;
		columnValues1.push_back(tColumnValue1);

		std::vector<TColumnValue> columnValues2;
		columnValues2.push_back(tColumnValue2);

		TPut tput1;
		tput1.__set_row("row2");
		tput1.__set_columnValues(columnValues1);

		TPut tput2;
		tput2.__set_row("row3");
		tput2.__set_columnValues(columnValues2);

		std::vector<TPut> tputs;
		tputs.push_back(tput1);
		tputs.push_back(tput2);

		client.putMultiple(table, tputs);
		printf("putMultiple success\n");

		TGet tget;
		TResult tresult;
		tget.__set_row("row1");
		client.get(tresult, table, tget);
		std::ostringstream h;
		tresult.printTo(h);
		printf("get result: %s\n", h.str().c_str());

		std::vector<TResult> getMultipleResult;
		std::vector<TGet> gets;

		TGet tget1;
		tget1.__set_row("row2");

		TGet tget2;
		tget2.__set_row("row3");

		gets.push_back(tget1);
		gets.push_back(tget2);

		client.getMultiple(getMultipleResult, table, gets);
		printf("get multiple result start: \n");
		for(std::vector<TResult>::const_iterator iter=getMultipleResult.begin();iter!=getMultipleResult.end();iter++) {
			std::ostringstream h1;
			(*iter).printTo(h1);
			printf("\t%s\n", h1.str().c_str());
		}
		printf("get multiple result end: \n");


		TScan tscan;
		tscan.__set_startRow("row0");
		tscan.__set_stopRow("row9");

		std::vector<TResult> allResult;
		while (true) {
			TResult* lastResult = NULL;
			std::vector<TResult> scannerResult;
			client.getScannerResults(scannerResult, table, tscan, 2);
			for(std::vector<TResult>::const_iterator iter=scannerResult.begin();iter!=scannerResult.end();iter++) {
				allResult.push_back(*iter);
				lastResult = (TResult*) &(*iter);
			}

			if (lastResult == NULL) {
				break;
			} else {
				tscan.__set_startRow((*lastResult).row + (char)0);
			}
		}

		printf("scan result start: \n");
		for(std::vector<TResult>::const_iterator iter=allResult.begin();iter!=allResult.end();iter++) {
			std::ostringstream h1;
			(*iter).printTo(h1);
			printf("\t%s\n", h1.str().c_str());
		}
		printf("scan result end: \n");

		client.disableTable(tTableName);
		printf("table disabled\n");
		client.deleteTable(tTableName);
		printf("table deleted\n");
		client.deleteNamespace(nnamespace);
		printf("namespace deleted\n");
		http->close();
		printf("transport close\n");
	} catch (const TException &tx) {
		std::cerr << "ERROR(exception): " << tx.what() << std::endl;
	}
	return 0;
}
