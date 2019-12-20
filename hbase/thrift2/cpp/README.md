1. thrift --gen cpp hbase.thrift
2. g++ -std=c++11 -DHAVE_NETINET_IN_H -o HbaseClient -I./ -I/usr/local/include/thrift -I./gen-cpp -L/usr/local/lib HbaseClient.cpp ./gen-cpp/hbase_types.cpp ./gen-cpp/hbase_constants.cpp ./gen-cpp/THBaseService.cpp AliTHttpClient.cpp -lthrift -g
3. ./HbaseClient host port 
