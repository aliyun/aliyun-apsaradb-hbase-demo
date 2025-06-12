Quick start 
1. Install thrift python package (required).
```
pip install thrift==0.13.0
```
2. Generate thrift api codes (optional).
```
thrift --gen py hbase.thrift
```
3. Move the generated codes to current work directory (optional).
```
mv gen-py/hbase .
```
4. Change url, username, password in index.py from line 9 to line 15, and then run the example. 
```
python index.py
```
