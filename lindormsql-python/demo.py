import phoenixdb
class PhoenixConnectionTest:

    def __init__(self):
        return

    def _connect(self, connect_kw_args):
        try:
            # url = 'http://ld-bp1955k72j03cxh1m-proxy-phoenix-pub.lindorm.rds.aliyuncs.com:8765'
            url = '链接地址'
            r = phoenixdb.connect(url, autocommit=True, **connect_kw_args)
            return r
        except AttributeError:
            print ("Failed to connect")

# connect_kw_args = {'user': 'root', 'password': 'root','phoenix.connection.schema':'sql'}
# connect_kw_args = {'user': 'root', 'password': 'root'}
connect_kw_args = {'user': '账户', 'password': '密码'}
db = PhoenixConnectionTest()
connection = db._connect(connect_kw_args)

with connection.cursor() as statement:
    # use_namespace = "use sql"
    # print (use_namespace)
    # statement.execute(use_namespace)
    sql_drop_table = "drop table if exists test_python"
    print (sql_drop_table)
    statement.execute(sql_drop_table)

    sql_create_table ="create table if not exists test_python(c1 integer primary key, c2 integer)"
    print (sql_create_table)
    statement.execute(sql_create_table)

    sql_upsert = "upsert into test_python(c1, c2) values(1,1)"
    print (sql_upsert)
    statement.execute(sql_upsert)

    sql_select = "SELECT * FROM test_python"
    print (sql_select)
    statement.execute(sql_select)
    rows = statement.fetchall()
    print (rows)

    # sql_drop_table = "drop table if exists test_python"
    # print (sql_drop_table)
    # statement.execute(sql_drop_table)
    statement.close()
connection.close
