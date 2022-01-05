import phoenixdb

# 这里的链接地址与lindorm-cli的链接地址比，需要去掉http之前的字符串)
database_url = "http://localhost:30060"


def connect(kw_args):
    try:
        return phoenixdb.connect(database_url, autocommit=True, **kw_args)
    except AttributeError:
        print("Failed to connect")


# 用户名通过lindorm_user字段传递，密码使用lindorm_password字段设置，database字段设置连接初始化默认数据库。
connect_kw_args = {'lindorm_user': 'test', 'lindorm_password': 'test', 'database': 'default'}
connection = connect(connect_kw_args)

with connection.cursor() as statement:
    # 创建表
    sql_create_table = "create table if not exists test_python(c1 integer, c2 integer, primary key(c1))"
    print(sql_create_table)
    statement.execute(sql_create_table)

    # 插入一行数据
    sql_upsert = "upsert into test_python(c1, c2) values(1,1)"
    print(sql_upsert)
    statement.execute(sql_upsert)

    # 插入多行数据
    with connection.cursor() as stat:
        sql_upsert = "upsert into test_python(c1, c2) values(?,?)"
        print(sql_upsert)
        stat.executemany(sql_upsert, [(2, 2), (3, 3)])

    # 删除数据
    sql_delete = "delete from test_python where c1=2"
    print(sql_delete)
    statement.execute(sql_delete)

    # 修改数据
    sql_update = "upsert into test_python(c1, c2) values(1,10)"
    print(sql_update)
    statement.execute(sql_update)

    # 查询
    sql_select = "select * from test_python"
    print(sql_select)
    statement.execute(sql_select)
    rows = statement.fetchall()
    print(rows)

    # 禁用表,删除表之前需要先禁用表
    sql_offline_table = "offline table test_python"
    print(sql_offline_table)
    statement.execute(sql_offline_table)

    # 删除表
    sql_drop_table = "drop table if exists test_python"
    print(sql_drop_table)
    statement.execute(sql_drop_table)

connection.close()
