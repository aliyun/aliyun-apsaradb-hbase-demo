import phoenixdb
import phoenixdb.cursor

# 使用python需在环境中安装phoenix-python驱动，参见https://python-phoenixdb.readthedocs.io/en/latest/
if __name__ == "__main__":
    # url修改为运行环境的QueryServer连接地址
    database_url = 'http://localhost:8765'
    # python客户端仅支持自动提交方式
    conn = phoenixdb.connect(database_url, autocommit=True)

    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS users")
    # 创建测试表
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
    # 创建索引
    cursor.execute("CREATE INDEX users_index ON users (username)")

    # 插入一条数据
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (11, 'admin'))
    # 批量插入数据
    param = ((1, "tom"), (2, "alice"), (3, "bob"))
    cursor.executemany("UPSERT INTO users VALUES (?, ?)", param)

    # 查询数据
    cursor.execute("SELECT * FROM users")
    print cursor.fetchall()
    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    # query data use index
    cursor.execute("SELECT * FROM users WHERE username='admin'")
    print cursor.fetchone()['ID']