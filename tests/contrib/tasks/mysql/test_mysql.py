import pytest

from prefect.contrib.tasks.mysql.mysql import MySQLExecute, MySQLFetch

class Dummy():
    def __init__(self):
        self.calls = []

    def called(self, name):
        self.calls.append(name)


class CursorDummy(Dummy):

    def execute(self, x):
        self.called("execute")
        return "OK"

    def fetchall(self):
        self.called("fetchall")
        return "OK"

    def fetchmany(self, fetch_count):
        self.called("fetchmany[%s]" % fetch_count)
        return "OK"

    def fetchone(self):
        self.called("fetchone")
        return "OK"

    def __enter__(self, *args, **kwargs):
        self.called("__enter__")
        return self

    def __exit__(self, *args, **kwargs):
        self.called("__exit__")
        return self


class ConnDummy(Dummy):

    def __init__(self, cursor_dummy):
        super().__init__()
        self.cursor_dummy = cursor_dummy

    def cursor(self):
        self.called("cursor")
        return self.cursor_dummy

    def commit(self):
        self.called("commit")
        return "OK"

    def close(self):
        self.called("close")
        return "OK"


class PymysqlDummy(Dummy):

    MySQLError = Exception

    def __init__(self, conn_dummy):
        super().__init__()
        self.conn_dummy = conn_dummy

    def connect(self, **kwargs):
        self.called("connect")
        return self.conn_dummy



def get_dummies():
    cursor_dummy = CursorDummy()
    conn_dummy = ConnDummy(cursor_dummy)
    pymysql_dummy = PymysqlDummy(conn_dummy)
    return cursor_dummy, conn_dummy, pymysql_dummy


class TestMySQLExecute:
    def test_construction(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLExecute(db_name="test", user="test", password="test",
                            host="test", pymysql=pymysql_dummy)
        assert (task.commit is False) and (task.charset == "utf8mb4")

    def test_query_string_must_be_provided(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLExecute(db_name="test", user="test", password="test",
                            host="test", pymysql=pymysql_dummy)
        with pytest.raises(ValueError, match="A query string must be provided"):
            task.run()

    def test_query_string(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLExecute(db_name="test", user="test", password="test",
                            host="test", pymysql=pymysql_dummy)
        result = task.run(query="Any Query")
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', '__exit__']

    def test_query_string_commit(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLExecute(db_name="test", user="test", password="test",
                            host="test", commit=True, pymysql=pymysql_dummy)
        result = task.run(query="Any Query")
        assert task.commit is True
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'commit', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', '__exit__']

class TestMySQLFetch:
    def test_construction(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        assert task.fetch == "one"

    def test_query_string_must_be_provided(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        with pytest.raises(ValueError, match="A query string must be provided"):
            task.run()

    def test_bad_fetch_param_raises(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        with pytest.raises(
            ValueError,
            match="The 'fetch' parameter must be one of the following - \('one', 'many', 'all'\)",
        ):
            task.run(query="SELECT * FROM some_table", fetch="not a valid parameter")

    def test_fetch_run(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        with pytest.raises(
            ValueError,
            match="The 'fetch' parameter must be one of the following - \('one', 'many', 'all'\)",
        ):
            task.run(query="SELECT * FROM some_table", fetch="not a valid parameter")

    def test_fetchone_commit(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", commit=True, pymysql=pymysql_dummy)
        result = task.run(query="SELECT * FROM some_table", fetch="one")
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'commit', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', 'fetchone', '__exit__']

    def test_fetchone(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        result = task.run(query="SELECT * FROM some_table", fetch="one")
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', 'fetchone', '__exit__']

    def test_fetchmany(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        result = task.run(query="SELECT * FROM some_table", fetch="many", fetch_count=99)
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', 'fetchmany[99]', '__exit__']

    def test_fetchall(self):
        cursor_dummy, conn_dummy, pymysql_dummy = get_dummies()
        task = MySQLFetch(db_name="test", user="test", password="test",
                          host="test", pymysql=pymysql_dummy)
        result = task.run(query="SELECT * FROM some_table", fetch="all")
        assert result == "OK"
        assert pymysql_dummy.calls == ['connect']
        assert conn_dummy.calls == ['cursor', 'close']
        assert cursor_dummy.calls == ['__enter__', 'execute', 'fetchall', '__exit__']
