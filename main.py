import uvicorn


def start_uvicorn():
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)


def init_oracle_client():
    import os
    import platform
    import cx_Oracle

    if platform.system() == "Darwin":
        cx_Oracle.init_oracle_client(lib_dir=os.environ.get("HOME")+"/python/oracle_instantclient_19_16")
        # cx_Oracle.init_oracle_client(lib_dir="/Users/kuznetsov/python/oracle_instantclient_19_16")


def test_oracle():
    import oracledb
    import cx_Oracle
    import getpass

    oracledb.init_oracle_client(lib_dir="/Users/kuznetsov/python/oracle_instantclient_19_16")

    # Test to see if the cx_Oracle is recognized
    print(cx_Oracle.version)   # this returns 8.0.1 for me
    # test_oracle()

    # cx_Oracle.init_oracle_client(lib_dir="/Users/kuznetsov/python/oracle_instantclient_19_16")
    # This fails for me at this point but will succeed after the solution described below
    print(cx_Oracle.clientversion())

    print(oracledb.clientversion())
    un = "scott"
    cs = "localhost/orclpdb"
    # cs = "localhost/freepdb1"   # for Oracle Database Free users
    # cs = "localhost/orclpdb1"   # some databases may have this service
    pw = getpass.getpass(f"Enter password for {un}@{cs}: ")


    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        with connection.cursor() as cursor:
            sql = "select sysdate from dual"
            for r in cursor.execute(sql):
                print(r)


if __name__ == "__main__":
    # init_oracle_client()
    # test_oracle()
    start_uvicorn()
