import os

from client import Client, ClientConfig
from util import make_insert_binding, make_query_binding, print_affected_rows


def main():
    # Enables TLS if a TLS certificate is specified by the `TLS_CERT` environment variable.
    # The `tls_cert` is None if the variable is not set and the TLS is disabled.
    tls_cert = os.getenv("TLS_CERT")
    #! Python grpc requires to set the hostname to a valid domain name For TLS rather than an numeric address.
    config = ClientConfig(
        host="localhost",
        port=8360,
        username="admin",
        password="public",
        tls_cert=tls_cert,
    )
    # Creates a client to connect to the Datalayers server.
    client = Client(config)

    # Creates a database `python`.
    sql = "create database python;"
    result = client.execute(sql)
    # The result should be:
    # Affected rows: 0
    print_affected_rows(result)

    # Sets the database context to `python`.
    client.use_database("python")

    # Creates a table `demo` within the database `python`.
    sql = """
        CREATE TABLE python.demo (
                ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                sid INT32,
                value REAL,
                flag INT8,
                timestamp key(ts)
        )
        PARTITION BY HASH(sid) PARTITIONS 8
        ENGINE=TimeSeries;
        """
    result = client.execute(sql)
    # The result should be:
    # Affected rows: 0
    print_affected_rows(result)

    # Inserts some data into the `demo` table.
    sql = """
        INSERT INTO python.demo (ts, sid, value, flag) VALUES
            ('2024-09-01T10:00:00+08:00', 1, 12.5, 0),
            ('2024-09-01T10:05:00+08:00', 2, 15.3, 1),
            ('2024-09-01T10:10:00+08:00', 3, 9.8, 0),
            ('2024-09-01T10:15:00+08:00', 4, 22.1, 1),
            ('2024-09-01T10:20:00+08:00', 5, 30.0, 0);
        """
    result = client.execute(sql)
    # The result should be:
    # Affected rows: 5
    print_affected_rows(result)

    # Queries the inserted data.
    sql = "SELECT * FROM python.demo"
    result = client.execute(sql)
    # The result should be:
    #                             ts  sid  value  flag
    # 0 2024-09-01 10:00:00+08:00    1   12.5     0
    # 1 2024-09-01 10:05:00+08:00    2   15.3     1
    # 2 2024-09-01 10:15:00+08:00    4   22.1     1
    # 3 2024-09-01 10:20:00+08:00    5   30.0     0
    # 4 2024-09-01 10:10:00+08:00    3    9.8     0
    print(result)

    # Inserts some data into the `demo` table with prepared statement.
    #
    # The with block is used to manage the life cycle of the prepared statement automatically.
    # Otherwise, you have call the `close` method of the prepared statement manually.
    sql = "INSERT INTO python.demo (ts, sid, value, flag) VALUES (?, ?, ?, ?);"
    with client.prepare(sql) as prepared_stmt:
        binding = make_insert_binding()
        result = client.execute_prepared(prepared_stmt, binding)
        # The result should be:
        # Affected rows: 5
        print_affected_rows(result)

    # Queries the inserted data with prepared statement.
    sql = "SELECT * FROM python.demo WHERE sid = ?"
    with client.prepare(sql) as prepared_stmt:
        # Retrieves all rows with `sid` == 1.
        binding = make_query_binding(1)
        result = client.execute_prepared(prepared_stmt, binding)
        # The result should be:
        #                                  ts  sid  value  flag
        # 0 2024-09-01 10:00:00+08:00    1   12.5     0
        # 1 2024-09-02 10:00:00+08:00    1   12.5     0
        print(result)

        # Retrieves all rows with `sid` == 2.
        binding = make_query_binding(2)
        result = client.execute_prepared(prepared_stmt, binding)
        # The result should be:
        #                                  ts  sid  value  flag
        # 0 2024-09-01 10:05:00+08:00    2   15.3     1
        # 1 2024-09-02 10:05:00+08:00    2   15.3     1
        print(result)

    # There provides a dedicated interface `execute_update` for executing DMLs, including Insert, Delete.
    # This interface directly returns the affected rows which might be convenient for some use cases.
    #
    # Note, Datalayers does not support Update and the development for Delete is in progress.
    sql = """
        INSERT INTO python.demo (ts, sid, value, flag) VALUES
            ('2024-09-03T10:00:00+08:00', 1, 4.5, 0),
            ('2024-09-03T10:05:00+08:00', 2, 11.6, 1);
        """
    #! It's expected that the affected rows is 2.
    #! However, the flightsql-dbapi library seems does not implement the `execute_update` correctly
    #! and the returned affected rows is always 0.
    affected_rows = client.execute_update(sql)
    # The output should be:
    # Affected rows: 2
    # print("Affected rows: {}".format(affected_rows))

    # Checks that the data are inserted successfully.
    sql = "SELECT * FROM python.demo where ts >= '2024-09-03T10:00:00+08:00'"
    result = client.execute(sql)
    # The result should be:
    #                          ts  sid  value  flag
    # 0 2024-09-03 10:00:00+08:00    1    4.5     0
    # 1 2024-09-03 10:05:00+08:00    2   11.6     1
    print(result)


if __name__ == "__main__":
    main()
