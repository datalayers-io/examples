from collections import OrderedDict
from typing import Optional

import flightsql.flightsql_pb2 as flightsql_pb
import pandas
import pyarrow as pa
import pyarrow.flight as flight
from flightsql import FlightSQLClient
from flightsql.client import PreparedStatement
from google.protobuf import any_pb2


class ClientConfig:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        tls_cert: Optional[str] = None,
    ):
        """
        Instantiates a FlightSQLClient configured for Datalayers.
        """

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.tls_cert = tls_cert


class Client:
    def __init__(self, config: ClientConfig):
        """
        Instantiates a FlightSQLClient configured for Datalayers.
        """

        #! Since the flightsql-dbapi library does not provide interfaces for passing in the TLS certificate,
        #! we have to first create a Arrow Flight client and then set the fields of the FlightSQLClient instance manually.
        #!
        #! On the other hand, gRPC performs hostname verification on the server side after the TLS handshake.
        #! This verification ensures that the hostname provided by the client matches one of the names presented in the server's certificate.
        #! Otherwise, the verification fails. To generalize the demo for various test environments, we have chosen to disable this verification.

        kwargs = {}
        kwargs["disable_server_verification"] = True

        # Enabls TLS if a TLS certificate is provided.
        if config.tls_cert is not None:
            # Read the certificate file.
            protocol = "tls"
            with open(config.tls_cert, "rb") as cert_file:
                kwargs["tls_root_certs"] = cert_file.read()
        else:
            protocol = "tcp"

        # Creates a Arrow Flight client.
        location = "grpc+{}://{}:{}".format(protocol, config.host, config.port)
        flight_client = flight.FlightClient(
            location,
            **kwargs,
        )

        # The authorization returns a tuple where the key is `Bearer` and the value is the associated token.
        headers = []
        headers.append(
            flight_client.authenticate_basic_token(config.username, config.password)
        )

        # Creates a Arrow FlightSQL client based on the Arrow Flight client.
        flight_sql_client = FlightSQLClient.__new__(FlightSQLClient)
        flight_sql_client.client = flight_client
        flight_sql_client.headers = headers
        flight_sql_client.features = {}
        flight_sql_client.closed = False

        self.inner = flight_sql_client

    def use_database(self, database: str):
        """
        Sets the database context to the given database.
        """

        # Appends a database context to the existent headers.
        headers = self.inner.headers + [(b"database", database.encode("utf-8"))]
        # Removes the old database context header by deduplicating.
        headers = list(OrderedDict(headers).items())
        self.inner.headers = headers

    def execute(self, sql: str) -> pandas.DataFrame:
        """
        Executes the sql on Datalayers and returns the result as a pandas Dataframe.
        """

        # Requests the server to execute the given sql.
        # The server replies with a flight into containing tickets for retrieving the response.
        flight_info = self.inner.execute(sql)
        # By Datalayers' design, there's always a single returned no matter of the Datalayers is in standalone mode or cluster mode.
        ticket = flight_info.endpoints[0].ticket
        # Retrieves the response from the server.
        reader = self.inner.do_get(ticket)
        # Reads the response as a pandas Dataframe.
        df = reader.read_pandas()
        return df

    def prepare(self, sql: str) -> PreparedStatement:
        """
        Creates a prepared statement.
        """

        return self.inner.prepare(sql)

    def execute_prepared(
        self, prepared_stmt: PreparedStatement, binding: pa.RecordBatch
    ) -> pandas.DataFrame:
        """
        Binds the `binding` record batch with the prepared statement and requests the server to execute the statement.
        """

        #! Since the flightsql-dbapi library misses setting options for do_put, we have to manually pass in the options.
        #! We have filed a pull request to fix this issue. When the PR is merged and a new release of the flightsql-dbapi library is published,
        #! the codes could be simplified to:
        #!
        #! ``` Python
        #! flight_info = prepared_stmt.execute(binding)
        #! ticket = flight_info.endpoints[0].ticket
        #! reader = self.inner.do_get(ticket)
        #! df = reader.read_pandas()
        #! return df
        #! ```

        # Creates a flight descriptor from the command.
        cmd = flightsql_pb.CommandPreparedStatementQuery(
            prepared_statement_handle=prepared_stmt.handle
        )
        any = any_pb2.Any()
        any.Pack(cmd)
        desc = flight.FlightDescriptor.for_command(any.SerializeToString())

        # Writes the binding to the Datalayers server through the do_put interface.
        if binding is not None and binding.num_rows > 0:
            writer, reader = self.inner.client.do_put(
                desc, binding.schema, prepared_stmt.options
            )
            writer.write(binding)
            writer.done_writing()
            reader.read()

        # Executes the prepared statement and retrieves the execution result.
        flight_info = self.inner.client.get_flight_info(desc, prepared_stmt.options)
        ticket = flight_info.endpoints[0].ticket
        reader = self.inner.do_get(ticket)
        df = reader.read_pandas()
        return df

    def close(self):
        """
        Closes the inner Arrow FlightSQL client.
        """

        self.inner.close()
