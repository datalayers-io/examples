package main

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"time"
)

type ClientConfig struct {
	Host     string
	Port     uint32
	Username string
	Password string
	TlsCert  *string
}

type Client struct {
	inner flightsql.Client
	md    metadata.MD
}

type DbClient struct {
	Client
	md metadata.MD
}

// Creates a client for executing SQLs on the Datalayers server.
func MakeClient(config *ClientConfig) (*Client, error) {
	// Creates a FlightSQL client to connect to Datalayers.
	// The TLS is enabled if tls_cert is provided, otherwise insecure.
	addr := fmt.Sprintf("%s:%v", config.Host, config.Port)
	var dialOpts []grpc.DialOption

	if config.TlsCert != nil {
		// Load the certificate from the provided path.
		creds, err := loadTLSCredentials(*config.TlsCert)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %v", err)
		}

		// Use secure credentials with the loaded certificate.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		// Use insecure credentials if no TLS certificate is provided.
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Creates a FlightSQL client to connect to Datalayers.
	flightSqlClient, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create a Arrow Flight SQL client: %v", err)
	}
	// Authenticates with the server.
	ctx, err := flightSqlClient.Client.AuthenticateBasicToken(context.Background(), config.Username, config.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate with the server: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("failed to authenticate with the server")
	}

	client := &Client{
		inner: *flightSqlClient,
		md:    md,
	}
	return client, nil
}

// Helper function to load TLS credentials from a certificate file.
func loadTLSCredentials(tlsCert string) (credentials.TransportCredentials, error) {
	// Reads the certificate file.
	cert, err := os.ReadFile(tlsCert)
	if err != nil {
		return nil, fmt.Errorf("could not read TLS certificate: %v", err)
	}

	// Creates certificate pool and append cert.
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(cert) {
		return nil, fmt.Errorf("failed to append cert to pool")
	}

	// Creates TLS credentials based on the certificate pool.
	creds := credentials.NewClientTLSFromCert(certPool, "")
	return creds, nil
}

func (client *Client) timeoutContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	ctx = metadata.NewOutgoingContext(ctx, client.md)
	return ctx, cancel
}

// UseDatabase Sets the database context for each outgoing request.
func (client *Client) UseDatabase(database string) DbClient {
	md := client.md.Copy()
	md.Set("database", database)
	return DbClient{
		Client: *client,
		md:     md,
	}
}

// Execute the sql on Datalayers and returns the result as a slice of arrow records.
func (client *Client) Execute(sql string) ([]arrow.Record, error) {
	ctx, cancel := client.timeoutContext()
	flightInfo, err := client.inner.Execute(ctx, sql)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to execute a sql: %v", err)
	}
	return client.doGet(flightInfo.GetEndpoint()[0].GetTicket())
}

// ExecuteUpdate the sql on Datalayers and returns the affected rows.
// The supported sqls are Insert and Delete. Note, the development for supporting Delete is in progress.
func (client *Client) ExecuteUpdate(sql string) (int64, error) {
	ctx, cancel := client.timeoutContext()
	affectedRows, err := client.inner.ExecuteUpdate(ctx, sql)
	cancel()
	if err != nil {
		return 0, fmt.Errorf("failed to execute a sql: %v", err)
	}
	return affectedRows, nil
}

// Prepare Creates a prepared statement.
func (client *Client) Prepare(sql string) (*flightsql.PreparedStatement, error) {
	ctx, cancel := client.timeoutContext()
	stmt, err := client.inner.Prepare(ctx, sql)
	cancel()
	return stmt, err
}

// ExecutePrepared binds the record to the prepared statement and executes it on the server.
func (client *Client) ExecutePrepared(preparedStmt *flightsql.PreparedStatement, binding arrow.Record) ([]arrow.Record, error) {
	ctx, cancel := client.timeoutContext()
	preparedStmt.SetParameters(binding)
	flightInfo, err := preparedStmt.Execute(ctx)
	cancel()
	binding.Release()
	if err != nil {
		return nil, fmt.Errorf("failed to execute a prepared statement: %v", err)
	}
	return client.doGet(flightInfo.GetEndpoint()[0].GetTicket())
}

// ClosePrepared closes the prepared statement.
func (client *Client) ClosePrepared(preparedStmt *flightsql.PreparedStatement) error {
	ctx, cancel := client.timeoutContext()
	err := preparedStmt.Close(ctx)
	cancel()
	return err
}

// Calls the `DoGet` method of the FlightSQL client.
func (client *Client) doGet(ticket *flight.Ticket) ([]arrow.Record, error) {
	ctx, cancel := client.timeoutContext()
	reader, err := client.inner.DoGet(ctx, ticket)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to perform DoGet: %v", err)
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		record := reader.Record()
		// Increments ref count for each record to not let it release immediately when the reader gets released.
		record.Retain()
		records = append(records, record)
	}
	return records, nil
}
