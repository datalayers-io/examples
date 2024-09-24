package main

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type ClientConfig struct {
	Host     string
	Port     uint32
	Username string
	Password string
	TlsCert  *string
}

type Client struct {
	inner *flightsql.Client
	// Golang uses context to pass Grpc context back and forth.
	ctx context.Context
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

	client := &Client{
		inner: flightSqlClient,
		ctx:   ctx,
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

// Sets the database context for each outgoing request.
func (client *Client) UseDatabase(database string) {
	client.ctx = metadata.AppendToOutgoingContext(client.ctx, "database", database)
}

// Executes the sql on Datalayers and returns the result as a slice of arrow records.
func (client *Client) Execute(sql string) ([]arrow.Record, error) {
	flightInfo, err := client.inner.Execute(client.ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute a sql: %v", err)
	}
	return client.doGet(flightInfo.GetEndpoint()[0].GetTicket())
}

// Creates a prepared statement.
func (client *Client) Prepare(sql string) (*flightsql.PreparedStatement, error) {
	return client.inner.Prepare(client.ctx, sql)
}

// Binds the record to the prepared statement and executes it on the server.
func (client *Client) ExecutePrepared(preparedStmt *flightsql.PreparedStatement, binding arrow.Record) ([]arrow.Record, error) {
	defer binding.Release()

	preparedStmt.SetParameters(binding)
	flightInfo, err := preparedStmt.Execute(client.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute a prepared statement: %v", err)
	}
	return client.doGet(flightInfo.GetEndpoint()[0].GetTicket())
}

// Calls the `DoGet` method of the FlightSQL client.
func (client *Client) doGet(ticket *flight.Ticket) ([]arrow.Record, error) {
	reader, err := client.inner.DoGet(client.ctx, ticket)
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
