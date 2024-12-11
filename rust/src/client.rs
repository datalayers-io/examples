use std::{process::exit, str::FromStr, time::Duration};

use crate::util::filter_message;

use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_flight::{
    sql::client::{FlightSqlServiceClient, PreparedStatement},
    Ticket,
};
use futures::TryStreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// The configuration for the client connecting to the Datalayers server via Arrow Flight SQL protocol.
pub struct ClientConfig {
    /// The hostname of the Datalayers database server.
    pub host: String,
    /// The port number on which the Datalayers database server is listening.
    pub port: u32,
    /// The username for authentication when connecting to the database.
    pub username: String,
    /// The password for authentication when connecting to the database.
    pub password: String,
    /// The optional TLS certificate for secure connections.
    /// The certificate is self-signed by Datalayers and is used as the pem file by the client to certify itself.
    pub tls_cert: Option<String>,
}

pub struct Client {
    /// The Arrow Flight SQL client.
    inner: FlightSqlServiceClient<Channel>,
}

impl Client {
    pub async fn try_new(config: &ClientConfig) -> Result<Self> {
        let protocol = config.tls_cert.as_ref().map(|_| "https").unwrap_or("http");
        let uri = format!("{}://{}:{}", protocol, config.host, config.port);
        let mut endpoint = Endpoint::from_str(&uri)
            .context(format!("Failed to create an endpoint with uri {}", uri))?
            .connect_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true);

        // Configures TLS if a certificate is provided.
        if let Some(tls_cert) = &config.tls_cert {
            let cert = std::fs::read_to_string(tls_cert)
                .context(format!("Failed to read the TLS cert file {}", tls_cert))?;
            let cert = Certificate::from_pem(cert);
            let tls_config = ClientTlsConfig::new()
                .domain_name(&config.host)
                .ca_certificate(cert);
            endpoint = endpoint
                .tls_config(tls_config)
                .context("failed to configure TLS")?;
        }

        let channel = endpoint
            .connect()
            .await
            .context(format!("Failed to connect to server with uri {}", uri))?;
        let mut flight_sql_client = FlightSqlServiceClient::new(channel);

        // Performs authorization with the Datalayers server.
        let _ = flight_sql_client
            .handshake(&config.username, &config.password)
            .await
            .inspect_err(|e| {
                println!("{}", filter_message(&e.to_string()));
                exit(1)
            });

        Ok(Self {
            inner: flight_sql_client,
        })
    }

    pub fn use_database(&mut self, database: &str) {
        self.inner.set_header("database", database);
    }

    pub async fn execute(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let flight_info = self
            .inner
            .execute(sql.to_string(), None)
            .await
            .inspect_err(|e| {
                println!("{}", filter_message(&e.to_string()));
                exit(1)
            })?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = self.do_get(ticket).await?;
        Ok(batches)
    }

    pub async fn execute_update(&mut self, sql: &str) -> Result<i64> {
        let affected_rows = self
            .inner
            .execute_update(sql.to_string(), None)
            .await
            .inspect_err(|e| {
                println!("{}", filter_message(&e.to_string()));
                exit(1)
            })?;
        Ok(affected_rows)
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement<Channel>> {
        let prepared_stmt = self
            .inner
            .prepare(sql.to_string(), None)
            .await
            .inspect_err(|e| {
                println!("{}", filter_message(&e.to_string()));
                exit(1)
            })?;
        Ok(prepared_stmt)
    }

    pub async fn execute_prepared(
        &mut self,
        prepared_stmt: &mut PreparedStatement<Channel>,
        binding: RecordBatch,
    ) -> Result<Vec<RecordBatch>> {
        prepared_stmt
            .set_parameters(binding)
            .context("Failed to bind a record batch to the prepared statement")?;
        let flight_info = prepared_stmt.execute().await.inspect_err(|e| {
            println!("{}", filter_message(&e.to_string()));
            exit(1)
        })?;
        let ticket = flight_info
            .endpoint
            .first()
            .context("No endpoint in flight info")?
            .ticket
            .clone()
            .context("No ticket in endpoint")?;
        let batches = self.do_get(ticket).await?;
        Ok(batches)
    }

    pub async fn close_prepared(&self, prepared_stmt: PreparedStatement<Channel>) -> Result<()> {
        prepared_stmt
            .close()
            .await
            .context("Failed to close a prepared statement")
    }

    async fn do_get(&mut self, ticket: Ticket) -> Result<Vec<RecordBatch>> {
        let stream = self.inner.do_get(ticket).await.inspect_err(|e| {
            println!("{}", filter_message(&e.to_string()));
            exit(1)
        })?;
        let batches = stream.try_collect::<Vec<_>>().await.inspect_err(|e| {
            println!("{}", filter_message(&e.to_string()));
            exit(1)
        })?;
        if batches.is_empty() {
            bail!("Unexpected empty batches");
        }
        Ok(batches)
    }
}
