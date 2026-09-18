//! Arrow Flight gRPC server for ApexBase
//!
//! High-performance columnar data transfer via Arrow IPC over HTTP/2 (gRPC).
//! Avoids the row-by-row text serialization overhead of PG wire protocol.
//!
//! Usage:
//!   apexbase-flight --dir /path/to/data --port 50051
//!
//! Python client example:
//!   import pyarrow.flight as flight
//!   client = flight.connect("grpc://127.0.0.1:50051")
//!   # SELECT
//!   reader = client.do_get(flight.Ticket(b"SELECT * FROM t LIMIT 1000"))
//!   df = reader.read_pandas()
//!   # DML/DDL
//!   result = list(client.do_action(flight.Action("sql", b"INSERT INTO t VALUES ...")))

mod service;
pub use service::ApexFlightService;

use std::net::SocketAddr;
use std::path::PathBuf;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

pub struct FlightConfig {
    pub data_dir: PathBuf,
    pub host: String,
    pub port: u16,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("."),
            host: "127.0.0.1".to_string(),
            port: 50051,
        }
    }
}

pub async fn start_flight_server(config: FlightConfig) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    let service = ApexFlightService::new(config.data_dir.clone());

    println!("ApexBase Arrow Flight Server");
    println!("  Listening on: grpc://{}", addr);
    println!("  Data dir:     {}", config.data_dir.display());
    println!(
        "  Python:       pyarrow.flight.connect(\"grpc://{}\")",
        addr
    );

    log::info!("ApexBase Flight server listening on {}", addr);

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests;
