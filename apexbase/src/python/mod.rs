//! Python bindings via PyO3
//!
//! ApexStorage is the storage implementation using on-demand reading.

mod bindings;
#[cfg(feature = "flight")]
mod flight_bridge;
#[cfg(feature = "server")]
mod server_bridge;

pub use bindings::ApexStorageImpl as ApexStorage;
pub use bindings::ArrowBatchStream;
#[cfg(feature = "flight")]
pub use flight_bridge::start_flight_server;
#[cfg(feature = "server")]
pub use server_bridge::start_pg_server;
