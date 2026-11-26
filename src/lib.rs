//! Rust RPC framework for services sharing protobuf definitions with grpc-go.
pub use rpc_discovery as discovery;
pub use rpc_governance as governance;
pub use rpc_registry as registry;
pub use tonic;

mod health;
pub mod server;
pub mod static_registry;
