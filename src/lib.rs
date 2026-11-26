//! Rust RPC framework for services sharing protobuf definitions with grpc-go.
pub use rpc_discovery as discovery;
pub use rpc_governance as governance;
pub use rpc_naming as naming;
pub use rpc_naming::static_registry;
pub use rpc_registry as registry;
pub use tonic;

mod health;
pub mod server;
