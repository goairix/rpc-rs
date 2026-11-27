//! Protocol shared by the Greeter server and client.
pub mod pb {
    tonic::include_proto!("example.v1");
}
