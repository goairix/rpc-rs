use tonic::{Request, Response, Status};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse, health_check_response::ServingStatus,
    health_server::Health,
};

/// Matches the Go health service for both known and unknown names.
pub(crate) struct Serving;

#[tonic::async_trait]
impl Health for Serving {
    type WatchStream = tokio_stream::Empty<Result<HealthCheckResponse, Status>>;

    async fn check(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }

    async fn watch(
        &self,
        _: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("health watch is not implemented"))
    }
}
