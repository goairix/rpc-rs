use crate::{
    DiscoveredService, DiscoveryError, DiscoveryOptions, DiscoverySubscription, LbPolicy, Resolver,
    Snapshot,
    balancer::{Picker, effective_weight},
    transport::{self, Routes, SharedRoutes},
};
use arc_swap::ArcSwap;
use rpc_naming::ServiceStatus;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// Owns a service subscription, connection workers and client routing state.
pub struct Discovery {
    subscription: Box<dyn DiscoverySubscription>,
    service: DiscoveredService,
    routes: SharedRoutes,
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl Discovery {
    /// Read the service snapshot and start dialing up instances. Initial dialing is asynchronous.
    pub async fn connect(
        resolver: &dyn Resolver,
        service: &str,
        options: DiscoveryOptions,
    ) -> Result<Self, DiscoveryError> {
        if options.connect_timeout.is_zero()
            || options
                .keepalive
                .is_some_and(|interval| interval.is_zero() || options.keepalive_timeout.is_zero())
        {
            return Err(DiscoveryError(format!(
                "{service}: zero connection or keepalive timeout"
            )));
        }
        let subscription = resolver.subscribe(service, options.resync).await?;
        let routes = Arc::new(ArcSwap::from_pointee(Routes::empty(
            "no available instance",
        )));
        let stop = CancellationToken::new();
        let task = tokio::spawn(run(
            subscription.snapshots(),
            routes.clone(),
            options,
            stop.clone(),
        ));
        Ok(Self {
            subscription,
            service: DiscoveredService::new(routes.clone()),
            routes,
            stop,
            task: Some(task),
        })
    }

    /// Create a transport handle for a generated client, sharing existing connections.
    pub fn service(&self) -> DiscoveredService {
        self.service.clone()
    }
    /// Number of currently eligible, connected instances.
    pub fn ready_count(&self) -> usize {
        self.routes.load().channels.len()
    }
    /// Force a fresh membership snapshot; transport reconciliation proceeds asynchronously.
    pub async fn refresh(&self) -> Result<(), DiscoveryError> {
        self.subscription.refresh().await
    }
    /// Close discovery and join connection workers. Existing client clones see an empty route set.
    pub async fn close(&mut self) {
        self.stop.cancel();
        self.subscription.close().await;
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Discovery {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct Node {
    address: String,
    weight: i64,
    /// Unique across removal and replacement of this instance in this discovery.
    generation: u64,
    channel: Option<Channel>,
    worker: AbortHandle,
}
struct Event {
    id: String,
    generation: u64,
    outcome: Result<Channel, String>,
}

async fn monitor(
    id: String,
    generation: u64,
    address: String,
    options: DiscoveryOptions,
    events: mpsc::Sender<Event>,
) {
    let mut backoff = Duration::from_millis(100);
    loop {
        let result = transport::connect(&address, &options).await;
        let reason = match result {
            Ok((channel, ended)) => {
                if events
                    .send(Event {
                        id: id.clone(),
                        generation,
                        outcome: Ok(channel),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                backoff = Duration::from_millis(100);
                ended
                    .await
                    .unwrap_or_else(|_| "connection driver stopped".into())
            }
            Err(error) => transport::describe(error.as_ref()),
        };
        let reason = format!("{address}: {reason}");
        if events
            .send(Event {
                id: id.clone(),
                generation,
                outcome: Err(reason),
            })
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

async fn run(
    mut updates: watch::Receiver<Snapshot>,
    routes: SharedRoutes,
    options: DiscoveryOptions,
    stop: CancellationToken,
) {
    let (tx, mut events) = mpsc::channel::<Event>(64);
    let mut workers = JoinSet::new();
    let mut nodes = BTreeMap::<String, Node>::new();
    let mut generation = 0;
    let mut incumbent = None;
    let mut reason = "no available instance".to_owned();
    updates.mark_changed();
    loop {
        tokio::select! {
            _ = stop.cancelled() => {
                reason = "discovery closed".into();
                break;
            }
            changed = updates.changed() => {
                if changed.is_err() { break; }
                let snapshot = updates.borrow_and_update();
                let members = match snapshot.as_ref() {
                    Ok(members) => members,
                    Err(error) => { reason = error.to_string(); break; }
                };
                // Providers may publish all instances, including those being drained.
                let eligible = |m: &rpc_naming::ServiceMetadata| m.status == ServiceStatus::Up && !m.address.is_empty();
                let mut routing_changed = false;
                nodes.retain(|id, node| {
                    let retain = members.get(id).is_some_and(|m| eligible(m) && m.address == node.address);
                    if !retain {
                        routing_changed |= node.channel.is_some();
                        node.worker.abort();
                    }
                    retain
                });
                for (id, member) in members.iter().filter(|(_, m)| eligible(m)) {
                    let weight = effective_weight(member.weight);
                    if let Some(node) = nodes.get_mut(id) {
                        routing_changed |= node.channel.is_some()
                            && options.policy == LbPolicy::WeightedRoundRobin
                            && node.weight != weight;
                        node.weight = weight;
                        continue;
                    }
                    generation += 1;
                    let worker = workers.spawn(monitor(
                        id.clone(), generation, member.address.clone(), options.clone(), tx.clone(),
                    ));
                    nodes.insert(id.clone(), Node {
                        address: member.address.clone(), weight,
                        generation, channel: None, worker,
                    });
                }
                // New workers publish their channel once connected. Until then, an
                // unchanged ready set must keep its picker and current round-robin ticket.
                if !routing_changed { continue; }
            }
            event = events.recv() => {
                let Some(event) = event else { break };
                let Some(node) = nodes.get_mut(&event.id) else { continue };
                // An aborted worker may already have enqueued a socket notification.
                // Ignore it if this ID now refers to another connection generation.
                if node.generation != event.generation { continue; }
                match event.outcome {
                    Ok(channel) => node.channel = Some(channel),
                    Err(error) => {
                        reason = error;
                        // Repeated failures of an already excluded endpoint must not
                        // restart healthy nodes' round-robin sequences.
                        if node.channel.take().is_none() && !routes.load().channels.is_empty() {
                            continue;
                        }
                    }
                }
            }
            _ = workers.join_next(), if !workers.is_empty() => continue,
        }
        let mut eligible: Vec<_> = nodes
            .iter()
            .filter_map(|(id, n)| Some((id, n.weight, n.channel.as_ref()?.clone())))
            .collect();
        if options.policy == LbPolicy::PickFirst {
            if let Some(i) = eligible
                .iter()
                .position(|(id, _, _)| Some(*id) == incumbent.as_ref())
            {
                eligible.swap(0, i);
            }
            incumbent = eligible.first().map(|(id, _, _)| (*id).clone());
        }
        let weights = eligible.iter().map(|(_, w, _)| *w).collect::<Vec<_>>();
        routes.store(Arc::new(Routes {
            picker: Picker::new(options.policy, &weights),
            channels: eligible.into_iter().map(|(_, _, ch)| ch).collect(),
            reason: reason.clone(),
        }));
    }
    routes.store(Arc::new(Routes::empty(&reason)));
    workers.shutdown().await;
}
