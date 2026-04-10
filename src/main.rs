use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
};

use alloy_consensus::Transaction;
use alloy_primitives::Address;
use futures_util::{FutureExt, TryStreamExt};
use reth::{api::FullNodeComponents, builder::NodeTypes, primitives::EthPrimitives};
use reth_exex::{ExExContext, ExExEvent};
use reth_node_ethereum::EthereumNode;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};

const GAS_DB_PATH: &str = "./gas_db";

#[derive(Serialize, Deserialize)]
struct GasData {
    gas_consumed: u64,
}
struct MyExEx<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    /// Block gas transaction by To address (TODO: remove this)
    gas_consumed_by_address: HashMap<Address, u64>,
    /// DB for gas consumption by address
    gas_db: DB,
}

impl<Node: FullNodeComponents> MyExEx<Node> {
    fn new(ctx: ExExContext<Node>) -> Self {
        let gas_db = db_init(GAS_DB_PATH);

        Self {
            ctx,
            gas_consumed_by_address: HashMap::new(),
            gas_db,
        }
    }
}

impl<Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>> Future
    for MyExEx<Node>
{
    type Output = eyre::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Some(notification) = ready!(this.ctx.notifications.try_next().poll_unpin(cx))? {
            if let Some(reverted_chain) = notification.reverted_chain() {
                reverted_chain.blocks_iter().for_each(|b| {
                    b.body().transactions().for_each(|t| {
                        let tx = t.as_eip4844();

                        let Some(eip4844) = tx else {
                            return;
                        };

                        let blob_gas_consumed = eip4844.blob_gas_used().unwrap_or_default();
                        let destination = eip4844.to().unwrap_or_default();

                        let gas_db_entry = this.gas_db.get(destination).unwrap();

                        let mut current_gas: GasData = match gas_db_entry {
                            Some(value_bytes) => serde_json::from_slice(&value_bytes).unwrap(),
                            None => GasData { gas_consumed: 0 },
                        };

                        current_gas.gas_consumed =
                            current_gas.gas_consumed.wrapping_sub(blob_gas_consumed);

                        let updated_gas_db_entry = serde_json::to_vec(&current_gas).unwrap();

                        this.gas_db.put(destination, updated_gas_db_entry).unwrap();

                        *this
                            .gas_consumed_by_address
                            .entry(destination)
                            .or_insert(blob_gas_consumed) -= blob_gas_consumed;
                    })
                });

                this.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(reverted_chain.tip().num_hash()))?;
            }

            if let Some(committed_chain) = notification.committed_chain() {
                committed_chain.blocks_iter().for_each(|b| {
                    b.body().transactions().for_each(|t| {
                        let tx = t.as_eip4844();

                        let Some(eip4844) = tx else {
                            return;
                        };

                        let blob_gas_consumed = eip4844.blob_gas_used().unwrap_or_default();
                        let destination = eip4844.to().unwrap_or_default();

                        let gas_db_entry = this.gas_db.get(destination).unwrap();

                        let mut current_gas: GasData = match gas_db_entry {
                            Some(value_bytes) => serde_json::from_slice(&value_bytes).unwrap(),
                            None => GasData { gas_consumed: 0 },
                        };

                        current_gas.gas_consumed =
                            current_gas.gas_consumed.wrapping_add(blob_gas_consumed);

                        let updated_gas_db_entry = serde_json::to_vec(&current_gas).unwrap();

                        this.gas_db.put(destination, updated_gas_db_entry).unwrap();

                        *this
                            .gas_consumed_by_address
                            .entry(destination)
                            .or_insert(blob_gas_consumed) += blob_gas_consumed;
                    })
                });

                this.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }

        Poll::Ready(Ok(()))
    }
}

fn db_init(path: &str) -> DB {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = match DB::open(&opts, path) {
        Ok(db) => db,
        Err(e) => panic!("failed to open database: {}", e),
    };
    db
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(async move |builder, _| {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("my-exex", async move |ctx| Ok(MyExEx::new(ctx)))
            .launch_with_debug_capabilities()
            .await?;

        handle.wait_for_node_exit().await
    })
}
