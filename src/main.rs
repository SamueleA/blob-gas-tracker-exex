use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
};

use alloy_consensus::Transaction;
use alloy_primitives::{Address, BlockNumber};
use futures_util::{FutureExt, TryStreamExt};
use reth::{
    api::{BlockBody, FullNodeComponents},
    builder::NodeTypes,
    primitives::EthPrimitives,
};
use reth_exex::{ExExContext, ExExEvent};
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::info;

struct MyExEx<Node: FullNodeComponents> {
    ctx: ExExContext<Node>,
    /// Block gas transaction by To address
    gas_consumed_by_address: HashMap<Address, u64>,
}

impl<Node: FullNodeComponents> MyExEx<Node> {
    fn new(ctx: ExExContext<Node>) -> Self {
        Self {
            ctx,
            gas_consumed_by_address: HashMap::new(),
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
