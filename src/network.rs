use ant_core::{P2PNode, NodeConfig};

pub struct NetworkManager {
    node: P2PNode,
}

impl NetworkManager {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let node = P2PNode::builder()
            .with_mcp_config(Default::default())
            .build()
            .await?;
        Ok(Self { node })
    }

    pub async fn run(&mut self) {
        self.node.run().await.unwrap();
    }
}
