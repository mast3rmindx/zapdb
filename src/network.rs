use ant_core::{P2PNode, NodeConfig, P2PEvent, PeerId};
use crate::encryption::Encryption;
use tokio::sync::broadcast::Receiver;

pub struct NetworkManager {
    node: P2PNode,
    key: [u8; 32],
    events: Receiver<P2PEvent>,
}

impl NetworkManager {
    pub async fn new(key: [u8; 32]) -> Result<Self, Box<dyn std::error::Error>> {
        let node = P2PNode::builder()
            .with_mcp_config(Default::default())
            .build()
            .await?;
        let events = node.subscribe_events();
        Ok(Self { node, key, events })
    }

    pub async fn run(&mut self) {
        loop {
            match self.events.recv().await {
                Ok(P2PEvent::Message { source, data }) => {
                    if let Ok(decrypted_data) = self.receive_and_decrypt(&data) {
                        // TODO: Handle the decrypted message
                    }
                }
                _ => {}
            }
        }
    }

    pub async fn encrypt_and_send(&self, peer_id: &PeerId, data: &[u8]) -> Result<(), &'static str> {
        let encrypted_data = Encryption::encrypt(&self.key, data)?;
        self.node
            .send_message(peer_id, "zapdb", encrypted_data)
            .await
            .map_err(|_| "Failed to send message")
    }

    pub fn receive_and_decrypt(&self, encrypted_data: &[u8]) -> Result<Vec<u8>, &'static str> {
        Encryption::decrypt(&self.key, encrypted_data)
    }
}
