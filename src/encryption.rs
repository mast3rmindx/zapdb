use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, NewAead};
use rand::rngs::OsRng;
use rand::RngCore;

pub struct Encryption;

impl Encryption {
    pub fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }

    pub fn encrypt(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>, &'static str> {
        let key = Key::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        cipher.encrypt(nonce, data)
            .map(|mut ciphertext| {
                ciphertext.extend_from_slice(&nonce_bytes);
                ciphertext
            })
            .map_err(|_| "Encryption failed")
    }

    pub fn decrypt(key: &[u8; 32], encrypted_data: &[u8]) -> Result<Vec<u8>, &'static str> {
        if encrypted_data.len() < 12 {
            return Err("Invalid encrypted data");
        }
        let (ciphertext, nonce_bytes) = encrypted_data.split_at(encrypted_data.len() - 12);
        let key = Key::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);
        cipher.decrypt(nonce, ciphertext)
            .map_err(|_| "Decryption failed")
    }
}
