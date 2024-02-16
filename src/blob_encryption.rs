use bytes::Bytes;
use std::path::Path;
use chacha20poly1305::{
    aead::{generic_array::GenericArray, Aead, AeadCore, KeyInit, OsRng}, ChaCha20Poly1305, KeySizeUser, Nonce
};
use chacha20poly1305::aead::generic_array::typenum::Unsigned;
use anyhow::anyhow;

#[derive(Clone)]
pub struct EncryptWithChacha {
    key: chacha20poly1305::Key
}

impl EncryptWithChacha {
    pub fn new_with_key_from_file(path: &Path) -> anyhow::Result<Self> {
        let file_content = std::fs::read(path)?;
        if file_content.len() != ChaCha20Poly1305::key_size() {
            anyhow::bail!("Key file content does not have the right length for a key")
        }
        let key: chacha20poly1305::Key = *GenericArray::from_slice(file_content.as_slice());
        let me = Self {
            key
        };
        Ok(me)
    }

    pub fn encrypt_blob(&self, data: Bytes) -> anyhow::Result<Bytes> {
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let cipher = ChaCha20Poly1305::new(&self.key);
        let cipher_text = cipher.encrypt(&nonce, data.as_ref())
            .map_err(|err| anyhow!("cipher.encrypt error: {}", err))?;
        
        use bytes::BufMut;
        let mut blob_with_nonce: Vec<u8> = Vec::with_capacity(nonce.len() + cipher_text.len());
        blob_with_nonce.put_slice(nonce.as_ref());
        blob_with_nonce.put_slice(cipher_text.as_ref());

        Ok(Bytes::from(blob_with_nonce))
    }

    pub fn decrypt_blob(&self, mut data: Bytes) -> anyhow::Result<Bytes> {

        let nonce_size = <ChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;

        if data.len() < nonce_size {
            anyhow::bail!("decrypt_blob not enough bytes in data to contain a nonce")
        }
        else if data.len() < nonce_size + 1 {
            anyhow::bail!("decrypt_blob data is just the nonce?")
        }

        let nonce = *Nonce::from_slice(&data.slice(0..nonce_size));
        let cipher_text = data.split_off(nonce_size);

        let cipher = ChaCha20Poly1305::new(&self.key);
        let plain_text = cipher.decrypt(&nonce, cipher_text.as_ref())
            .map_err(|err| anyhow!("cipher.decrypt error: {}", err))?;

        Ok(bytes::Bytes::from(plain_text))
    }
}

const CHACHA_KEY_SIZE: usize = <ChaCha20Poly1305 as KeySizeUser>::KeySize::USIZE;
pub fn create_key() -> [u8; CHACHA_KEY_SIZE] {
    let key = ChaCha20Poly1305::generate_key(OsRng);
    key.into()
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use super::EncryptWithChacha;

    #[test]
    fn encrypt_and_decrypt() {
        let stuffing: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let key = [&stuffing[..], &stuffing[..], &stuffing[..], &stuffing[..]].concat();
        let mut key_file = tempfile::NamedTempFile::new().expect("create a tempfile");
        key_file.write_all(key.as_ref()).expect("write key file content");

        let encrypt = EncryptWithChacha::new_with_key_from_file(key_file.path()).expect("create encrypt");

        let plain_text = bytes::Bytes::from("Hello world");

        let blob = encrypt.encrypt_blob(plain_text.clone()).expect("encrypt blob");

        println!("plain_text: {:x?}", plain_text.as_ref());
        println!("encrypt_blob out: {:x?}", blob.as_ref());

        let plain_text_bis = encrypt.decrypt_blob(blob).expect("decrypt blob");

        assert_eq!(plain_text, plain_text_bis);
    }
}