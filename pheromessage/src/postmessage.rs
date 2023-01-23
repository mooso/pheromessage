//! Implementation of message serialization using the postcard crate.
//! Raw bytes produced by this can be converted back to messages using
//! `postcard::from_bytes()`.

use crate::net::ToBytes;
use serde::ser::Serialize;

/// Empty implementer of `ToBytes` using postcard.
pub struct PostMessage();

/// A singleton `PostMessage` value.
pub const POST_MESSAGE: PostMessage = PostMessage();

impl<T> ToBytes<T> for PostMessage
where
    T: Serialize + Sized,
{
    type Bytes = Vec<u8>;

    type Error = postcard::Error;

    fn to_bytes(&self, message: &T) -> Result<Self::Bytes, Self::Error> {
        postcard::to_allocvec(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let message = "Hello".to_owned();
        let bytes = POST_MESSAGE.to_bytes(&message).unwrap();
        let back: String = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(&message, &back);
    }
}
