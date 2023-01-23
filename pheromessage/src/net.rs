//! Delivery mechanisms for messages over a network.

use std::net::{SocketAddr, UdpSocket};

use crate::Delivery;

/// A converter for messages (of type `M`) to raw bytes that can be sent over a network.
pub trait ToBytes<M> {
    /// The concrete type of the raw bytes (e.g. `Vec<u8>`)
    type Bytes: AsRef<[u8]>;
    /// The type of error that can happen while converting.
    type Error;

    /// Convert the message to raw bytes.
    fn to_bytes(&self, message: &M) -> Result<Self::Bytes, Self::Error>;
}

/// A delivery mechanism for messages using UDP.
pub struct UdpDelivery<S> {
    /// The local UDP socket for delivery.
    pub socket: UdpSocket,
    /// The serializer to convert messages to raw bytes.
    pub serializer: S,
}

impl<S> UdpDelivery<S> {
    /// Create a new `UdpDelivery` over the given local socket and using the given serializer for messages.
    pub fn new(socket: UdpSocket, serializer: S) -> UdpDelivery<S> {
        UdpDelivery { socket, serializer }
    }
}

/// Error while delivering a message over the network.
#[derive(Debug)]
pub enum Error<E> {
    /// A serialization error while converting the message to raw bytes.
    Serialization(E),
    /// A send error while sending the message over the network.
    Send(std::io::Error),
}

impl<S, M> Delivery<M, SocketAddr> for UdpDelivery<S>
where
    S: ToBytes<M>,
{
    type Error = Error<S::Error>;

    fn deliver<'a, I>(&self, message: &M, endpoints: I) -> Result<(), Self::Error>
    where
        I: ExactSizeIterator<Item = &'a SocketAddr>,
        SocketAddr: 'a,
    {
        let bytes = self
            .serializer
            .to_bytes(message)
            .map_err(Error::Serialization)?;
        for endpoint in endpoints {
            self.socket
                .send_to(bytes.as_ref(), endpoint)
                .map_err(Error::Send)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread::spawn;

    use super::*;

    struct ByteSer();

    impl ToBytes<u8> for ByteSer {
        type Bytes = [u8; 1];

        type Error = ();

        fn to_bytes(&self, message: &u8) -> Result<Self::Bytes, Self::Error> {
            Ok([*message])
        }
    }

    #[test]
    fn deliver() {
        let sender = UdpSocket::bind("127.0.0.1:44455").unwrap();
        let target_endpoint: SocketAddr = "127.0.0.1:44456".parse().unwrap();
        let receiver = UdpSocket::bind(target_endpoint).unwrap();
        let sender = UdpDelivery::new(sender, ByteSer());
        let rec_thread = spawn(move || {
            let mut buf = [0; 1];
            let (amt, src) = receiver.recv_from(&mut buf).unwrap();
            assert_eq!(44455, src.port());
            assert_eq!(1, amt);
            assert_eq!(10, buf[0]);
        });
        sender.deliver(&10, [target_endpoint].iter()).unwrap();
        rec_thread.join().unwrap();
    }
}
