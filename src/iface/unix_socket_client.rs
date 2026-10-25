use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;

use crate::buffer::{InputBuffer, OutputBuffer};
use crate::error::RnsError;
use crate::iface::RxMessage;
use crate::packet::Packet;
use crate::serde::Serialize;

use tokio::io::AsyncReadExt;

use alloc::string::String;

use super::hdlc::Hdlc;
use super::{Interface, InterfaceContext};

// TODO: Configure via features
const PACKET_TRACE: bool = false;

pub struct UnixSocketClient {
    addr: String,
    stream: Option<UnixStream>,
}

impl UnixSocketClient {
    pub fn new<T: Into<String>>(addr: T) -> Self {
        Self {
            addr: addr.into(),
            stream: None,
        }
    }

    pub fn new_from_stream<T: Into<String>>(addr: T, stream: UnixStream) -> Self {
        Self {
            addr: addr.into(),
            stream: Some(stream),
        }
    }

    pub async fn spawn(context: InterfaceContext<UnixSocketClient>) {
        let iface_stop = context.channel.stop.clone();
        let addr = { context.inner.lock().unwrap().addr.clone() };
        let iface_address = context.channel.address;
        let mut stream = { context.inner.lock().unwrap().stream.take() };

        let (rx_channel, tx_channel) = context.channel.split();
        let tx_channel = Arc::new(tokio::sync::Mutex::new(tx_channel));

        let mut running = true;
        loop {
            if !running || context.cancel.is_cancelled() {
                break;
            }

            let stream = {
                match stream.take() {
                    Some(stream) => {
                        running = false;
                        Ok(stream)
                    }
                    None => UnixStream::connect(addr.clone())
                        .await
                        .map_err(|_| RnsError::ConnectionError),
                }
            };

            if let Err(_) = stream {
                log::info!("unix_socker_client: couldn't connect to <{}>", addr);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }

            let cancel = context.cancel.clone();
            let stop = CancellationToken::new();

            let stream = stream.unwrap();
            let (read_stream, write_stream) = stream.into_split();

            log::info!("unix_client connected to <{}>", addr);

            const BUFFER_SIZE: usize = core::mem::size_of::<Packet>() * 8; // setting this to 2 fails (which is what it was set to orginally) for some reason I don't understand. Seemed to work fine at * 4 but I set it at * 8 just to be sure, someone smarter can fix this later

            // Start receive task
            let rx_task = {
                let cancel = cancel.clone();
                let stop = stop.clone();
                let mut stream = read_stream;
                let rx_channel = rx_channel.clone();

                tokio::spawn(async move {
                    let mut hdlc_rx_buffer = [0u8; BUFFER_SIZE];
                    let mut rx_buffer = [0u8; BUFFER_SIZE + (BUFFER_SIZE / 2)];
                    let mut unix_socker_buffer = [0u8; (BUFFER_SIZE * 16)];

                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                    break;
                            }
                            _ = stop.cancelled() => {
                                    break;
                            }
                            result = stream.read(&mut unix_socker_buffer[..]) => {
                                    match result {
                                        Ok(0) => {
                                            log::warn!("unix_socker_client: connection closed");
                                            stop.cancel();
                                            break;
                                        }
                                        Ok(n) => {
                                            // unix_socker stream may contain several or partial HDLC frames
                                            for i in 0..n {
                                                // Push new byte from the end of buffer
                                                rx_buffer[BUFFER_SIZE-1] = unix_socker_buffer[i];

                                                // Check if it is contains a HDLC frame
                                                let frame = Hdlc::find(&rx_buffer[..]);
                                                if let Some(frame) = frame {
                                                    // Decode HDLC frame and deserialize packet
                                                    let frame_buffer = &mut rx_buffer[frame.0..frame.1+1];
                                                    let mut output = OutputBuffer::new(&mut hdlc_rx_buffer[..]);
                                                    if let Ok(_) = Hdlc::decode(frame_buffer, &mut output) {
                                                        println!("{:?}", output.as_slice());
                                                        if let Ok(packet) = Packet::deserialize(&mut InputBuffer::new(output.as_slice())) {
                                                            if PACKET_TRACE {
                                                                log::trace!("unix_socker_client: rx << ({}) {}", iface_address, packet);
                                                            }
                                                            let _ = rx_channel.send(RxMessage { address: iface_address, rssi: None, snr: None, packet: packet }).await;
                                                        } else {
                                                            log::warn!("unix_socker_client: couldn't decode packet");
                                                        }
                                                    } else {
                                                        log::warn!("unix_socker_client: couldn't decode hdlc frame");
                                                    }

                                                    // Remove current HDLC frame data
                                                    frame_buffer.fill(0);
                                                } else {
                                                    // Move data left
                                                    rx_buffer.copy_within(1.., 0);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!("unix_socker_client: connection error {}", e);
                                            break;
                                        }
                                    }
                                },
                        };
                    }
                })
            };

            // Start transmit task
            let tx_task = {
                let cancel = cancel.clone();
                let tx_channel = tx_channel.clone();
                let mut stream = write_stream;

                tokio::spawn(async move {
                    loop {
                        if stop.is_cancelled() {
                            break;
                        }

                        let mut hdlc_tx_buffer = [0u8; BUFFER_SIZE];
                        let mut tx_buffer = [0u8; BUFFER_SIZE];

                        let mut tx_channel = tx_channel.lock().await;

                        tokio::select! {
                            _ = cancel.cancelled() => {
                                    break;
                            }
                            _ = stop.cancelled() => {
                                    break;
                            }
                            Some(message) = tx_channel.recv() => {
                                let packet = message.packet;
                                if PACKET_TRACE {
                                    log::trace!("unix_socker_client: tx >> ({}) {}", iface_address, packet);
                                }
                                let mut output = OutputBuffer::new(&mut tx_buffer);
                                if let Ok(_) = packet.serialize(&mut output) {

                                    let mut hdlc_output = OutputBuffer::new(&mut hdlc_tx_buffer[..]);

                                    if let Ok(_) = Hdlc::encode(output.as_slice(), &mut hdlc_output) {
                                        let _ = stream.write_all(hdlc_output.as_slice()).await;
                                        let _ = stream.flush().await;
                                    }
                                }
                            }
                        };
                    }
                })
            };

            tx_task.await.unwrap();
            rx_task.await.unwrap();

            log::info!("unix_socker_client: disconnected from <{}>", addr);
        }

        iface_stop.cancel();
    }
}

impl Interface for UnixSocketClient {
    fn hw_mtu(&self) -> usize {
        2048
    }
}
