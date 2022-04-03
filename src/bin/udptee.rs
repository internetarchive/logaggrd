#![deny(warnings)]

use std::iter::Iterator;
use std::net::{SocketAddr, ToSocketAddrs};

use futures::sink::BoxSink;
use futures::{Future, Sink, Stream};
use tokio::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio::net::{UdpFramed, UdpSocket};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap::App::new("udptee")
        .version("0.1.0")
        .about(
            "udptee - like `tee` but duplicates output to udp addresses \
             instead of files",
        )
        .arg(
            clap::Arg::with_name("ADDRESS")
                .help("destination address host:port(s)")
                .multiple(true),
        )
        .get_matches();

    let addresses: Vec<String> = match matches.values_of("ADDRESS") {
        Some(addresses) => addresses.map(|address| address.into()).collect(),
        None => vec![],
    };

    let stdout_sink =
        Box::new(FramedWrite::new(tokio::io::stdout(), BytesCodec::new()));

    let in_stream = FramedRead::new(tokio::io::stdin(), BytesCodec::new())
        .map(|bytes_mut| bytes_mut.freeze());

    let starter = futures::stream::iter_ok::<_, ()>(addresses)
        .map(move |address| {
            // synchronous dns resolution, meh
            let remote_addr = match address.to_socket_addrs() {
                Ok(mut addr_iter) => match addr_iter.next() {
                    Some(addr) => addr,
                    None => {
                        eprintln!("{} does not resolve to anything?", address);
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("{}: {}", address, e);
                    std::process::exit(1);
                }
            };

            let local_addr: SocketAddr = if remote_addr.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            }
            .parse()
            .unwrap();

            let socket = UdpSocket::bind(&local_addr).unwrap();
            let (udp_sink, _) =
                UdpFramed::new(socket, BytesCodec::new()).split();

            let udp_sink = udp_sink.with(move |bytes: bytes::Bytes| {
                // eprintln!("sending {} bytes to {}", bytes.len(), remote_addr);
                futures::future::ok((bytes, remote_addr))
            });

            udp_sink
        })
        .fold::<_, BoxSink<_, _>, _>(stdout_sink, |fanout_sink, udp_sink| {
            let result = fanout_sink.fanout(udp_sink);
            futures::future::ok(Box::new(result) as BoxSink<_, _>)
        })
        .map(move |fanout_sink| {
            let tee_pipe = fanout_sink
                .send_all(in_stream)
                .map(|_| ()) // input finished
                .map_err(|e: std::io::Error| {
                    // not sure when this error can happen
                    eprintln!("error: {:?}", e);
                    std::process::exit(1);
                });
            tokio::spawn(tee_pipe);
        });

    tokio::run(starter);

    Ok(())
}
