#![deny(warnings)]

use std::collections::HashMap;
use std::net::SocketAddr;

use futures::{Future, Sink, Stream};
use tokio::codec::{BytesCodec, FramedWrite};
use tokio::net::{UdpFramed, UdpSocket};

fn start_aggregator(
    address: SocketAddr,
    file: tokio::fs::File,
) -> Result<(), Box<std::error::Error>> {
    // create input Stream
    let socket = UdpSocket::bind(&address)?;
    let (_, in_stream) = UdpFramed::new(socket, BytesCodec::new()).split();

    // create output Sink
    let out_sink = FramedWrite::new(file, BytesCodec::new());

    // Create a new Stream by modifying the input Stream. Split on newline
    // and prefix each line with the remote ip.
    let mut incomplete_lines: HashMap<String, Vec<u8>> = HashMap::new();
    let formatted = in_stream.map(move |(buf, peer)| {
        let mut outbuf = bytes::BytesMut::new();
        let mut line_iter = buf.split(|c| *c == b'\n').peekable();
        while line_iter.peek().is_some() {
            let line = line_iter.next().unwrap();
            if line_iter.peek().is_some() {
                outbuf.extend_from_slice(peer.ip().to_string().as_bytes());
                outbuf.extend_from_slice(b" ");
                if let Some(incomplete_line) =
                    incomplete_lines.remove(&peer.ip().to_string())
                {
                    outbuf.extend_from_slice(&incomplete_line);
                }
                outbuf.extend_from_slice(&line);
                outbuf.extend_from_slice(b"\n");
            } else {
                match incomplete_lines.get_mut(&peer.ip().to_string()) {
                    Some(incomplete_line) => {
                        incomplete_line.extend_from_slice(&line);
                    }
                    None => {
                        incomplete_lines
                            .insert(peer.ip().to_string(), line.to_vec());
                    }
                }
            }
        }
        outbuf.freeze()
    });

    // connect the formatted input to the output Sink
    let service = out_sink
        .send_all(formatted)
        .map(|(_t, _u)| {
            eprintln!("when does this happen?");
        })
        .map_err(|e| {
            eprintln!("{}", e);
            std::process::exit(1);
        });

    tokio::spawn(service);

    Ok(())
}

fn main() -> Result<(), Box<std::error::Error>> {
    let matches = clap::App::new("logaggrd")
        .version("0.1.0")
        .about("logaggrd - simple udp log aggregator daemon")
        .arg(
            clap::Arg::with_name("address::file")
                .help("listen on <address> and write to <file>")
                .multiple(true)
                .required(true),
        )
        .get_matches();

    tokio::run(futures::future::lazy(move || {
        for addr_file in matches.values_of("address::file").unwrap() {
            let colons = match addr_file.rfind("::") {
                Some(colons) => colons,
                None => {
                    eprintln!(
                        "unable to parse argument as address::file: {}",
                        addr_file
                    );
                    std::process::exit(1);
                }
            };
            let address: SocketAddr = match addr_file[..colons].parse() {
                Ok(address) => address,
                Err(e) => {
                    eprintln!("{}: {}", &addr_file[..colons], e);
                    std::process::exit(1);
                }
            };
            let file = &addr_file[colons + 2..];
            let file = file.to_string();
            let file_clone = file.clone();

            let open_and_start = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(file.clone())
                .and_then(move |f| {
                    match start_aggregator(address, f) {
                        Ok(()) => (),
                        Err(e) => {
                            eprintln!("{}::{}: {}", address, file_clone, e);
                            std::process::exit(1);
                        }
                    }
                    eprintln!(
                        "listening on {}, writing to {}",
                        address, file_clone
                    );
                    Ok(())
                })
                .map_err(move |e| {
                    eprintln!("{}: {}", file, e);
                    std::process::exit(1);
                });
            tokio::spawn(open_and_start);
        }
        Ok(())
    }));

    println!("tokio::run() returned");
    Ok(())
}
