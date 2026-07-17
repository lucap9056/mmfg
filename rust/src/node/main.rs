use std::env;
use mmfg::node::Node;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::sync::Arc;
use mmfg::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <socket_path>", args[0]);
        std::process::exit(1);
    }
    let socket_path = &args[1];

    let handler: mmfg::node::Handler = Arc::new(|mut conn| {
        Box::pin(async move {
            let mut buf = Vec::new();
            // ShmConnection needs some data to read from, but here we just read whatever is in the stripe
            if let Ok(_) = conn.read_to_end(&mut buf).await {
                println!("Node received: {} bytes", buf.len());
                // Echo handler
                let _ = conn.write_all(&buf).await;
                let _ = conn.flush().await;
            }
        })
    });

    println!("READY"); // Integration test script waits for this
    Node::listen(socket_path, handler).await
}
