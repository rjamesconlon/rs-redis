use rs_redis::{network};

#[tokio::main]
async fn main() {
    network::start_network().await.ok();
}