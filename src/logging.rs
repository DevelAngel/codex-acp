use std::io;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

pub fn init_from_env() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .init();

    Ok(())
}
