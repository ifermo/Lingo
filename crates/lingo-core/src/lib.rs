//! Domain, storage, identity and transfer primitives for Lingo.

pub mod config;
pub mod identity;
pub mod model;
pub mod msg;
pub mod paths;
pub mod service;
pub mod store;

pub const APP_NAME: &str = "Lingo";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SYNC_AUDIENCE: &str = "api.lingo.app";
