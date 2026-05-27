//! MQTT Protocol Core Library
//!
//! Provides packet type definitions, encoding/decoding for MQTT 3.1.1 and 5.0.

#![deny(unsafe_code)]
#![allow(missing_docs)]

pub mod common;
pub mod v3;
pub mod v5;
pub mod codec;

// Re-exports for convenience
pub use common::*;
pub use codec::*;
