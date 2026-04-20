use super::{
    ALPEN_MAINNET, ALPEN_REGTEST, ALPEN_SIGNET, BITCOIN_MAINNET, BITCOIN_REGTEST, BITCOIN_TESTNET,
};
use bitcoin::{Address, Network};
use eyre::{eyre, Context, Result};
use orderbook::primitives::MatchedOrderVerbose;
use std::str::FromStr;

/// Get the Bitcoin network for this swap
///
/// # Arguments
/// * `chain` - The chain identifier string: "bitcoin", "bitcoin_testnet", or "bitcoin_regtest"
///
/// # Returns
/// * `Ok(Network)` with the Bitcoin network type
/// * `Err` if the chain is not a valid Bitcoin network
pub fn get_bitcoin_network(chain: &str) -> Result<Network> {
    match chain.to_lowercase().as_str() {
        BITCOIN_MAINNET => Ok(Network::Bitcoin),
        BITCOIN_REGTEST => Ok(Network::Regtest),
        BITCOIN_TESTNET => Ok(Network::Testnet),
        _ => Err(eyre::eyre!(
            "Expected one of the following networks: {}, {}, {}",
            BITCOIN_MAINNET,
            BITCOIN_TESTNET,
            BITCOIN_REGTEST
        )),
    }
}

/// Validates a Bitcoin address for a specific network.
///
/// # Arguments
/// * `addr` - The Bitcoin address to validate
/// * `network` - The Bitcoin network (Mainnet, Testnet, etc.)
///
/// # Returns
/// * `Ok(Address)` with the validated address
/// * `Err` if the address is invalid or doesn't match the network
pub fn validate_btc_address_for_network(addr: &str, network: Network) -> Result<Address> {
    let address = Address::from_str(addr)
        .with_context(|| format!("Invalid Bitcoin address format: {}", addr))?;

    if address.is_valid_for_network(network) {
        Ok(address.assume_checked())
    } else {
        Err(eyre::eyre!(
            "Address {} is not valid for network {:?}",
            addr,
            network
        ))
    }
}

/// Get the Bitcoin recipient address from a matched order
///
/// # Arguments
/// * `order` - The matched order
///
/// # Returns
/// * `Ok(Address)` with the Bitcoin recipient address
pub fn get_bitcoin_recipient_address(order: &MatchedOrderVerbose) -> Result<Address> {
    // Get the Bitcoin network from the order
    let network = get_bitcoin_network(&order.source_swap.chain)?;

    // Get the Bitcoin recipient address from the order
    let recipient_str = order
        .create_order
        .additional_data
        .bitcoin_optional_recipient
        .clone()
        .ok_or(eyre!("Bitcoin optional address is required"))?;

    // Validate the Bitcoin recipient address for the network
    validate_btc_address_for_network(&recipient_str, network)
}

/// Enum to represent Alpen networks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlpenNetwork {
    Mainnet,
    Testnet,
    Regtest,
}

impl AlpenNetwork {
    pub fn to_bitcoin_network(self) -> Network {
        match self {
            AlpenNetwork::Mainnet => Network::Bitcoin,
            AlpenNetwork::Testnet => Network::Testnet,
            AlpenNetwork::Regtest => Network::Regtest,
        }
    }
}

/// Get the Alpen network from a string identifier
///
/// # Arguments
/// * `chain` - The chain identifier string: "alpen", "alpen_testnet", or "alpen_regtest"
///
/// # Returns
/// * `Ok(AlpenNetwork)` for valid identifiers
/// * `Err` otherwise
pub fn get_alpen_network(chain: &str) -> Result<AlpenNetwork> {
    match chain.to_lowercase().as_str() {
        ALPEN_MAINNET => Ok(AlpenNetwork::Mainnet),
        ALPEN_SIGNET => Ok(AlpenNetwork::Testnet),
        ALPEN_REGTEST => Ok(AlpenNetwork::Regtest),
        _ => Err(eyre::eyre!(
            "Expected one of the following Alpen networks: {}, {}, {}",
            ALPEN_MAINNET,
            ALPEN_SIGNET,
            ALPEN_REGTEST
        )),
    }
}

/// Validates an Alpen address for a specific network.
///
/// # Arguments
/// * `addr` - The address to validate
/// * `network` - The Alpen network (Mainnet, Testnet, etc.)
///
/// # Returns
/// * `Ok(Address)` with the validated address
/// * `Err` if the address is invalid or doesn't match the network
pub fn validate_alpen_address_for_network(addr: &str, network: AlpenNetwork) -> Result<Address> {
    let bitcoin_network = network.to_bitcoin_network();
    let address = Address::from_str(addr)
        .with_context(|| format!("Invalid Alpen address format: {}", addr))?;

    if address.is_valid_for_network(bitcoin_network) {
        Ok(address.assume_checked())
    } else {
        Err(eyre::eyre!(
            "Address {} is not valid for Alpen network {:?}",
            addr,
            network
        ))
    }
}
