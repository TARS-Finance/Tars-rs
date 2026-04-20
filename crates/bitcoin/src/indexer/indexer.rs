use super::{primitives::Utxo, traits::Indexer};
use crate::{
    indexer::primitives::{OutSpend, TransactionMetadata},
    UtxoStatus,
};
use alloy::hex;
use async_trait::async_trait;
use bitcoin::{Address, Transaction};
use eyre::{eyre, Result};
use serde::Deserialize;
use std::time::Duration;
use tracing::debug;

/// Default timeout in seconds for indexer client requests
const INDEXER_CLIENT_TIMEOUT_SECS: u64 = 5;

/// Endpoint for retrieving unspent transaction outputs (UTXOs) for an address
const UTXO_ENDPOINT: &str = "/address/{}/utxo";

/// Endpoint for retrieving the current block height
const BLOCK_HEIGHT_ENDPOINT: &str = "/blocks/tip/height";

/// Endpoint for retrieving a transaction's raw hex data
const TX_HEX_ENDPOINT: &str = "/tx/{}/hex";

/// Endpoint for retrieving detailed transaction metadata
const TX_ENDPOINT: &str = "/tx/{}";

/// Endpoint for retrieving spending information for a transaction's outputs
const TX_OUTSPENDS_ENDPOINT: &str = "/tx/{}/outspends";

/// Endpoint for submitting a new transaction to the network
const SUBMIT_TX_ENDPOINT: &str = "/tx";

/// A helper struct used for deserializing UTXO data from JSON responses.
///
/// Unlike [`Utxo`], this struct represents the transaction ID (`txid`) as a
/// `String` instead of a [`Txid`] type, since [`Txid`] does not implement
/// `Deserialize`. This makes it suitable for use with `serde` when parsing
/// JSON data, after which it can be converted into a [`Utxo`].
#[derive(Debug, Deserialize)]
pub struct UtxoJson {
    /// The transaction ID that contains this output (as a string).
    pub txid: String,

    /// The output index (vout) within the transaction.
    pub vout: u32,

    /// The value of this UTXO in satoshis (1 BTC = 100,000,000 satoshis).
    pub value: u64,

    /// The status of the UTXO.
    pub status: UtxoStatus,
}

/// A client for interacting with Bitcoin blockchain indexer services.
#[derive(Debug, Clone)]
pub struct BitcoinIndexerClient {
    /// Base URL of the indexer service
    url: String,

    /// HTTP client used for making requests to the indexer
    client: reqwest::Client,
}

impl BitcoinIndexerClient {
    /// Creates a new Bitcoin indexer client.
    ///
    /// # Arguments
    /// * `url` - The base URL of the indexer service
    /// * `timeout_secs` - Optional timeout in seconds (defaults to 5 seconds)
    ///
    /// # Returns
    /// A new `BitcoinIndexerClient` instance or an error if the HTTP client
    /// could not be created
    pub fn new(url: String, timeout_secs: Option<u64>) -> Result<Self> {
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(INDEXER_CLIENT_TIMEOUT_SECS));

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| eyre!("Failed to build client: {e}"))?;

        Ok(Self { url, client })
    }

    /// Helper method to handle HTTP response errors
    async fn handle_response_error(
        &self,
        endpoint: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let err_msg = response
            .text()
            .await
            .unwrap_or_else(|_| "Failed to read error message".to_string());

        Err(eyre!(
            "Request to {endpoint} failed with status {status}: {err_msg}"
        ))
    }
}

#[async_trait]
impl Indexer for BitcoinIndexerClient {
    /// Retrieves a transaction by its transaction ID and returns it as a Transaction object.
    async fn get_tx_hex(&self, txid: &str) -> Result<Transaction> {
        let endpoint = format!("{}{}", self.url, TX_HEX_ENDPOINT.replace("{}", txid));
        debug!(target: "indexer", "Fetching transaction {txid}");

        let resp = self
            .client
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| eyre!("Failed to send GET request to fetch tx {txid}: {e}"))?;

        let resp = self
            .handle_response_error(TX_HEX_ENDPOINT.replace("{}", txid).as_str(), resp)
            .await?;

        let hex = resp
            .text()
            .await
            .map_err(|e| eyre!("Failed to read transaction hex from response: {e}"))?;

        let tx_bytes =
            hex::decode(&hex).map_err(|e| eyre!("Failed to decode transaction hex: {e}"))?;

        bitcoin::consensus::deserialize(&tx_bytes)
            .map_err(|e| eyre!("Failed to deserialize transaction: {e}"))
    }

    /// Retrieves detailed transaction information from the indexer.
    ///
    /// # Arguments
    /// * `txid` - The transaction ID to fetch details for
    ///
    /// # Returns
    /// A `TransactionMetadata` containing the transaction details, or an error if the request failed
    async fn get_tx(&self, txid: &str) -> Result<TransactionMetadata> {
        let endpoint = format!("{}{}", self.url, TX_ENDPOINT.replace("{}", txid));
        debug!(target: "indexer", "Fetching transaction details for {txid}");

        let resp = self
            .client
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| eyre!("Failed to send GET request to fetch tx details {txid}: {e}"))?;

        let resp = self
            .handle_response_error(TX_ENDPOINT.replace("{}", txid).as_str(), resp)
            .await?;

        resp.json::<TransactionMetadata>()
            .await
            .map_err(|e| eyre!("Failed to parse transaction response: {e}"))
    }

    /// Submits a transaction to the Bitcoin network.
    async fn submit_tx(&self, tx: &Transaction) -> Result<()> {
        let endpoint = format!("{}{}", self.url, SUBMIT_TX_ENDPOINT);
        debug!(target: "indexer", "Submitting transaction");

        let tx_hex = hex::encode(bitcoin::consensus::serialize(tx));

        let resp = self
            .client
            .post(&endpoint)
            .header("Content-Type", "application/text")
            .body(tx_hex)
            .send()
            .await
            .map_err(|e| eyre!("Failed to submit transaction: {e}"))?;

        self.handle_response_error(SUBMIT_TX_ENDPOINT, resp)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Retrieves the current block height of the Bitcoin blockchain.
    async fn get_block_height(&self) -> Result<u64> {
        let url = format!("{}{}", self.url, BLOCK_HEIGHT_ENDPOINT);
        debug!(target: "indexer", "Fetching block height");

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| eyre!("Failed to send request to fetch block height: {e}"))?;

        let resp = self
            .handle_response_error(BLOCK_HEIGHT_ENDPOINT, resp)
            .await?;

        let height_str = resp
            .text()
            .await
            .map_err(|e| eyre!("Failed to read block height response text: {e}"))?
            .trim()
            .to_string();

        height_str
            .parse::<u64>()
            .map_err(|e| eyre!("Invalid block height format: {e}"))
    }

    /// Retrieves the unspent transaction outputs (UTXOs) for a given address.
    async fn get_utxos(&self, address: &Address) -> Result<Vec<Utxo>> {
        let endpoint = format!(
            "{}{}",
            self.url,
            UTXO_ENDPOINT.replace("{}", &address.to_string())
        );
        debug!(target: "indexer", "Fetching UTXOs for address {address}");

        let resp = self
            .client
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| eyre!("Failed to fetch UTXOs for address {address}: {e}"))?;

        let resp = self
            .handle_response_error(
                UTXO_ENDPOINT.replace("{}", &address.to_string()).as_str(),
                resp,
            )
            .await?;

        let utxos_json: Vec<UtxoJson> = resp
            .json()
            .await
            .map_err(|e| eyre!("Failed to parse response : {e}"))?;

        utxos_json
            .iter()
            .map(|utxo_json| Utxo::try_from(utxo_json))
            .collect()
    }

    /// Retrieves the spending status of all outputs for a given transaction.
    async fn get_tx_outspends(&self, txid: &str) -> Result<Vec<OutSpend>> {
        let endpoint = format!("{}{}", self.url, TX_OUTSPENDS_ENDPOINT.replace("{}", txid));
        debug!(target: "indexer", "Fetching outspends for transaction {txid}");

        // Send request to get outspends for the given transaction
        let resp = self
            .client
            .get(&endpoint)
            .send()
            .await
            .map_err(|e| eyre!("Failed to send GET request for outspends: {e}"))?;

        let outspends: Vec<OutSpend> = resp
            .json()
            .await
            .map_err(|e| eyre!("Failed to parse outspends response: {e}"))?;

        Ok(outspends)
    }
}
