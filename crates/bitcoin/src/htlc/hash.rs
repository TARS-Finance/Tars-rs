use super::{
    primitives::HTLCParams,
    tx::{build_tx, create_previous_outputs, sort_utxos},
};
use crate::{
    get_htlc_address, htlc::validate::validate_hash_generation_params, indexer::primitives::Utxo,
    instant_refund_leaf,
};
use bitcoin::{
    hashes::Hash,
    sighash::{Prevouts, SighashCache},
    Address, Network, Sequence, TapLeafHash, TapSighashType, Transaction, TxOut, Witness,
};
use eyre::{bail, eyre, Result};

/// Generates signature hashes for Taproot script path spending
///
/// This struct is used to generate signature message hashes for each transaction
/// input using the specified sighash type. These hashes are used for creating
/// Schnorr signatures for Taproot script path spending.
#[derive(Clone, Debug)]
pub struct TapScriptSpendSigHashGenerator {
    tx: Transaction,
    leaf_hash: TapLeafHash,
}

impl TapScriptSpendSigHashGenerator {
    /// Create a new `TapScriptSpendSigHashGenerator` instance
    ///
    /// # Arguments
    /// * `tx` - Transaction to generate hashes for
    /// * `leaf_hash` - Taproot script leaf hash for the spending path
    ///
    /// # Returns
    /// * `Self` - A new `TapScriptSpendSigHashGenerator` instance
    pub fn new(tx: Transaction, leaf_hash: TapLeafHash) -> Self {
        Self { tx, leaf_hash }
    }

    /// Generate a signature hash for a single input
    ///
    /// Generates a signature message hash for the specified input index using
    /// the specified sighash type.
    ///
    /// # Arguments
    /// * `input_index` - Index of the input to generate the hash for
    /// * `prevouts` - Previous output information for the input
    /// * `sighash_type` - HashType of an input's signature
    ///
    /// # Returns
    /// * `Result<[u8; 32]>` - Raw signature hash for the input
    ///
    /// # Errors
    /// * When signature hash computation fails for the input
    fn generate(
        &mut self,
        input_index: usize,
        prevouts: &Prevouts<TxOut>,
        sighash_type: TapSighashType,
    ) -> Result<[u8; 32]> {
        let mut sighash_cache = SighashCache::new(&mut self.tx);
        let sighash = sighash_cache
            .taproot_script_spend_signature_hash(
                input_index,
                prevouts,
                self.leaf_hash,
                sighash_type,
            )
            .map_err(|e| {
                eyre!(
                    "Failed to generate signature hash for input {}: {e}",
                    input_index
                )
            })?;

        Ok(sighash.to_raw_hash().to_byte_array())
    }

    /// Generate a signature hash for a single input with a single previous output
    ///
    /// Generates a signature message hash for the specified input index using
    /// the specified sighash type and a single previous output.
    ///
    /// # Arguments
    /// * `input_index` - Index of the input to generate the hash for
    /// * `previous_output` - Previous output information for the input
    /// * `sighash_type` - HashType of an input's signature
    ///
    /// # Returns
    /// * `Result<[u8; 32]>` - Raw signature hash for the input
    ///
    /// # Errors
    /// * When signature hash computation fails for the input
    pub fn with_prevout(
        &mut self,
        input_index: usize,
        previous_output: &TxOut,
        sighash_type: TapSighashType,
    ) -> Result<[u8; 32]> {
        let prevouts = Prevouts::One(input_index, previous_output.clone());
        self.generate(input_index, &prevouts, sighash_type)
    }

    /// Generate signature hashes for all inputs with all previous outputs
    ///
    /// Generates signature message hashes for all inputs using the specified
    /// sighash type and all previous outputs.
    ///
    /// # Arguments
    /// * `previous_outputs` - Previous output information for each input
    /// * `sighash_type` - HashType of an input's signature
    ///
    /// # Returns
    /// * `Result<Vec<[u8; 32]>>` - Raw signature hashes for all inputs
    ///
    /// # Errors
    /// * When signature hash computation fails for any input
    pub fn with_all_prevouts(
        &mut self,
        previous_outputs: &[TxOut],
        sighash_type: TapSighashType,
    ) -> Result<Vec<[u8; 32]>> {
        if self.tx.input.len() != previous_outputs.len() {
            bail!(
                "Number of transaction inputs ({}) does not match number of previous outputs ({})",
                self.tx.input.len(),
                previous_outputs.len()
            );
        }

        let mut sighashes = Vec::with_capacity(previous_outputs.len());
        let prevouts = Prevouts::All(previous_outputs);

        for input_index in 0..self.tx.input.len() {
            sighashes.push(self.generate(input_index, &prevouts, sighash_type)?);
        }

        Ok(sighashes)
    }
}

/// Generates transaction hashes that need to be signed for Bitcoin SACP (Signature Adaptable Commitment Protocol)
///
/// This function:
/// 1. Validates input parameters
/// 2. Retrieves and sorts UTXOs to ensure deterministic processing
/// 3. Creates transaction inputs and collects previous outputs using the HTLC address
/// 4. Creates a 1-to-1 mapping between inputs and outputs to conform with SINGLE|ANYONECANPAY sighash
/// 5. Deducts the optional fee from the output which has the max output value.
/// 6. Generates the taproot script leaf for instant refund
/// 7. Generates signature hashes for each input using taproot script spend
///
/// # Arguments
/// * `htlc_params` - Contains swap details including amount and public keys
/// * `utxos` - Slice of UTXOs to spend from the HTLC address
/// * `recipient` - Bitcoin address where funds will be sent
/// * `network` - Bitcoin network (e.g., Testnet, Mainnet)
/// * `fee` - Optional transaction fee to deduct from the output
///
/// # Returns
/// * `Result<Vec<[u8; 32]>>` - Raw signature hashes for each input
/// * `Err` with descriptive message if any step fails
pub fn generate_instant_refund_hash(
    htlc_params: &HTLCParams,
    utxos: &[Utxo],
    recipient: &Address,
    network: Network,
    fee: Option<u64>,
) -> Result<Vec<[u8; 32]>> {
    // Validate all input parameters
    validate_hash_generation_params(htlc_params, utxos, network)?;

    let sighash_type = TapSighashType::SinglePlusAnyoneCanPay;
    let utxos = sort_utxos(utxos);
    let htlc_address = get_htlc_address(htlc_params, network)?;
    let previous_outputs = create_previous_outputs(&utxos, &htlc_address);
    let instant_refund_tx = build_tx(
        &utxos,
        recipient,
        &Witness::new(),
        Sequence::MAX,
        sighash_type,
        fee,
    )?;

    let leaf_hash =
        instant_refund_leaf(&htlc_params.initiator_pubkey, &htlc_params.redeemer_pubkey)
            .tapscript_leaf_hash();

    let mut sighash_generator = TapScriptSpendSigHashGenerator::new(instant_refund_tx, leaf_hash);

    let mut message_hashes = Vec::with_capacity(utxos.len());
    for input_index in 0..utxos.len() {
        message_hashes.push(sighash_generator.with_prevout(
            input_index,
            &previous_outputs[input_index],
            sighash_type,
        )?);
    }

    Ok(message_hashes)
}
