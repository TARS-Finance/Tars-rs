use crate::{htlc::validate::validate_utxos, indexer::primitives::Utxo};
use bitcoin::{
    absolute::LockTime, transaction::Version, Address, Amount, ScriptBuf, Sequence, TapSighashType,
    Transaction, TxIn, TxOut, Witness,
};
use eyre::{bail, eyre, Result};

/// Default transaction version used for HTLC transactions
pub const DEFAULT_TX_VERSION: Version = Version::TWO;

/// Default transaction locktime, set to zero (no timelock)
pub const DEFAULT_TX_LOCKTIME: LockTime = LockTime::ZERO;

/// Bitcoin dust limit (546 satoshis for most output types)
pub const DUST_LIMIT: u64 = 546;

/// Base transaction overhead, which includes the size of the following fixed components:
/// * version (4 bytes)
/// * locktime (4 bytes)
pub const TX_BASE_OVERHEAD: usize = 8;

/// Checks if an output value meets Bitcoin dust limits.
///
/// # Arguments
/// * `value` - Output value in satoshis
///
/// # Returns
/// * `bool` - True if value is above dust limit
fn is_above_dust_limit(value: u64) -> bool {
    value >= DUST_LIMIT
}

/// Sorts UTXOs by txid and vout for deterministic transaction structure.
///
/// This ensures that transaction construction remains consistent across
/// multiple builds with the same inputs.
///
/// # Arguments
/// * `utxos` - Slice of UTXOs to sort
///
/// # Returns
/// * `Vec<Utxo>` - Sorted vector of UTXOs
pub fn sort_utxos(utxos: &[Utxo]) -> Vec<Utxo> {
    let mut sorted_utxos = utxos.to_vec();
    sorted_utxos.sort_by(|a, b| a.txid.cmp(&b.txid).then(a.vout.cmp(&b.vout)));
    sorted_utxos
}

/// Creates unsigned transaction inputs from UTXOs.
///
/// Converts a slice of UTXOs into transaction inputs with empty signatures.
///
/// # Arguments
/// * `utxos` - A slice of UTXOs to convert into inputs.
/// * `witness` - A witness to associate with each input.
/// * `sequence` - A sequence number to associate with each input.
///
/// # Returns
/// * `Vec<TxIn>` - A vector of unsigned transaction inputs.
pub fn create_inputs_from_utxos(
    utxos: &[Utxo],
    witness: &Witness,
    sequence: Sequence,
) -> Vec<TxIn> {
    utxos
        .iter()
        .map(|utxo| TxIn {
            previous_output: utxo.to_outpoint(),
            script_sig: ScriptBuf::new(),
            sequence,
            witness: witness.clone(),
        })
        .collect()
}

/// Creates transaction outputs from output values with fee deduction from the largest value.
///
/// This function creates Bitcoin transaction outputs for a single recipient, where the transaction
/// fee is deducted from the largest output value. This approach ensures that the
/// fee burden falls on the largest output, minimizing the impact on smaller outputs.
///
/// # Arguments
/// * `output_values` - Vector of output values in satoshis
/// * `recipient` - Destination address for all outputs
/// * `fee` - Optional transaction fee in satoshis (defaults to 0 if None)
///
/// # Returns
/// * `Result<Vec<TxOut>>` - Vector of transaction outputs ready for inclusion in a transaction.
pub fn create_outputs(
    output_values: Vec<u64>,
    recipient: &Address,
    fee: Option<u64>,
) -> Result<Vec<TxOut>> {
    let fee = fee.unwrap_or(0);

    // Find index of the largest value
    let max_index = output_values
        .iter()
        .enumerate()
        .max_by_key(|(_, &value)| value)
        .map(|(i, _)| i)
        .ok_or_else(|| eyre!("Output values are empty"))?;

    output_values
        .into_iter()
        .enumerate()
        .map(|(i, value)| {
            let mut output_value = value;
            if i == max_index {
                output_value = output_value
                    .checked_sub(fee)
                    .ok_or_else(|| eyre!("Fee ({}) exceeds output value ({})", fee, value))?;
            }

            if !is_above_dust_limit(output_value) {
                bail!(
                    "Output value {} below dust limit ({})",
                    output_value,
                    DUST_LIMIT
                );
            }

            Ok(TxOut {
                value: Amount::from_sat(output_value),
                script_pubkey: recipient.script_pubkey(),
            })
        })
        .collect()
}

/// Creates transaction outputs representing previous HTLC outputs.
///
/// Generates outputs that match the original HTLC address outputs,
/// used for signature hash computation and witness validation.
///
/// # Arguments
/// * `utxos` - Slice of UTXOs to create outputs for
/// * `address` - The address these outputs were sent to
///
/// # Returns
/// * `Vec<TxOut>` - Vector of transaction outputs
pub fn create_previous_outputs(utxos: &[Utxo], address: &Address) -> Vec<TxOut> {
    utxos
        .iter()
        .map(|utxo| TxOut {
            value: Amount::from_sat(utxo.value),
            script_pubkey: address.script_pubkey(),
        })
        .collect()
}

/// Generates output values array based on the sighash type and UTXOs.
///
/// # Arguments
/// * `utxos` - Slice of UTXOs to process
/// * `sighash_type` - The Taproot sighash type that determines output distribution
///
/// # Returns
/// * `Result<Vec<u64>>` - Vector of output values or an error
///
/// # Errors
/// * When an unsupported sighash type is provided
pub fn get_output_values(utxos: &[Utxo], sighash_type: TapSighashType) -> Result<Vec<u64>> {
    match sighash_type {
        // One output per UTXO
        TapSighashType::SinglePlusAnyoneCanPay => Ok(utxos.iter().map(|utxo| utxo.value).collect()),

        // Single output for total input value
        TapSighashType::All => {
            let total_value: u64 = utxos.iter().map(|u| u.value).sum();
            Ok(vec![total_value])
        }

        _ => Err(eyre!(
            "Unsupported sighash type: {:?}. Only SinglePlusAnyoneCanPay and All are supported.",
            sighash_type
        )),
    }
}

/// Creates a Bitcoin transaction from a set of UTXOs with specified parameters.
///
/// This function constructs a unsigned Bitcoin transaction by combining UTXOs as inputs
/// and creating outputs . The transactbased on the specified sighash typeion can include
/// an optional fee that will be deducted from the maximum utxo value.
///
/// # Arguments
/// * `utxos` - Vector of UTXOs to use as transaction inputs
/// * `recpient` - The recipient address for the transaction outputs
/// * `witness` - The witness to associate with each input
/// * `sighash_type` - The Taproot sighash type that determines output value distribution
/// * `sequence` - The sequence number for all transaction inputs
/// * `fee` - Optional fee amount to deduct from the total input value
///
/// # Returns
/// * `Result<Transaction>` - The constructed Bitcoin transaction or an error
pub fn build_tx(
    utxos: &[Utxo],
    recpient: &Address,
    witness: &Witness,
    sequence: Sequence,
    sighash_type: TapSighashType,
    fee: Option<u64>,
) -> Result<Transaction> {
    validate_utxos(utxos)?;

    // Calculate output values based on sighash type (e.g., single output vs multiple outputs)
    let output_values = get_output_values(utxos, sighash_type)?;

    let outputs = create_outputs(output_values, recpient, fee)?;

    let inputs = create_inputs_from_utxos(&utxos, &witness, sequence);

    // Construct and return the unsigned transaction
    Ok(Transaction {
        version: DEFAULT_TX_VERSION,
        lock_time: DEFAULT_TX_LOCKTIME,
        input: inputs,
        output: outputs,
    })
}

/// Returns the size in bytes needed to encode a length value using Bitcoin's VarInt format.
///
/// This function is adapted from the `bitcoin` crate's internal implementation,
/// specifically from the `encode::VarInt::len` logic in `bitcoin/src/consensus/encode.rs`.
///
/// Reference:
/// https://docs.rs/bitcoin/latest/src/bitcoin/consensus/encode.rs.html
///
/// # Arguments
/// * `length` - The value to be encoded
///
/// # Returns
/// * `usize` - Number of bytes required to encode the length as a VarInt
pub const fn size(length: usize) -> usize {
    match length {
        0..=0xFC => 1,
        0xFD..=0xFFFF => 3,
        0x10000..=0xFFFFFFFF => 5,
        _ => 9,
    }
}
