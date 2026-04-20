use crate::{generate_instant_refund_hash, indexer::primitives::Utxo, HTLCParams};
use alloy::hex::{self, ToHexExt};
use bitcoin::{
    consensus, hashes::Hash, key::Secp256k1, secp256k1::Message, taproot::Signature, Address,
    Network, TapSighash, TapSighashType, Transaction, XOnlyPublicKey,
};
use eyre::{bail, eyre, Result};
use sha2::{Digest, Sha256};
use utils::ToBytes;

/// Verifies a Bitcoin SACP (Single Plus Anyone Can Pay) instant refund transaction
///
/// 1. Validates that the number of inputs matches the number of expected hashes
/// 2. Verifies Schnorr signatures for each input using the initiator's public key
/// 3. Ensures at least one input references the expected initiate transaction hash
///
/// # Arguments
/// * `instant_refund_sacp_hex` - Hex-encoded bytes of the instant refund SACP transaction
/// * `initiate_tx_hash` - Hash of the original initiate transaction
/// * `htlc_params` - HTLC parameters containing public keys and other contract details
/// * `utxos` - List of UTXOs used as inputs for the instant refund transaction
/// * `recipient` - Bitcoin address that will receive the refunded funds
/// * `network` - Bitcoin network (mainnet, testnet, regtest)
///
/// # Returns
/// * `Ok(())` - Transaction is valid and properly signed
pub fn validate_instant_refund_sacp_tx(
    instant_refund_sacp_hex: &str,
    initiate_tx_hash: &str,
    htlc_params: &HTLCParams,
    utxos: &[Utxo],
    recipient: &Address,
    network: Network,
) -> Result<()> {
    let initiate_tx_hash = initiate_tx_hash.to_lowercase();

    let instant_refund_sacp_bytes = hex::decode(instant_refund_sacp_hex).map_err(|e| {
        eyre!(
            "Failed to decode instant refund SACP transaction bytes: {:?}",
            e
        )
    })?;

    let instant_refund_sacp: Transaction =
        bitcoin::consensus::deserialize(&instant_refund_sacp_bytes).map_err(|e| {
            eyre!(
                "Failed to deserialize instant refund SACP transaction: {:?}",
                e
            )
        })?;

    let input_total: u64 = utxos.iter().map(|utxo| utxo.value).sum();
    let output_total: u64 = instant_refund_sacp
        .output
        .iter()
        .map(|output| output.value.to_sat())
        .sum();

    if output_total > input_total {
        return Err(eyre!(
            "Total output value ({}) exceeds input total ({}) in HTLC params",
            output_total,
            input_total
        ));
    }

    let fee = input_total - output_total;

    let hashes = generate_instant_refund_hash(&htlc_params, &utxos, &recipient, network, Some(fee))
        .map_err(|e| eyre!("Failed to generate instant refund hash : {:?}", e))?;

    if instant_refund_sacp.input.len() != hashes.len() {
        bail!("Mismatch between transaction inputs and expected hashes");
    }

    let mut has_matching_input = false;

    for (input, hash) in instant_refund_sacp.input.iter().zip(hashes) {
        if input.witness.len() != 4 {
            bail!("Instant refund SACP transaction input witness must have exactly 4 elements")
        }

        let signature = input
            .witness
            .nth(1)
            .ok_or_else(|| eyre!("Missing initiator's signature in SACP transaction input"))?;

        validate_schnorr_signature(
            &htlc_params.initiator_pubkey,
            &signature,
            &hash,
            TapSighashType::SinglePlusAnyoneCanPay,
        )
        .map_err(|e| eyre!("Invalid Schnorr signature : {:?}", e))?;

        if initiate_tx_hash.eq(&input.previous_output.txid.to_string()) {
            has_matching_input = true;
        }
    }

    if !has_matching_input {
        bail!("Input txid does not match expected transaction hash");
    }

    Ok(())
}

/// Validates a transaction
///
/// 1. Validates that the inputs match the expected transaction hash
/// 2. Validates that the outputs match the expected recipient
///
/// # Arguments
/// * `tx_bytes` - Hex-encoded bytes of the transaction
/// * `input_tx_hash` - Transaction id (txid)
/// * `recipient` - Bitcoin address that will receive the redeemed funds
///
/// # Returns
/// * `Result<Transaction>` - The transaction if valid
/// * `Err` - If the transaction is invalid
pub fn validate_tx(
    tx_bytes: &[u8],
    input_tx_hash: &str,
    recipient: &Address,
) -> Result<Transaction> {
    let tx: Transaction = consensus::deserialize(&tx_bytes)
        .map_err(|e| eyre!("Failed to deserialie tx : {:#?}", e))?;

    // Validate the inputs
    for input in tx.input.iter() {
        if input.previous_output.txid.to_string() != input_tx_hash {
            bail!("Tx has invalid inputs");
        }
    }

    // Validate the outputs
    for output in tx.output.iter() {
        if output.script_pubkey != recipient.script_pubkey() {
            bail!("Tx has invalid outputs");
        }
    }

    Ok(tx)
}

/// Validates a Schnorr signature against a public key for Bitcoin transactions.
///
/// This function:
/// 1. Validates the binary-encoded Schnorr signature and message hash.
/// 2. Performs cryptographic verification against the provided public key.
///
/// # Arguments
/// * `verifying_key` - The XOnly public key used for signature verification.
/// * `signature` - The raw bytes of the taproot Schnorr signature (including sighash type).
/// * `message_hash` - The raw bytes of the message hash to verify.
/// * `hash_type` - The expected sighash type for the signature.
///
/// # Returns
/// * `Ok(())` if the provided signature is valid.
/// * `Err` with a descriptive message if validation fails.
pub fn validate_schnorr_signature(
    verifying_key: &XOnlyPublicKey,
    signature: &[u8],
    message_hash: &[u8],
    hash_type: TapSighashType,
) -> Result<()> {
    let secp = Secp256k1::verification_only();

    let signature = Signature::from_slice(signature)
        .map_err(|e| eyre!(format!("Invalid Schnorr signature format: {}", e)))?;

    if !hash_type.eq(&signature.sighash_type) {
        bail!(
            "Invalid signature hash type: expected {}, got {}",
            hash_type,
            signature.sighash_type
        )
    }

    let message_hash = TapSighash::from_slice(message_hash)
        .map_err(|e| eyre!(format!("Invalid message hash format: {}", e)))?;

    let msg = Message::from(message_hash);

    if let Err(e) = secp.verify_schnorr(&signature.signature, &msg, verifying_key) {
        bail!(format!("Signature verification failed: {}", e))
    }

    Ok(())
}

/// Validates UTXO inputs
///
/// Checks that all UTXOs have positive values and are properly formatted.
///
/// # Arguments
/// * `utxos` - Slice of UTXOs to validate
///
/// # Returns
/// * `Result<()>` - Ok if all UTXOs are valid
///
/// # Errors
/// * When any UTXO has zero or invalid value
pub fn validate_utxos(utxos: &[Utxo]) -> Result<()> {
    if utxos.is_empty() {
        bail!("No UTXOs provided")
    }

    for utxo in utxos {
        if utxo.value == 0 {
            bail!(
                "UTXO with txid {} and vout {} has zero value",
                utxo.txid,
                utxo.vout
            )
        }
    }

    Ok(())
}

/// Validates that the provided secret matches the expected secret hash.
///
/// # Arguments
/// * `secret` - The secret as a hex-encoded string
/// * `secret_hash` - The expected secret hash as a hex-encoded string
///
/// # Returns
/// * `Result<Vec<u8>>` - The decoded secret as a 32-byte vector if valid
pub fn validate_secret(secret: &str, secret_hash: &str) -> Result<Vec<u8>> {
    let secret_bytes = secret.hex_to_bytes()?;

    let hash = Sha256::digest(&secret_bytes);
    if hash.encode_hex() != secret_hash {
        bail!("Secret hash mismatch");
    }

    Ok(secret_bytes.to_vec())
}

/// Validates htlc parameters for production safety.
///
/// # Arguments
/// * `htlc_params` - HTLC params information to validate
/// * `utxos` - UTXOs to validate
/// * `network` - Bitcoin network to validate against
///
/// # Returns
/// * `Result<()>` - Ok if all parameters are valid
pub fn validate_hash_generation_params(
    htlc_params: &HTLCParams,
    utxos: &[Utxo],
    network: Network,
) -> Result<()> {
    if utxos.is_empty() {
        bail!("No UTXOs provided")
    }

    if htlc_params.amount == 0 {
        bail!("Invalid swap amount: must be greater than zero")
    }

    if htlc_params.timelock == 0 {
        bail!("Invalid HTLC timelock: must be greater than zero")
    }

    // Validate UTXOs have positive values
    for utxo in utxos {
        if utxo.value == 0 {
            bail!("UTXO has zero value")
        }
    }

    let _ = network;
    Ok(())
}
