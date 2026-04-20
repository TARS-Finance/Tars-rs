use bitcoin::{opcodes, script::Builder, secp256k1::XOnlyPublicKey, ScriptBuf};

/// Creates a Bitcoin script that allows spending with a secret preimage and redeemer's signature.
///
/// # Arguments
/// * `secret_hash` - SHA256 hash of the secret (32 bytes)
/// * `redeemer_pubkey` - Public key of the redeemer
///
/// # Returns
/// A script that verifies the preimage hash and checks the redeemer's signature
pub fn redeem_leaf(secret_hash: &[u8; 32], redeemer_pubkey: &XOnlyPublicKey) -> ScriptBuf {
    Builder::new()
        .push_opcode(opcodes::all::OP_SHA256)
        .push_slice(secret_hash)
        .push_opcode(opcodes::all::OP_EQUALVERIFY)
        .push_slice(redeemer_pubkey.serialize())
        .push_opcode(opcodes::all::OP_CHECKSIG)
        .into_script()
}

/// Creates a Bitcoin script that allows refunding after a timelock expires.
///
/// # Arguments
/// * `timelock` - Number of blocks to lock the funds
/// * `initiator_pubkey` - Public key of the initiator who can claim the refund
///
/// # Returns
/// A script that enforces the timelock and verifies the initiator's signature
pub fn refund_leaf(timelock: u64, initiator_pubkey: &XOnlyPublicKey) -> ScriptBuf {
    Builder::new()
        .push_int(timelock as i64)
        .push_opcode(opcodes::all::OP_CSV)
        .push_opcode(opcodes::all::OP_DROP)
        .push_slice(&initiator_pubkey.serialize())
        .push_opcode(opcodes::all::OP_CHECKSIG)
        .into_script()
}

/// Creates a Bitcoin script that requires both initiator and redeemer signatures for instant refund.
///
/// # Arguments
/// * `initiator_pubkey` - Public key of the initiator
/// * `redeemer_pubkey` - Public key of the redeemer
///
/// # Returns
/// A script that enforces both parties must sign to execute the refund
pub fn instant_refund_leaf(
    initiator_pubkey: &XOnlyPublicKey,
    redeemer_pubkey: &XOnlyPublicKey,
) -> ScriptBuf {
    Builder::new()
        .push_slice(&initiator_pubkey.serialize())
        .push_opcode(opcodes::all::OP_CHECKSIG)
        .push_slice(&redeemer_pubkey.serialize())
        .push_opcode(opcodes::all::OP_CHECKSIGADD)
        .push_int(2)
        .push_opcode(opcodes::all::OP_NUMEQUAL)
        .into_script()
}
