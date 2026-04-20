use super::{
    primitives::{HTLCLeaf, HTLCParams},
    script::{instant_refund_leaf, redeem_leaf, refund_leaf},
};
use alloy::hex;
use bitcoin::{
    key::{Secp256k1, XOnlyPublicKey},
    secp256k1::PublicKey,
    taproot::{ControlBlock, LeafVersion, TaprootBuilder, TaprootSpendInfo},
    Address, KnownHrp, Network, ScriptBuf,
};
use eyre::{eyre, Result};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};

pub static GARDEN_NUMS: Lazy<XOnlyPublicKey> = Lazy::new(|| {
    let r = Sha256::digest(b"GardenHTLC");
    const H_HEX: &str = "0250929b74c1a04954b78b4b6035e97a5e078a5a0f28ec96d547bfee9ace803ac0";
    let h_bytes = hex::decode(H_HEX).expect("Invalid hex in GARDEN_NUMS_KEY");
    let h = PublicKey::from_slice(&h_bytes).expect("Invalid H point in GARDEN_NUMS_KEY");

    let secp = Secp256k1::new();
    let r_scalar =
        bitcoin::secp256k1::SecretKey::from_slice(&r).expect("Invalid scalar in GARDEN_NUMS_KEY");
    let r_g = PublicKey::from_secret_key(&secp, &r_scalar);

    let nums = h
        .combine(&r_g)
        .expect("Point addition failed in GARDEN_NUMS_KEY");
    let (xonly, _) = nums.x_only_public_key();
    xonly
});

const REDEEM_LEAF_WEIGHT: u8 = 1;
const OTHER_LEAF_WEIGHT: u8 = 2;

pub fn get_htlc_address(htlc_params: &HTLCParams, network: Network) -> Result<Address> {
    let secp = Secp256k1::new();
    let internal_key = *GARDEN_NUMS;
    let taproot_spend_info = construct_taproot_spend_info(htlc_params)?;

    Ok(Address::p2tr(
        &secp,
        internal_key,
        taproot_spend_info.merkle_root(),
        KnownHrp::from(network),
    ))
}

pub fn get_htlc_leaf_script(htlc_params: &HTLCParams, leaf: HTLCLeaf) -> ScriptBuf {
    match leaf {
        HTLCLeaf::Redeem => redeem_leaf(&htlc_params.secret_hash, &htlc_params.redeemer_pubkey),
        HTLCLeaf::Refund => refund_leaf(htlc_params.timelock, &htlc_params.initiator_pubkey),
        HTLCLeaf::InstantRefund => {
            instant_refund_leaf(&htlc_params.initiator_pubkey, &htlc_params.redeemer_pubkey)
        }
    }
}

pub fn get_control_block(htlc_params: &HTLCParams, leaf: HTLCLeaf) -> Result<ControlBlock> {
    let spend_info = construct_taproot_spend_info(htlc_params)?;
    let script = get_htlc_leaf_script(htlc_params, leaf);

    spend_info
        .control_block(&(script, LeafVersion::TapScript))
        .ok_or_else(|| eyre!("Failed to get control block for '{:?}'", leaf))
}

pub fn construct_taproot_spend_info(htlc_params: &HTLCParams) -> Result<TaprootSpendInfo> {
    let redeem_leaf = redeem_leaf(&htlc_params.secret_hash, &htlc_params.redeemer_pubkey);
    let refund_leaf = refund_leaf(htlc_params.timelock, &htlc_params.initiator_pubkey);
    let instant_refund_leaf =
        instant_refund_leaf(&htlc_params.initiator_pubkey, &htlc_params.redeemer_pubkey);

    let secp = Secp256k1::new();
    let mut taproot_builder = TaprootBuilder::new();
    taproot_builder = taproot_builder
        .add_leaf(REDEEM_LEAF_WEIGHT, redeem_leaf)
        .map_err(|e| eyre!("Unable to add redeem leaf to Taproot tree: {e}"))?
        .add_leaf(OTHER_LEAF_WEIGHT, refund_leaf)
        .map_err(|e| eyre!("Unable to add refund leaf to Taproot tree: {e}"))?
        .add_leaf(OTHER_LEAF_WEIGHT, instant_refund_leaf)
        .map_err(|e| eyre!("Unable to add instant refund leaf to Taproot tree: {e}"))?;

    if !taproot_builder.is_finalizable() {
        return Err(eyre!("Taproot builder is not in a finalizable state"));
    }

    taproot_builder
        .finalize(&secp, *GARDEN_NUMS)
        .map_err(|_| eyre!("Failed to finalize Taproot spend info"))
}
