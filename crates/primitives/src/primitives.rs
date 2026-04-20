use crate::ChainType;
use alloy::hex;
use alloy::primitives::Bytes;
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssetId {
    chain: String,
    token: String,
}

impl AssetId {
    pub fn new(chain: impl Into<String>, token: impl Into<String>) -> Self {
        AssetId {
            chain: chain.into(),
            token: token.into(),
        }
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.chain, self.token)
    }
}

impl FromStr for AssetId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid asset ID format: '{}'. Expected 'chain:token'",
                s
            ));
        }

        let chain = parts[0].trim();
        let token = parts[1].trim();

        if chain.is_empty() || token.is_empty() {
            return Err("Invalid asset ID: chain and token cannot be empty".to_string());
        }

        Ok(AssetId {
            chain: chain.to_string(),
            token: token.to_string(),
        })
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl From<AssetId> for String {
    fn from(asset_id: AssetId) -> Self {
        asset_id.to_string()
    }
}

impl From<&AssetId> for String {
    fn from(asset_id: &AssetId) -> Self {
        asset_id.to_string()
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum HTLCAction {
    Initiate,
    InitiateWithSignature,
    InitiateWithUserSignature { signature: Bytes },
    Redeem { secret: Bytes },
    Refund,
    InstantRefund,
    NoOp,
}

#[derive(Serialize, Deserialize)]
struct HTLCActionSerialized {
    method: String,
    params: Vec<String>,
}

impl HTLCAction {
    fn to_serialized(&self) -> HTLCActionSerialized {
        let params = match self {
            HTLCAction::InitiateWithUserSignature { signature } => {
                vec![hex::encode(signature.as_ref())]
            }
            HTLCAction::Redeem { secret } => vec![hex::encode(secret.as_ref())],
            _ => vec![],
        };

        HTLCActionSerialized {
            method: self.to_string(),
            params,
        }
    }

    fn from_serialized(serialized: HTLCActionSerialized) -> Result<Self, String> {
        match serialized.method.as_str() {
            "Initiate" => Ok(HTLCAction::Initiate),
            "InitiateWithSignature" => Ok(HTLCAction::InitiateWithSignature),
            "InitiateWithUserSignature" => {
                if serialized.params.len() != 1 {
                    return Err(format!(
                        "InitiateWithUserSignature expects 1 param, got {}",
                        serialized.params.len()
                    ));
                }
                let bytes = hex::decode(&serialized.params[0])
                    .map_err(|e| format!("Invalid hex data: {}", e))?;
                Ok(HTLCAction::InitiateWithUserSignature {
                    signature: Bytes::from(bytes),
                })
            }
            "Redeem" => {
                if serialized.params.len() != 1 {
                    return Err(format!("Redeem expects 1 param, got {}", serialized.params.len()));
                }
                let bytes = hex::decode(&serialized.params[0])
                    .map_err(|e| format!("Invalid hex data: {}", e))?;
                Ok(HTLCAction::Redeem {
                    secret: Bytes::from(bytes),
                })
            }
            "Refund" => Ok(HTLCAction::Refund),
            "InstantRefund" => Ok(HTLCAction::InstantRefund),
            "NoOp" => Ok(HTLCAction::NoOp),
            _ => Err(format!(
                "Unknown action: '{}'. Expected one of: Initiate, InitiateWithSignature, InitiateWithUserSignature, Redeem, Refund, InstantRefund, NoOp",
                serialized.method
            )),
        }
    }
}

impl Serialize for HTLCAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_serialized().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HTLCAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = HTLCActionSerialized::deserialize(deserializer)?;
        HTLCAction::from_serialized(serialized).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for HTLCAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HTLCAction::Initiate => write!(f, "Initiate"),
            HTLCAction::InitiateWithSignature => write!(f, "InitiateWithSignature"),
            HTLCAction::InitiateWithUserSignature { .. } => write!(f, "InitiateWithUserSignature"),
            HTLCAction::Redeem { .. } => write!(f, "Redeem"),
            HTLCAction::Refund => write!(f, "Refund"),
            HTLCAction::InstantRefund => write!(f, "InstantRefund"),
            HTLCAction::NoOp => write!(f, "NoOp"),
        }
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SwapSide {
    Source,
    Destination,
}

impl fmt::Display for SwapSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwapSide::Source => write!(f, "Source"),
            SwapSide::Destination => write!(f, "Destination"),
        }
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ChainAction {
    pub id: String,
    pub side: SwapSide,
    #[serde(flatten)]
    pub htlc_action: HTLCAction,
}

impl fmt::Display for ChainAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.side, self.htlc_action)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(untagged)]
pub enum ExecuteActionRequest {
    Initiate { signature: String },
    Redeem { secret: String },
    InstantRefund { signatures: Vec<String> },
    Refund { recipient: String },
}

#[derive(Debug, Clone)]
pub struct HTLCActionRequest {
    pub action: ExecuteActionRequest,
    pub headers: HeaderMap,
}

impl fmt::Display for ExecuteActionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecuteActionRequest::Initiate { .. } => write!(f, "initiate"),
            ExecuteActionRequest::Redeem { .. } => write!(f, "redeem"),
            ExecuteActionRequest::InstantRefund { .. } => write!(f, "instant-refund"),
            ExecuteActionRequest::Refund { .. } => write!(f, "refund"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash, PartialOrd, Ord, Default)]
#[serde(rename_all = "lowercase")]
pub enum HTLCVersion {
    #[default]
    V1,
    V2,
    V3,
}

impl HTLCVersion {
    pub fn as_str(&self) -> &'static str {
        match self {
            HTLCVersion::V1 => "v1",
            HTLCVersion::V2 => "v2",
            HTLCVersion::V3 => "v3",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "v1" => Some(HTLCVersion::V1),
            "v2" => Some(HTLCVersion::V2),
            "v3" => Some(HTLCVersion::V3),
            _ => None,
        }
    }

    pub fn all() -> impl Iterator<Item = Self> {
        [HTLCVersion::V1, HTLCVersion::V2, HTLCVersion::V3].into_iter()
    }
}

fn default_gas_threshold_usd() -> u64 {
    0
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ChainDeadlineConfig {
    pub init_confirmation_deadline: u64,
    pub init_detection_deadline: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Chain {
    pub chain: String,
    pub name: Option<String>,
    pub id: String,
    pub native_asset_id: AssetId,
    #[serde(default = "default_gas_threshold_usd")]
    #[serde(skip_serializing)]
    pub gas_threshold_usd: u64,
    #[serde(skip_serializing)]
    pub native_token_ids: HashMap<FiatProvider, String>,
    pub icon: String,
    pub explorer_url: String,
    pub confirmation_target: u64,
    pub source_timelock: String,
    pub destination_timelock: String,
    pub supported_htlc_schemas: Vec<String>,
    pub supported_token_schemas: Vec<String>,
    pub assets: Vec<Asset>,
    pub deadline_config: ChainDeadlineConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractInfo {
    pub address: String,
    pub schema: Option<String>,
}

impl ContractInfo {
    pub fn is_primary(&self) -> bool {
        self.address == "primary"
            && (self.schema.is_none() || self.schema.as_ref() == Some(&"primary".to_string()))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum FiatProvider {
    #[serde(rename = "aggregate")]
    Aggregate,
    #[serde(rename = "coingecko")]
    Coingecko,
    #[serde(rename = "cmc")]
    Cmc,
}

impl fmt::Display for FiatProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FiatProvider::Aggregate => write!(f, "aggregate"),
            FiatProvider::Coingecko => write!(f, "coingecko"),
            FiatProvider::Cmc => write!(f, "cmc"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Asset {
    pub id: AssetId,
    pub name: String,
    pub chain: String,
    pub icon: String,
    pub htlc: ContractInfo,
    pub token: ContractInfo,
    pub decimals: u8,
    pub min_amount: String,
    pub max_amount: String,
    pub chain_id: Option<String>,
    pub chain_icon: String,
    pub chain_type: ChainType,
    pub explorer_url: String,
    pub price: Option<f64>,
    pub version: HTLCVersion,
    pub min_timelock: u64,
    pub token_ids: HashMap<FiatProvider, String>,
    pub solver: String,
    #[serde(default)]
    pub private: bool,
}

impl Asset {
    pub fn serialize_chain(&self) -> String {
        match &self.chain_id {
            Some(chain_id) => format!("{}:{}", self.chain_type, chain_id),
            None => self.chain_type.to_string(),
        }
    }
}

impl serde::Serialize for Asset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Asset", 9)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("chain", &self.serialize_chain())?;
        state.serialize_field("icon", &self.icon)?;

        if self.htlc.is_primary() {
            state.serialize_field("htlc", &Option::<ContractInfo>::None)?;
        } else {
            state.serialize_field("htlc", &self.htlc)?;
        }

        if self.token.is_primary() {
            state.serialize_field("token", &Option::<ContractInfo>::None)?;
        } else {
            state.serialize_field("token", &self.token)?;
        }

        state.serialize_field("decimals", &self.decimals)?;
        state.serialize_field("min_amount", &self.min_amount)?;
        state.serialize_field("max_amount", &self.max_amount)?;
        state.serialize_field("price", &self.price)?;
        state.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairDirection {
    Forward,
    Both,
}

impl fmt::Display for PairDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PairDirection::Forward => write!(f, "->"),
            PairDirection::Both => write!(f, "<->"),
        }
    }
}

impl FromStr for PairDirection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "->" => Ok(PairDirection::Forward),
            "<->" => Ok(PairDirection::Both),
            _ => Err(format!(
                "Invalid pair direction: '{}'. Expected '->' or '<->'",
                s
            )),
        }
    }
}

impl Serialize for PairDirection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PairDirection::Forward => serializer.serialize_str("->"),
            PairDirection::Both => serializer.serialize_str("<->"),
        }
    }
}

impl<'de> Deserialize<'de> for PairDirection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for AssetPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetPair(pub AssetId, pub PairDirection, pub AssetId);

impl FromStr for AssetPair {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.len() != 3 {
            return Err(format!(
                "Invalid asset pair format: '{}'. Expected 'from_asset <direction> to_asset'",
                s
            ));
        }
        let from_ident = AssetId::from_str(parts[0])?;
        let direction = parts[1].parse::<PairDirection>()?;
        let to_ident = AssetId::from_str(parts[2])?;

        Ok(AssetPair(from_ident, direction, to_ident))
    }
}

impl Serialize for AssetPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for AssetPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl From<AssetPair> for String {
    fn from(asset_pair: AssetPair) -> Self {
        asset_pair.to_string()
    }
}

impl From<AssetPair> for (AssetId, PairDirection, AssetId) {
    fn from(asset_pair: AssetPair) -> Self {
        (asset_pair.0, asset_pair.1, asset_pair.2)
    }
}
