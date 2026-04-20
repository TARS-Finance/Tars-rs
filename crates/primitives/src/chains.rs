use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

pub const SPARK: &str = "spark";
pub const SPARK_REGTEST: &str = "spark_regtest";

pub const LIGHTNING: &str = "lightning";
pub const LIGHTNING_REGTEST: &str = "lightning_regtest";

pub const STARKNET: &str = "starknet";
pub const STARKNET_SEPOLIA: &str = "starknet_sepolia";
pub const STARKNET_DEVNET: &str = "starknet_devnet";

pub const SOLANA: &str = "solana";
pub const SOLANA_TESTNET: &str = "solana_testnet";
pub const SOLANA_LOCALNET: &str = "solana_localnet";

pub const BITCOIN: &str = "bitcoin";
pub const BITCOIN_TESTNET: &str = "bitcoin_testnet";
pub const BITCOIN_REGTEST: &str = "bitcoin_regtest";

pub const SUI: &str = "sui";
pub const SUI_TESTNET: &str = "sui_testnet";
pub const SUI_LOCALNET: &str = "sui_localnet";

pub const TRON: &str = "tron";
pub const TRON_TESTNET: &str = "tron_shasta";

pub const ZCASH: &str = "zcash";
pub const ZCASH_TESTNET: &str = "zcash_testnet";
pub const ZCASH_REGTEST: &str = "zcash_regtest";

pub const ALPEN_REGTEST: &str = "alpen_regtest";
pub const ALPEN_SIGNET: &str = "alpen_signet";
pub const ALPEN: &str = "alpen";

pub const LITECOIN: &str = "litecoin";
pub const LITECOIN_TESTNET: &str = "litecoin_testnet";
pub const LITECOIN_REGTEST: &str = "litecoin_regtest";

pub const XRPL: &str = "xrpl";
pub const XRPL_TESTNET: &str = "xrpl_testnet";
pub const XRPL_REGTEST: &str = "xrpl_regtest";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainType {
    Tron,
    Starknet,
    XRPL,
    Evm,
    Solana,
    Bitcoin,
    Sui,
    Zcash,
    Alpen,
    Litecoin,
    Spark,
    Lightning,
}

impl From<&str> for ChainType {
    fn from(chain: &str) -> Self {
        match chain {
            STARKNET | STARKNET_SEPOLIA | STARKNET_DEVNET => Self::Starknet,
            SOLANA | SOLANA_TESTNET | SOLANA_LOCALNET => Self::Solana,
            BITCOIN | BITCOIN_TESTNET | BITCOIN_REGTEST => Self::Bitcoin,
            SUI | SUI_TESTNET | SUI_LOCALNET => Self::Sui,
            TRON | TRON_TESTNET => Self::Tron,
            ZCASH | ZCASH_TESTNET | ZCASH_REGTEST => Self::Zcash,
            ALPEN | ALPEN_SIGNET | ALPEN_REGTEST => Self::Alpen,
            LITECOIN | LITECOIN_TESTNET | LITECOIN_REGTEST => Self::Litecoin,
            XRPL | XRPL_TESTNET | XRPL_REGTEST => Self::XRPL,
            SPARK | SPARK_REGTEST => Self::Spark,
            LIGHTNING | LIGHTNING_REGTEST => Self::Lightning,
            _ => Self::Evm,
        }
    }
}

impl From<String> for ChainType {
    fn from(chain: String) -> Self {
        Self::from(chain.to_ascii_lowercase().as_str())
    }
}

impl Display for ChainType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ChainType::Evm => "evm",
            ChainType::Bitcoin => "bitcoin",
            ChainType::Solana => "solana",
            ChainType::Starknet => "starknet",
            ChainType::Sui => "sui",
            ChainType::Tron => "tron",
            ChainType::Zcash => "zcash",
            ChainType::Alpen => "alpen",
            ChainType::Litecoin => "litecoin",
            ChainType::XRPL => "xrpl",
            ChainType::Spark => "spark",
            ChainType::Lightning => "lightning",
        };
        write!(f, "{s}")
    }
}
