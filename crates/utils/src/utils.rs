use alloy::{
    hex::FromHex,
    primitives::{Bytes, FixedBytes},
};
use serde::{
    de::{self},
    Deserialize, Deserializer,
};
use std::{collections::HashSet, fmt, str::FromStr};

pub trait ToBytes {
    fn hex_to_bytes(&self) -> eyre::Result<Bytes>;
    fn hex_to_fixed_bytes(&self) -> eyre::Result<FixedBytes<32>>;
}

impl ToBytes for &str {
    fn hex_to_bytes(&self) -> eyre::Result<Bytes> {
        let s = self.strip_prefix("0x").unwrap_or(self);
        Bytes::from_hex(s).map_err(|e| eyre::eyre!("Failed to convert hex to bytes: {}", e))
    }

    fn hex_to_fixed_bytes(&self) -> eyre::Result<FixedBytes<32>> {
        let s = self.strip_prefix("0x").unwrap_or(self);
        FixedBytes::<32>::from_hex(s)
            .map_err(|e| eyre::eyre!("Failed to convert hex to FixedBytes<{}>: {}", 32, e))
    }
}

impl ToBytes for String {
    fn hex_to_bytes(&self) -> eyre::Result<Bytes> {
        self.as_str().hex_to_bytes()
    }

    fn hex_to_fixed_bytes(&self) -> eyre::Result<FixedBytes<32>> {
        self.as_str().hex_to_fixed_bytes()
    }
}

pub fn deserialize_csv_field<'de, D, T>(deserializer: D) -> Result<Option<HashSet<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr + Eq + std::hash::Hash + Deserialize<'de>,
    <T as FromStr>::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(deserializer)?;

    match opt {
        Some(s) => {
            let items = s
                .split(',')
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.parse::<T>().map_err(de::Error::custom))
                .collect::<Result<HashSet<_>, _>>()?;
            Ok(Some(items))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::deserialize_csv_field;
    use serde::Deserialize;
    use std::collections::HashSet;

    #[derive(Debug, Deserialize)]
    struct CsvHolder {
        #[serde(deserialize_with = "deserialize_csv_field")]
        values: Option<HashSet<String>>,
    }

    #[test]
    fn test_deserialize_csv_field() {
        let parsed: CsvHolder = serde_json::from_str(r#"{"values":"btc,eth, sol"}"#).unwrap();
        let values = parsed.values.unwrap();
        assert!(values.contains("btc"));
        assert!(values.contains("eth"));
        assert!(values.contains("sol"));
    }
}
