use crate::{
    collections::{PolicyMap, PolicySet},
    common::is_asset_id_match,
    primitives::{Fee, SourceAmount},
    DefaultPolicy, PolicyError, SolverPolicyConfig,
};
use bigdecimal::BigDecimal;
use eyre::Result;
use primitives::{Asset, AssetId, AssetPair, PairDirection};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    str::FromStr,
};

/// Unified overrides structure containing all optional route-specific overrides.
///
/// All fields are optional, allowing different routes to override only the specific
/// values they need. When searching for a value, the system will search through
/// all matching routes in precedence order until it finds one where the field is `Some`.
#[derive(Debug, Clone)]
pub struct Overrides {
    /// Fee override for the route
    pub fee: Option<Fee>,
    /// Max slippage override for the route
    pub max_slippage: Option<u64>,
    /// Confirmation target override for the route
    pub confirmation_target: Option<u64>,
    /// Source amount override for the route
    pub source_amount: Option<SourceAmount>,
}

/// Isolation rules storage.
#[derive(Debug, Clone)]
pub struct IsolationRules {
    /// Source -> allowed destinations (sorted most specific first)
    pub source_to_destination: Vec<(AssetId, Vec<AssetId>)>,
    /// Destination -> allowed sources (sorted most specific first)
    pub destination_to_source: Vec<(AssetId, Vec<AssetId>)>,
}

/// Policy manager for a single solver that enforces trading rules and fee structures.
///
/// `SolverPolicy` provides comprehensive policy enforcement for asset trading, including:
/// - Asset support validation
/// - Isolation rules that restrict which assets can trade together
/// - Blacklist/whitelist pair management
/// - Fee calculation with route-specific overrides
///
/// # Policy Validation Order
///
/// When validating a trade, the policy checks are performed in this order:
/// 1. **Asset Support**: Both source and destination must be in the supported assets list
/// 2. **Isolation Rules**: If isolation rules exist for the source, destination must be allowed
/// 3. **Blacklist/Whitelist**: Pair must not be blacklisted, unless whitelisted
///
/// # Wildcards and Specificity
///
/// Rules support wildcards (`*`) for chain or token fields. When multiple rules match,
/// more specific rules (exact matches) take precedence over wildcards.
///
#[derive(Debug, Clone)]
pub struct SolverPolicy {
    /// The default policy type (e.g., "open", "closed")
    #[allow(unused)]
    default: DefaultPolicy,
    /// Isolation rules grouped by direction.
    isolation_rules: IsolationRules,
    /// Pairs that are explicitly blocked from trading.
    /// Uses PolicySet for efficient membership testing.
    blacklist_pairs: PolicySet,
    /// Pairs that override blacklist restrictions.
    /// These pairs are allowed even if they would be blocked by blacklist rules.
    whitelist_overrides: PolicySet,
    /// Default fee structure for this solver
    default_fee: Fee,
    /// Default max slippage
    default_max_slippage: u64,
    /// Default confirmation target
    default_confirmation_target: u64,
    /// Unified route-specific overrides for fees, slippage, confirmation targets, and source amounts
    overrides: PolicyMap<Overrides>,
    /// Supported assets for this solver
    supported_assets: HashSet<AssetId>,
    /// Maximum source liquidity limit for assets
    max_limits: HashMap<AssetId, BigDecimal>,
}
impl SolverPolicy {
    /// Creates a new `SolverPolicy` from a configuration.
    ///
    /// This constructor parses and validates the policy configuration, converting
    /// string-based asset pairs and routes into efficient internal data structures.
    ///
    /// # Arguments
    ///
    /// * `policy` - The solver policy configuration containing rules and fee structures
    /// * `supported_assets` - The assets this solver supports
    ///
    /// # Returns
    ///
    /// * `Ok(SolverPolicy)` - Successfully created policy
    /// * `Err(PolicyError)` - Configuration is invalid (malformed asset IDs or pairs)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Asset IDs in `supported_assets` cannot be parsed
    /// - Asset pairs in isolation_groups, blacklist_pairs, or whitelist_overrides are invalid
    /// - Route specifications in fee overrides are malformed
    pub fn new(
        policy: SolverPolicyConfig,
        supported_assets: Vec<String>,
    ) -> Result<Self, PolicyError> {
        let isolation_groups = policy.isolation_groups;
        let blacklist_pairs = policy.blacklist_pairs;
        let whitelist_overrides = policy.whitelist_overrides;
        let default_fee = policy.default_fee;
        let route_overrides = policy.overrides;

        let supported_assets = supported_assets
            .clone()
            .iter()
            .map(|asset| {
                AssetId::from_str(asset).map_err(|e| PolicyError::InvalidAssetId(asset.clone(), e))
            })
            .collect::<Result<HashSet<AssetId>, PolicyError>>()?;

        // Build isolation rules in a temporary HashMap first, pre-allocating capacity
        let mut temp_isolation_rules: HashMap<AssetId, Vec<AssetId>> =
            HashMap::with_capacity(isolation_groups.len());
        let mut temp_incoming_isolation_rules: HashMap<AssetId, Vec<AssetId>> =
            HashMap::with_capacity(isolation_groups.len());
        for group in isolation_groups.clone() {
            let (source, direction, destination) = AssetPair::from_str(&group)
                .map_err(|e| PolicyError::InvalidAssetPair(group.clone(), e))?
                .into();

            let bidirectional = matches!(direction, PairDirection::Both);

            // Add forward direction rule
            temp_isolation_rules
                .entry(source.clone())
                .or_insert(Vec::new())
                .push(destination.clone());
            temp_incoming_isolation_rules
                .entry(destination.clone())
                .or_insert(Vec::new())
                .push(source.clone());

            // Add reverse direction rule if bidirectional
            if bidirectional {
                temp_isolation_rules
                    .entry(destination.clone())
                    .or_insert(Vec::new())
                    .push(source.clone());
                temp_incoming_isolation_rules
                    .entry(source)
                    .or_insert(Vec::new())
                    .push(destination);
            }
        }

        // Convert HashMap to Vec and sort by specificity (most specific first)
        // This ensures the most specific rules are checked first during validation
        let mut source_to_destination: Vec<(AssetId, Vec<AssetId>)> =
            Vec::with_capacity(temp_isolation_rules.len());
        source_to_destination.extend(temp_isolation_rules.into_iter());
        source_to_destination.sort_by(|(a, _), (b, _)| compare_asset_specificity(a, b));

        let mut destination_to_source: Vec<(AssetId, Vec<AssetId>)> =
            Vec::with_capacity(temp_incoming_isolation_rules.len());
        destination_to_source.extend(temp_incoming_isolation_rules.into_iter());
        destination_to_source.sort_by(|(a, _), (b, _)| compare_asset_specificity(a, b));

        let mut blacklist_pairs_set = PolicySet::with_capacity(blacklist_pairs.len());
        for pair in blacklist_pairs.clone() {
            let asset_pair = AssetPair::from_str(&pair)
                .map_err(|e| PolicyError::InvalidAssetPair(pair.clone(), e))?;
            blacklist_pairs_set.insert(asset_pair);
        }

        let mut whitelist_overrides_set = PolicySet::with_capacity(whitelist_overrides.len());
        for pair in whitelist_overrides.clone() {
            let asset_pair = AssetPair::from_str(&pair)
                .map_err(|e| PolicyError::InvalidAssetPair(pair.clone(), e))?;
            whitelist_overrides_set.insert(asset_pair);
        }

        // Build unified overrides map from the route overrides
        // Convert each RouteOverride to Overrides and insert into PolicyMap
        // PolicyMap will handle any duplicates (though each route should only appear once)
        let mut unified_overrides = PolicyMap::with_capacity(route_overrides.len());
        for route_override in route_overrides {
            let asset_pair = AssetPair::from_str(&route_override.route)
                .map_err(|e| PolicyError::InvalidAssetPair(route_override.route.clone(), e))?;

            let overrides = Overrides {
                fee: route_override.fee,
                max_slippage: route_override.max_slippage,
                confirmation_target: route_override.confirmation_target,
                source_amount: route_override.source_amount,
            };

            unified_overrides.insert(asset_pair, overrides);
        }

        Ok(Self {
            default: policy.default,
            isolation_rules: IsolationRules {
                source_to_destination,
                destination_to_source,
            },
            blacklist_pairs: blacklist_pairs_set,
            whitelist_overrides: whitelist_overrides_set,
            default_fee,
            default_max_slippage: policy.default_max_slippage,
            default_confirmation_target: policy.default_confirmation_target,
            overrides: unified_overrides,
            max_limits: policy.max_limits,
            supported_assets,
        })
    }

    /// Retrieves the fee for a specific trading route.
    ///
    /// Returns the fee override if one exists for the route, otherwise returns
    /// the default fee. This method does not perform validation - use
    /// `validate_and_get_fee` if you need combined validation and fee retrieval.
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// The applicable fee for the route (either override or default)
    pub fn get_fee(&self, source: &AssetId, destination: &AssetId) -> Fee {
        self.overrides
            .find_map(source, destination, |overrides| overrides.fee.clone())
            .unwrap_or(self.default_fee.clone())
    }

    /// Returns a reference to the default fee structure.
    ///
    /// This is the base fee applied to all routes that don't have
    /// specific fee overrides configured.
    pub fn default_fee(&self) -> &Fee {
        &self.default_fee
    }

    /// Returns a reference to the set of supported assets for this solver.
    ///
    /// Only assets in this set can participate in trades handled by this solver.
    /// Both the source and destination assets must be in the supported set for
    /// a trade to be valid.
    pub fn supported_assets(&self) -> &HashSet<AssetId> {
        &self.supported_assets
    }

    /// Checks if an asset is supported by this solver.
    ///
    /// # Arguments
    ///
    /// * `asset` - The asset identifier to check
    ///
    /// # Returns
    ///
    /// `true` if the asset is supported, `false` otherwise
    pub fn is_asset_supported(&self, asset: &AssetId) -> bool {
        self.supported_assets.contains(asset)
    }

    /// Validates an asset pair and returns the applicable fee if the trade is allowed.
    ///
    /// This method combines validation and fee retrieval into a single operation,
    /// providing both policy compliance checking and fee information for valid trades.
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// * `Ok(Fee)` - The trade is allowed and the applicable fee is returned
    /// * `Err(...)` - The trade is blocked, with details about which rule was violated
    pub fn validate_and_get_fee(&self, source: &AssetId, destination: &AssetId) -> Result<Fee> {
        self.validate_asset_pair(source, destination)?;
        Ok(self.get_fee(source, destination))
    }

    /// Validates whether a trade between two assets is allowed according to policy rules.
    ///
    /// This method performs comprehensive validation by checking:
    /// 1. Asset support - ensures both source and destination assets are supported by the solver
    /// 2. Isolation rules - ensures the source asset can trade with the destination
    /// 3. Blacklist restrictions - ensures the pair is not explicitly blocked
    /// 4. Whitelist overrides - allows pairs that would otherwise be blacklisted
    ///
    /// The validation follows a precedence system where more specific rules
    /// override less specific ones, and whitelist overrides take precedence
    /// over blacklist restrictions.
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The trade is allowed according to policy rules
    /// * `Err(...)` - The trade is blocked, with details about which rule was violated
    pub fn validate_asset_pair(&self, source: &AssetId, destination: &AssetId) -> Result<()> {
        if source == destination {
            return Err(eyre::eyre!(
                "Source and destination assets must be different (got both: {})",
                source
            ));
        }
        self.are_assets_supported(source, destination)?;
        self.is_not_isolated(source, destination)?;
        self.is_not_blacklisted(source, destination)?;
        Ok(())
    }

    /// Get the max slippage for a specific trading route.
    ///
    /// Returns the max slippage override if one exists for the route, otherwise returns
    /// the default max slippage.
    pub fn get_max_slippage(&self, source: &AssetId, destination: &AssetId) -> u64 {
        self.overrides
            .find_map(source, destination, |overrides| overrides.max_slippage)
            .unwrap_or(self.default_max_slippage)
    }

    /// Gets the default max slippage.
    pub fn default_max_slippage(&self) -> u64 {
        self.default_max_slippage
    }

    /// Gets the confirmation target for a specific trading route.
    ///
    /// Returns the confirmation target override if one exists for the route, otherwise returns
    /// the default confirmation target.
    pub fn get_confirmation_target(&self, source: &AssetId, destination: &AssetId) -> u64 {
        self.overrides
            .find_map(source, destination, |overrides| {
                overrides.confirmation_target
            })
            .unwrap_or(self.default_confirmation_target)
    }

    /// Gets the default confirmation target.
    pub fn default_confirmation_target(&self) -> u64 {
        self.default_confirmation_target
    }

    /// Gets the source amount for a specific trading route.
    ///
    /// Returns the source amount override if one exists for the route, otherwise returns
    /// the default source amount.
    pub fn get_source_amount(&self, source: &Asset, destination: &Asset) -> Result<SourceAmount> {
        self.overrides
            .find_map(&source.id, &destination.id, |overrides| {
                overrides.source_amount.clone()
            })
            .map(Ok)
            .unwrap_or_else(|| self.default_source_amount(source))
    }

    /// Gets the default source amount for an asset.
    pub fn default_source_amount(&self, asset: &Asset) -> Result<SourceAmount> {
        let min_amount = BigDecimal::from_str(&asset.min_amount)
            .map_err(|e| eyre::eyre!("Invalid min amount: {}", e))?;
        let max_amount = BigDecimal::from_str(&asset.max_amount)
            .map_err(|e| eyre::eyre!("Invalid max amount: {}", e))?;
        Ok(SourceAmount {
            min: min_amount,
            max: max_amount,
        })
    }

    /// Gets the maximum source liquidity limit for an asset.
    ///
    /// Returns the maximum source liquidity limit override if one exists for the asset, otherwise returns
    /// the default maximum source liquidity limit.
    pub fn get_max_source_liquidity_limit(&self, asset: &AssetId) -> Option<BigDecimal> {
        self.max_limits.get(asset).cloned()
    }

    /// Checks if both source and destination assets are supported by this solver.
    ///
    /// This validation ensures that the solver can handle both assets involved in the trade.
    /// If either asset is not supported, the trade should be rejected.
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Both assets are supported by the solver
    /// * `Err(...)` - One or both assets are not supported, with details about which assets
    fn are_assets_supported(&self, source: &AssetId, destination: &AssetId) -> Result<()> {
        let mut unsupported_assets = Vec::new();

        if !self.is_asset_supported(source) {
            unsupported_assets.push(source.to_string());
        }

        if !self.is_asset_supported(destination) {
            unsupported_assets.push(destination.to_string());
        }

        if !unsupported_assets.is_empty() {
            return Err(eyre::eyre!(
                "Unsupported assets: [{}]. Supported assets: [{}]",
                unsupported_assets.join(", "),
                self.supported_assets
                    .iter()
                    .map(|asset| asset.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }

        Ok(())
    }

    /// Checks if a trade violates isolation rules.
    ///
    /// Isolation rules restrict which assets can trade with each other. When an asset
    /// has isolation rules defined, it can only trade with assets specified in those rules.
    ///
    /// The method implements a specificity-based precedence system:
    /// - More specific rules (exact matches) override less specific ones (wildcards)
    /// - Rules are matched using wildcard support for both chain and token fields
    /// - The first matching rule (most specific) determines the allowed destinations
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// * `Ok(())` - No isolation rules apply or the trade is allowed by isolation rules
    /// * `Err(...)` - The trade violates isolation rules, with details about allowed destinations
    fn is_not_isolated(&self, source: &AssetId, destination: &AssetId) -> Result<()> {
        // The first matching rule determines the allowed destinations
        // Outbound isolation: source can only trade to allowed destinations
        for (source_asset, allowed_destinations) in &self.isolation_rules.source_to_destination {
            if is_asset_id_match(source, source_asset) {
                for rule in allowed_destinations {
                    if is_asset_id_match(rule, destination) {
                        return Ok(()); // Trade is allowed
                    }
                }

                // Found matching isolation rule, but destination is not allowed
                return Err(eyre::eyre!(
                    "Source {} is isolated and can only trade with: [{}], but trying to trade to {}",
                    source,
                    allowed_destinations
                        .iter()
                        .map(|rule| rule.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    destination
                ));
            }
        }

        // Inbound isolation: destination can only receive from allowed sources
        for (destination_asset, allowed_sources) in &self.isolation_rules.destination_to_source {
            if is_asset_id_match(destination, destination_asset) {
                for rule in allowed_sources {
                    if is_asset_id_match(rule, source) {
                        return Ok(()); // Trade is allowed
                    }
                }

                return Err(eyre::eyre!(
                    "Destination {} is isolated and can only receive from: [{}], but trying to receive from {}",
                    destination,
                    allowed_sources
                        .iter()
                        .map(|rule| rule.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                    source
                ));
            }
        }

        // No isolation rules apply in either direction
        Ok(())
    }

    /// Checks if a trade violates blacklist restrictions.
    ///
    /// Blacklist rules explicitly block specific asset pairs from trading. However,
    /// whitelist overrides can allow pairs that would otherwise be blacklisted.
    ///
    /// The validation logic:
    /// 1. Check if the pair is in the blacklist
    /// 2. If blacklisted, check if there's a whitelist override
    /// 3. Only block the trade if it's blacklisted AND not whitelisted
    ///
    /// # Arguments
    ///
    /// * `source` - The source asset identifier
    /// * `destination` - The destination asset identifier
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The pair is not blacklisted or is whitelisted
    /// * `Err(...)` - The pair is blacklisted and not whitelisted
    fn is_not_blacklisted(&self, source: &AssetId, destination: &AssetId) -> Result<()> {
        if self.blacklist_pairs.contains(source, destination)
            && !self.whitelist_overrides.contains(source, destination)
        {
            return Err(eyre::eyre!(
                "Pair {} to {} is blacklisted",
                source,
                destination
            ));
        }
        Ok(())
    }
}

/// Compare two AssetIds for specificity ordering
/// Returns Ordering::Less if `a` is more specific than `b`
fn compare_asset_specificity(a: &AssetId, b: &AssetId) -> std::cmp::Ordering {
    let a_chain_specific = a.chain() != "*";
    let a_token_specific = a.token() != "*";
    let b_chain_specific = b.chain() != "*";
    let b_token_specific = b.token() != "*";

    // Count specificity (2 = both specific, 1 = one specific, 0 = both wildcards)
    let a_specificity = (a_chain_specific as u8) + (a_token_specific as u8);
    let b_specificity = (b_chain_specific as u8) + (b_token_specific as u8);

    match a_specificity.cmp(&b_specificity) {
        Ordering::Equal => {
            // If same level of specificity, compare lexicographically for consistency
            (a.chain(), a.token()).cmp(&(b.chain(), b.token()))
        }
        other => other.reverse(), // More specific should come first (Less)
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::RouteOverride;

    use super::*;

    /// Helper function to create a mock policy response
    fn get_mock_policy() -> SolverPolicyConfig {
        SolverPolicyConfig {
            default: DefaultPolicy::Open,
            solver_id: "test".to_string(),
            isolation_groups: vec![
                "arbitrum:seed <-> ethereum:seed".to_string(),
                "starknet:* -> bitcoin:btc".to_string(),
                "starknet:usdc -> *:usdc".to_string(),
                "starknet:wbtc -> arbitrum:wbtc".to_string(),
            ],
            blacklist_pairs: vec![
                "starknet:* -> arbitrum:*".to_string(),
                "starknet:stark <-> solana:*".to_string(),
            ],
            whitelist_overrides: vec![
                "solana:usdc -> starknet:stark".to_string(),
                "starknet:* <-> solana:wbtc".to_string(),
            ],
            default_fee: Fee {
                fixed: 1.0,
                percent_bips: 50,
            },
            default_max_slippage: 50,
            default_confirmation_target: 1,
            overrides: vec![
                RouteOverride {
                    route: "solana:sol -> ethereum:eth".to_string(),
                    fee: Some(Fee {
                        fixed: 2.0,
                        percent_bips: 100,
                    }),
                    max_slippage: Some(100),
                    confirmation_target: Some(5),
                    source_amount: Some(SourceAmount {
                        min: BigDecimal::from_str("10.0").unwrap(),
                        max: BigDecimal::from_str("1000.0").unwrap(),
                    }),
                },
                RouteOverride {
                    route: "bitcoin:btc <-> starknet:stark".to_string(),
                    fee: Some(Fee {
                        fixed: 3.0,
                        percent_bips: 150,
                    }),
                    max_slippage: Some(200),
                    confirmation_target: Some(10),
                    source_amount: None,
                },
                RouteOverride {
                    route: "ethereum:eth -> bitcoin:btc".to_string(),
                    fee: None,
                    max_slippage: Some(75),
                    confirmation_target: None,
                    source_amount: Some(SourceAmount {
                        min: BigDecimal::from_str("5.0").unwrap(),
                        max: BigDecimal::from_str("500.0").unwrap(),
                    }),
                },
            ],
            max_limits: HashMap::new(),
        }
    }

    fn get_supported_assets() -> Vec<String> {
        Vec::from([
            "solana:sol".to_string(),
            "solana:usdc".to_string(),
            "ethereum:eth".to_string(),
            "bitcoin:btc".to_string(),
            "starknet:stark".to_string(),
            "arbitrum:seed".to_string(),
        ])
    }

    /// Helper function to create a test Asset with minimal required fields
    fn create_test_asset(asset_id: AssetId, min_amount: &str, max_amount: &str) -> Asset {
        use primitives::{ChainType, ContractInfo, HTLCVersion};
        use std::collections::HashMap;

        Asset {
            id: asset_id.clone(),
            name: format!("Test {}", asset_id),
            chain: asset_id.chain().to_string(),
            icon: "".to_string(),
            htlc: ContractInfo {
                address: "primary".to_string(),
                schema: Some("primary".to_string()),
            },
            token: ContractInfo {
                address: "primary".to_string(),
                schema: Some("primary".to_string()),
            },
            decimals: 8,
            min_amount: min_amount.to_string(),
            max_amount: max_amount.to_string(),
            chain_id: None,
            chain_icon: "".to_string(),
            chain_type: ChainType::Evm,
            explorer_url: "".to_string(),
            price: None,
            version: HTLCVersion::V1,
            min_timelock: 0,
            token_ids: HashMap::new(),
            solver: "".to_string(),
            private: false,
        }
    }

    struct TestCase {
        source: &'static str,
        destination: &'static str,
        should_pass: bool,
    }

    impl TestCase {
        fn new(source: &'static str, destination: &'static str, should_pass: bool) -> Self {
            Self {
                source,
                destination,
                should_pass,
            }
        }

        /// Execute the test case against a policy validation function
        fn run<F>(&self, policy_fn: F)
        where
            F: Fn(&AssetId, &AssetId) -> Result<()>,
        {
            let source = AssetId::from_str(self.source).unwrap();
            let destination = AssetId::from_str(self.destination).unwrap();

            let result = policy_fn(&source, &destination);
            assert_eq!(result.is_ok(), self.should_pass);
        }
    }

    #[tokio::test]
    async fn test_isolation_groups() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let test_cases = [
            // Non isolated pair - should be allowed
            TestCase::new("solana:sol", "ethereum:eth", true),
            // Bidirectional isolation forward - should be allowed within isolation group
            TestCase::new("arbitrum:seed", "ethereum:seed", true),
            // Bidirectional isolation reverse - should be allowed within isolation group
            TestCase::new("ethereum:seed", "arbitrum:seed", true),
            // Isolation violation - isolated asset trying to trade outside group
            TestCase::new("arbitrum:seed", "ethereum:wbtc", false),
            // Isolation violation - isolated asset trying to trade outside group
            TestCase::new("ethereum:wbtc", "arbitrum:seed", false),
            // Isolation violation - isolated asset trying to trade outside group
            TestCase::new("ethereum:wbtc", "ethereum:seed", false),
            // Source wildcard match - starknet:* -> bitcoin:btc rule
            TestCase::new("starknet:stark", "bitcoin:btc", true),
            // Destination wildcard match - starknet:usdc -> *:usdc rule
            TestCase::new("starknet:usdc", "ethereum:usdc", true),
            // Destination wildcard mismatch - starknet:usdc can only go to *:usdc
            TestCase::new("starknet:usdc", "solana:sol", false),
            // Specific rule match - starknet:wbtc -> arbitrum:wbtc
            TestCase::new("starknet:wbtc", "arbitrum:wbtc", true),
            // Specific rule mismatch - starknet:wbtc cannot go to bitcoin:btc
            TestCase::new("starknet:wbtc", "bitcoin:btc", false),
        ];

        for test_case in &test_cases {
            test_case.run(|source, dest| policy.is_not_isolated(source, dest));
        }
    }

    #[tokio::test]
    async fn test_blacklist_pairs() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let test_cases = [
            // Blacklisted wildcard - starknet:* -> arbitrum:* rule
            TestCase::new("starknet:eth", "arbitrum:eth", false),
            // Not blacklisted - ethereum is not in the blacklist
            TestCase::new("starknet:eth", "ethereum:eth", true),
            // Bidirectional blacklist forward - starknet:stark <-> solana:* rule
            TestCase::new("starknet:stark", "solana:sol", false),
            // Bidirectional blacklist reverse - starknet:stark <-> solana:* rule
            TestCase::new("solana:sol", "starknet:stark", false),
        ];

        for test_case in &test_cases {
            test_case.run(|source, dest| policy.is_not_blacklisted(source, dest));
        }
    }

    #[tokio::test]
    async fn test_whitelist_overrides() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let test_cases = [
            // Whitelist override specific - solana:usdc -> starknet:stark override
            TestCase::new("solana:usdc", "starknet:stark", true),
            // Whitelist override bidirectional - starknet:* <-> solana:wbtc override
            TestCase::new("starknet:wbtc", "solana:wbtc", true),
            // No whitelist override - should fall back to blacklist rules
            TestCase::new("starknet:stark", "solana:sol", false),
        ];

        for test_case in &test_cases {
            test_case.run(|source, dest| policy.is_not_blacklisted(source, dest));
        }
    }

    #[tokio::test]
    async fn test_precedence_rules() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test cases specifically for precedence (more specific rules should override wildcards)
        let test_cases = [
            // Specific rule should override wildcard - starknet:usdc -> *:usdc takes precedence
            TestCase::new("starknet:usdc", "ethereum:usdc", true),
            // Specific rule violation - starknet:usdc cannot go to bitcoin:btc
            TestCase::new("starknet:usdc", "bitcoin:btc", false),
            // Wildcard when no specific rule - starknet:eth uses starknet:* -> bitcoin:btc
            TestCase::new("starknet:eth", "bitcoin:btc", true),
        ];

        for test_case in &test_cases {
            test_case.run(|source, dest| policy.is_not_isolated(source, dest));
        }
    }

    #[tokio::test]
    async fn test_supported_assets() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let test_cases = [
            // Both assets supported - should pass
            TestCase::new("solana:sol", "ethereum:eth", true),
            TestCase::new("bitcoin:btc", "starknet:stark", true),
            // Source asset not supported - should fail
            TestCase::new("polygon:matic", "ethereum:eth", false),
            // Destination asset not supported - should fail
            TestCase::new("ethereum:eth", "polygon:matic", false),
            // Both assets not supported - should fail
            TestCase::new("polygon:matic", "avalanche:avax", false),
            // Valid trade - both assets supported, no policy violations
            TestCase::new("solana:sol", "ethereum:eth", true),
            // Asset not supported - should fail before checking other policies
            TestCase::new("polygon:matic", "ethereum:eth", false),
            // Asset supported but violates isolation rules
            TestCase::new("arbitrum:seed", "ethereum:wbtc", false),
            // Asset supported but blacklisted
            TestCase::new("starknet:eth", "arbitrum:eth", false),
            // Asset supported, blacklisted, but has whitelist override
            TestCase::new("solana:usdc", "starknet:stark", true),
        ];

        for test_case in &test_cases {
            test_case.run(|source, dest| policy.are_assets_supported(source, dest));
        }
    }

    #[tokio::test]
    async fn test_default_fee() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let expected_fee = Fee {
            fixed: 1.0,
            percent_bips: 50,
        };

        assert_eq!(policy.default_fee(), &expected_fee);
    }

    #[tokio::test]
    async fn test_get_fee_with_overrides() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test cases for fee retrieval
        let test_cases = vec![
            // Route with fee override (forward direction)
            (
                "solana:sol",
                "ethereum:eth",
                Fee {
                    fixed: 2.0,
                    percent_bips: 100,
                },
            ),
            // Route with fee override (reverse direction for bidirectional)
            (
                "bitcoin:btc",
                "starknet:stark",
                Fee {
                    fixed: 3.0,
                    percent_bips: 150,
                },
            ),
            (
                "starknet:stark",
                "bitcoin:btc",
                Fee {
                    fixed: 3.0,
                    percent_bips: 150,
                },
            ),
            // Route without override - should use default fee
            (
                "ethereum:eth",
                "bitcoin:btc",
                Fee {
                    fixed: 1.0,
                    percent_bips: 50,
                },
            ),
            (
                "arbitrum:seed",
                "solana:sol",
                Fee {
                    fixed: 1.0,
                    percent_bips: 50,
                },
            ),
        ];

        for (source_str, dest_str, expected_fee) in test_cases {
            let source = AssetId::from_str(source_str).unwrap();
            let destination = AssetId::from_str(dest_str).unwrap();

            let actual_fee = policy.get_fee(&source, &destination);
            assert_eq!(
                actual_fee, expected_fee,
                "Fee mismatch for route {} -> {}",
                source_str, dest_str
            );
        }
    }

    #[tokio::test]
    async fn test_validate_and_get_fee() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test cases for combined validation and fee retrieval
        let test_cases = vec![
            // Valid trade with fee override
            (
                "solana:sol",
                "ethereum:eth",
                Some(Fee {
                    fixed: 2.0,
                    percent_bips: 100,
                }),
            ),
            // Invalid trade - violates isolation rules
            ("ethereum:eth", "bitcoin:btc", None),
            // Invalid trade - unsupported asset
            ("polygon:matic", "ethereum:eth", None),
            // Invalid trade - violates isolation rules
            ("arbitrum:seed", "ethereum:wbtc", None),
            // Invalid trade - blacklisted pair
            ("starknet:eth", "arbitrum:eth", None),
            // Valid trade - blacklisted but has whitelist override
            (
                "solana:usdc",
                "starknet:stark",
                Some(Fee {
                    fixed: 1.0,
                    percent_bips: 50,
                }),
            ),
        ];

        for (source_str, dest_str, expected_fee) in test_cases {
            let source = AssetId::from_str(source_str).unwrap();
            let destination = AssetId::from_str(dest_str).unwrap();

            let result = policy.validate_and_get_fee(&source, &destination);

            match expected_fee {
                Some(expected) => {
                    assert!(
                        result.is_ok(),
                        "Expected valid trade for {} -> {} but got error: {:?}",
                        source_str,
                        dest_str,
                        result.err()
                    );
                    let actual_fee = result.unwrap();
                    assert_eq!(
                        actual_fee, expected,
                        "Fee mismatch for route {} -> {}",
                        source_str, dest_str
                    );
                }
                None => {
                    assert!(
                        result.is_err(),
                        "Expected invalid trade for {} -> {} but got fee: {:?}",
                        source_str,
                        dest_str,
                        result.ok()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_fee_override_bidirectional() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test that bidirectional fee overrides work in both directions
        let source1 = AssetId::from_str("bitcoin:btc").unwrap();
        let dest1 = AssetId::from_str("starknet:stark").unwrap();
        let source2 = AssetId::from_str("starknet:stark").unwrap();
        let dest2 = AssetId::from_str("bitcoin:btc").unwrap();

        let fee1 = policy.get_fee(&source1, &dest1);
        let fee2 = policy.get_fee(&source2, &dest2);

        let expected_fee = Fee {
            fixed: 3.0,
            percent_bips: 150,
        };

        assert_eq!(
            fee1, expected_fee,
            "Bidirectional fee override failed for btc -> stark"
        );
        assert_eq!(
            fee2, expected_fee,
            "Bidirectional fee override failed for stark -> btc"
        );
        assert_eq!(fee1, fee2, "Bidirectional fees should be equal");
    }

    #[tokio::test]
    async fn test_get_max_slippage() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test default max slippage
        assert_eq!(policy.default_max_slippage(), 50);

        // Test cases for max slippage retrieval
        let test_cases = vec![
            // Route with max slippage override
            ("solana:sol", "ethereum:eth", 100),
            // Route with max slippage override (bidirectional)
            ("bitcoin:btc", "starknet:stark", 200),
            ("starknet:stark", "bitcoin:btc", 200),
            // Route with max slippage override (no fee override)
            ("ethereum:eth", "bitcoin:btc", 75),
            // Route without override - should use default
            ("ethereum:eth", "bitcoin:btc", 75), // This one has override
            ("arbitrum:seed", "solana:sol", 50), // This one uses default
        ];

        for (source_str, dest_str, expected_slippage) in test_cases {
            let source = AssetId::from_str(source_str).unwrap();
            let destination = AssetId::from_str(dest_str).unwrap();

            let actual_slippage = policy.get_max_slippage(&source, &destination);
            assert_eq!(
                actual_slippage, expected_slippage,
                "Max slippage mismatch for route {} -> {}",
                source_str, dest_str
            );
        }
    }

    #[tokio::test]
    async fn test_get_confirmation_target() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test default confirmation target
        assert_eq!(policy.default_confirmation_target(), 1);

        // Test cases for confirmation target retrieval
        let test_cases = vec![
            // Route with confirmation target override
            ("solana:sol", "ethereum:eth", 5),
            // Route with confirmation target override (bidirectional)
            ("bitcoin:btc", "starknet:stark", 10),
            ("starknet:stark", "bitcoin:btc", 10),
            // Route without override - should use default
            ("ethereum:eth", "bitcoin:btc", 1), // No confirmation target override
            ("arbitrum:seed", "solana:sol", 1), // Uses default
        ];

        for (source_str, dest_str, expected_target) in test_cases {
            let source = AssetId::from_str(source_str).unwrap();
            let destination = AssetId::from_str(dest_str).unwrap();

            let actual_target = policy.get_confirmation_target(&source, &destination);
            assert_eq!(
                actual_target, expected_target,
                "Confirmation target mismatch for route {} -> {}",
                source_str, dest_str
            );
        }
    }

    #[tokio::test]
    async fn test_get_source_amount() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test cases for source amount retrieval
        let test_cases = vec![
            // Route with source amount override
            (
                "solana:sol",
                "ethereum:eth",
                Some(SourceAmount {
                    min: BigDecimal::from_str("10.0").unwrap(),
                    max: BigDecimal::from_str("1000.0").unwrap(),
                }),
            ),
            // Route with source amount override (no fee override)
            (
                "ethereum:eth",
                "bitcoin:btc",
                Some(SourceAmount {
                    min: BigDecimal::from_str("5.0").unwrap(),
                    max: BigDecimal::from_str("500.0").unwrap(),
                }),
            ),
            // Route without override - should use default from asset
            ("bitcoin:btc", "starknet:stark", None), // Will use default from asset
        ];

        for (source_str, dest_str, expected_amount) in test_cases {
            let source_id = AssetId::from_str(source_str).unwrap();
            let dest_id = AssetId::from_str(dest_str).unwrap();

            // Create mock assets for testing
            let source = create_test_asset(source_id, "1.0", "10000.0");
            let destination = create_test_asset(dest_id, "1.0", "10000.0");

            let result = policy.get_source_amount(&source, &destination);

            match expected_amount {
                Some(expected) => {
                    assert!(
                        result.is_ok(),
                        "Expected valid source amount for {} -> {} but got error: {:?}",
                        source_str,
                        dest_str,
                        result.err()
                    );
                    let actual_amount = result.unwrap();
                    assert_eq!(
                        actual_amount.min, expected.min,
                        "Source amount min mismatch for route {} -> {}",
                        source_str, dest_str
                    );
                    assert_eq!(
                        actual_amount.max, expected.max,
                        "Source amount max mismatch for route {} -> {}",
                        source_str, dest_str
                    );
                }
                None => {
                    // Should fall back to default from asset
                    assert!(
                        result.is_ok(),
                        "Expected valid default source amount for {} -> {} but got error: {:?}",
                        source_str,
                        dest_str,
                        result.err()
                    );
                    let actual_amount = result.unwrap();
                    // Should match asset defaults
                    assert_eq!(
                        actual_amount.min,
                        BigDecimal::from_str(&source.min_amount).unwrap(),
                        "Source amount min should match asset default for route {} -> {}",
                        source_str,
                        dest_str
                    );
                    assert_eq!(
                        actual_amount.max,
                        BigDecimal::from_str(&source.max_amount).unwrap(),
                        "Source amount max should match asset default for route {} -> {}",
                        source_str,
                        dest_str
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_default_source_amount() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        let asset = create_test_asset(AssetId::from_str("ethereum:eth").unwrap(), "0.1", "100.0");

        let result = policy.default_source_amount(&asset).unwrap();
        assert_eq!(result.min, BigDecimal::from_str("0.1").unwrap());
        assert_eq!(result.max, BigDecimal::from_str("100.0").unwrap());
    }

    #[tokio::test]
    async fn test_get_max_source_liquidity_limit() {
        use std::str::FromStr;

        let mut policy_config = get_mock_policy();
        // Add max limits to the policy
        policy_config.max_limits.insert(
            AssetId::from_str("ethereum:eth").unwrap(),
            BigDecimal::from_str("5000.0").unwrap(),
        );
        policy_config.max_limits.insert(
            AssetId::from_str("bitcoin:btc").unwrap(),
            BigDecimal::from_str("10000.0").unwrap(),
        );

        let policy = SolverPolicy::new(policy_config, get_supported_assets()).unwrap();

        // Test cases for max source liquidity limit
        let test_cases = vec![
            // Asset with max limit
            (
                "ethereum:eth",
                Some(BigDecimal::from_str("5000.0").unwrap()),
            ),
            (
                "bitcoin:btc",
                Some(BigDecimal::from_str("10000.0").unwrap()),
            ),
            // Asset without max limit
            ("solana:sol", None),
            ("arbitrum:seed", None),
        ];

        for (asset_str, expected_limit) in test_cases {
            let asset_id = AssetId::from_str(asset_str).unwrap();
            let actual_limit = policy.get_max_source_liquidity_limit(&asset_id);

            match expected_limit {
                Some(expected) => {
                    assert_eq!(
                        actual_limit,
                        Some(expected),
                        "Max source liquidity limit mismatch for asset {}",
                        asset_str
                    );
                }
                None => {
                    assert_eq!(
                        actual_limit, None,
                        "Expected no max limit for asset {}",
                        asset_str
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_max_slippage_override_bidirectional() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test that bidirectional max slippage overrides work in both directions
        let source1 = AssetId::from_str("bitcoin:btc").unwrap();
        let dest1 = AssetId::from_str("starknet:stark").unwrap();
        let source2 = AssetId::from_str("starknet:stark").unwrap();
        let dest2 = AssetId::from_str("bitcoin:btc").unwrap();

        let slippage1 = policy.get_max_slippage(&source1, &dest1);
        let slippage2 = policy.get_max_slippage(&source2, &dest2);

        assert_eq!(
            slippage1, 200,
            "Bidirectional max slippage override failed for btc -> stark"
        );
        assert_eq!(
            slippage2, 200,
            "Bidirectional max slippage override failed for stark -> btc"
        );
        assert_eq!(
            slippage1, slippage2,
            "Bidirectional max slippage should be equal"
        );
    }

    #[tokio::test]
    async fn test_confirmation_target_override_bidirectional() {
        let policy = SolverPolicy::new(get_mock_policy(), get_supported_assets()).unwrap();

        // Test that bidirectional confirmation target overrides work in both directions
        let source1 = AssetId::from_str("bitcoin:btc").unwrap();
        let dest1 = AssetId::from_str("starknet:stark").unwrap();
        let source2 = AssetId::from_str("starknet:stark").unwrap();
        let dest2 = AssetId::from_str("bitcoin:btc").unwrap();

        let target1 = policy.get_confirmation_target(&source1, &dest1);
        let target2 = policy.get_confirmation_target(&source2, &dest2);

        assert_eq!(
            target1, 10,
            "Bidirectional confirmation target override failed for btc -> stark"
        );
        assert_eq!(
            target2, 10,
            "Bidirectional confirmation target override failed for stark -> btc"
        );
        assert_eq!(
            target1, target2,
            "Bidirectional confirmation targets should be equal"
        );
    }
}
