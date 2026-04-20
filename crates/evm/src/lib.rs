pub mod errors;
pub mod events;
pub mod executor;
pub mod htlc;
pub mod multicall;
pub mod primitives;
pub mod relay;
pub mod traits;
pub mod tx_handler;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use alloy::{
    network::EthereumWallet, providers::ProviderBuilder, sol, transports::http::reqwest::Url,
};
use primitives::AlloyProvider;

sol!(
    #[sol(rpc)]
    GardenHTLC,
    "abi/htlc.json",
);

sol!(
    #[sol(rpc)]
    GardenHTLCv2,
    "abi/htlcv2.json",
);

sol!(
    #[sol(rpc)]
    GardenHTLCv3,
    "abi/htlcv3.json",
);

sol!(
    #[sol(rpc)]
    NativeHTLC,
    "abi/htlc.json",
);

sol!(
    #[sol(rpc)]
    NativeHTLCv2,
    "abi/htlcv2.json",
);

sol!(
    #[sol(rpc)]
    NativeHTLCv3,
    "abi/htlcv3.json",
);

sol!(
    #[sol(rpc)]
    ERC20,
    "abi/erc20.json",
);

sol! {
    #[sol(rpc)]
    Multicall3,
    "abi/multicall.json",
}

sol! {
    #[sol(rpc)]
    Distributor,
    "abi/distributor.json",
}

sol! {
    #[sol(rpc)]
    contract Orderbook {
        event Created(bytes data);
        event Filled(bytes data);
        function createOrder(bytes calldata data) public {
            emit Created(data);
        }
        function fillOrder(bytes calldata data) public {
            emit Filled(data);
        }
    }
}

pub type GardenHTLCContract = GardenHTLC::GardenHTLCInstance<AlloyProvider>;
pub type GardenHTLCv2Contract = GardenHTLCv2::GardenHTLCv2Instance<AlloyProvider>;
pub type GardenHTLCv3Contract = GardenHTLCv3::GardenHTLCv3Instance<AlloyProvider>;
pub type ERC20Contract = ERC20::ERC20Instance<AlloyProvider>;
pub type Multicall3Contract = Multicall3::Multicall3Instance<AlloyProvider>;
pub type OrderbookContract = Orderbook::OrderbookInstance<AlloyProvider>;
pub type NativeHTLCContract = NativeHTLC::NativeHTLCInstance<AlloyProvider>;
pub type NativeHTLCv2Contract = NativeHTLCv2::NativeHTLCv2Instance<AlloyProvider>;
pub type NativeHTLCv3Contract = NativeHTLCv3::NativeHTLCv3Instance<AlloyProvider>;
pub type DistributorContract = Distributor::DistributorInstance<AlloyProvider>;

pub fn get_provider(wallet: EthereumWallet, url: Url) -> AlloyProvider {
    ProviderBuilder::new()
        .disable_recommended_fillers()
        .with_gas_estimation()
        .with_simple_nonce_management()
        .fetch_chain_id()
        .wallet(wallet)
        .connect_http(url)
}
