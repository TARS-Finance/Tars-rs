//! Low level DB calls to get order details
use super::primitives;
use crate::{
    errors::OrderbookError,
    primitives::{
        Claim, MatchedOrderVerbose, OrderQueryFilters, OrderStatusVerbose, PaginatedData,
        SingleSwap, StatsQueryFilters, SwapChain,
    },
    traits::Orderbook,
    DbPool, Pool,
};
use async_trait::async_trait;
use bigdecimal::num_bigint::{BigInt, ToBigInt};
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};
use eyre::Result;
use serde_json::json;
use sqlx::{Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio::time::timeout;

#[derive(Clone)]
// The `Orderbook` trait implementation. Provides various orderbook queries direct from db.
pub struct OrderbookProvider {
    pub pool: Pool,
    pub sqlx_pool: sqlx::Pool<Postgres>,
}

impl OrderbookProvider {
    pub fn new(pool: Pool, sqlx_pool: sqlx::Pool<Postgres>) -> Self {
        OrderbookProvider { pool, sqlx_pool }
    }

    pub async fn from_db_url(db_url: &str) -> Result<Self> {
        let pool = DbPool::new(db_url, 150)?;
        let sqlx_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(150)
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(30))
            .idle_timeout(Some(Duration::from_secs(10 * 60)))
            .max_lifetime(Some(Duration::from_secs(30 * 60)))
            .connect(db_url)
            .await?;
        Ok(Self::new(pool, sqlx_pool))
    }
}

#[async_trait]
impl Orderbook for OrderbookProvider {
    /// Returns swap associated with given order_id and chain
    async fn get_swap(
        &self,
        order_id: &str,
        chain: SwapChain,
    ) -> Result<Option<primitives::SingleSwap>, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        let statement = match chain {
            SwapChain::Source =>  "SELECT * FROM swaps JOIN matched_orders mo on swaps.swap_id = mo.source_swap_id WHERE mo.create_order_id = $1",
            SwapChain::Destination => "SELECT * FROM swaps JOIN matched_orders mo on swaps.swap_id = mo.destination_swap_id WHERE mo.create_order_id = $1",
        };

        let swap = sqlx::query_as::<_, SingleSwap>(&statement)
            .bind(order_id)
            .fetch_optional(conn)
            .await?;

        Ok(swap)
    }

    async fn get_solver_committed_funds(
        &self,
        addr: &str,
        chain: &str,
        asset: &str,
    ) -> Result<BigDecimal, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        // lowercase the address
        let addr = addr.to_lowercase();
        let asset = asset.to_lowercase();
        let current_time = Utc::now().timestamp();
        let destination_swap_stm = "SELECT coalesce(SUM(s2.amount),0) as total_locked_amount FROM matched_orders mo
                           JOIN create_orders co ON mo.create_order_id = co.create_id
                           JOIN swaps s1 ON mo.source_swap_id = s1.swap_id
                           JOIN swaps s2 ON mo.destination_swap_id = s2.swap_id
                           WHERE (co.additional_data->>'deadline')::integer > $1 AND s2.chain = $2 AND s2.initiate_block_number = 0 AND s1.initiate_tx_hash != '' AND lower(s2.initiator) = $3 AND lower(s2.asset) = $4";

        let amount = sqlx::query_scalar::<_, BigDecimal>(destination_swap_stm)
            .bind(current_time)
            .bind(chain)
            .bind(&addr)
            .bind(&asset)
            .fetch_optional(conn)
            .await?
            .unwrap_or(BigDecimal::from(0));

        Ok(amount)
    }

    async fn exists(&self, secret_hash: &str) -> Result<bool, OrderbookError> {
        let conn = &mut *self.pool.get().await?;
        const EXISTS_QUERY: &str =
            "SELECT EXISTS(SELECT 1 FROM create_orders WHERE secret_hash = $1)";
        let exists = sqlx::query_scalar::<_, bool>(EXISTS_QUERY)
            .bind(secret_hash)
            .fetch_one(conn)
            .await?;

        Ok(exists)
    }

    async fn get_all_matched_orders(
        &self,
        filters: OrderQueryFilters,
    ) -> Result<PaginatedData<primitives::MatchedOrderVerbose>, OrderbookError> {
        let mut conn = self.pool.get().await?;
        const BASE_JOINS: &str = "FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
            JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id";
        const SELECT_COLUMNS: &str = "SELECT
            mo.created_at,
            mo.updated_at,
            mo.deleted_at,
            co.*,
            row_to_json(ss1.*) as source_swap,
            row_to_json(ss2.*) as destination_swap
            ";

        // When a tx_hash filter is present, try progressively broader match strategies:
        //   Exact → Prefix → Contains
        // Return as soon as a phase finds results, avoiding the slower strategies.
        // Without a tx_hash filter, only one iteration runs (mode is unused).
        let modes = if filters.tx_hash.is_some() {
            &TxHashMatchMode::PHASES[..]
        } else {
            &TxHashMatchMode::PHASES[..1]
        };

        for &mode in modes {
            let orders = {
                let mut builder = QueryBuilder::<Postgres>::new(SELECT_COLUMNS);
                builder.push(BASE_JOINS);
                builder.apply_order_filters(&filters, mode);
                builder.push(" ORDER BY mo.created_at DESC LIMIT ");
                builder.push_bind(filters.per_page());
                builder.push(" OFFSET ");
                builder.push_bind(filters.offset());
                builder
                    .build_query_as::<primitives::MatchedOrderVerbose>()
                    .fetch_all(&mut *conn)
                    .await?
            };

            if filters.tx_hash.is_some() && orders.is_empty() {
                continue;
            }

            let item_count = if !filters.are_empty() {
                let mut builder = QueryBuilder::<Postgres>::new("SELECT COUNT(*) ");
                builder.push(BASE_JOINS);
                builder.apply_order_filters(&filters, mode);
                builder
                    .build_query_scalar::<i64>()
                    .fetch_one(&mut *conn)
                    .await?
            } else {
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM matched_orders")
                    .fetch_one(&mut *conn)
                    .await?
            };

            return Ok(PaginatedData::new(
                orders,
                filters.page(),
                item_count,
                filters.per_page(),
            ));
        }

        // All match modes exhausted with no results
        Ok(PaginatedData::new(
            vec![],
            filters.page(),
            0,
            filters.per_page(),
        ))
    }

    async fn get_matched_order(
        &self,
        create_id: &str,
    ) -> Result<Option<primitives::MatchedOrderVerbose>, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        // Query for retrieving a specific matched order with its related data
        const MATCHED_ORDER_QUERY: &str = "SELECT
        mo.created_at,
        mo.updated_at,
        mo.deleted_at,
        co.*,
        row_to_json(ss1.*) as source_swap,
        row_to_json(ss2.*) as destination_swap
        FROM matched_orders mo
        JOIN create_orders co ON mo.create_order_id = co.create_id
        JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
        JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id
        WHERE mo.create_order_id = $1";

        // Fetch the matched order
        let matched_order =
            sqlx::query_as::<_, primitives::MatchedOrderVerbose>(MATCHED_ORDER_QUERY)
                .bind(create_id)
                .fetch_optional(conn)
                .await?;

        Ok(matched_order)
    }

    async fn get_matched_orders(
        &self,
        user: &str,
        filters: OrderQueryFilters,
    ) -> Result<PaginatedData<primitives::MatchedOrderVerbose>, OrderbookError> {
        let mut conn = self.pool.get().await?;

        // Convert user address to lowercase for case-insensitive comparison
        let user_lowercase = user.to_lowercase();

        // Get current timestamp for deadline checking
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| OrderbookError::InternalError("Failed to get current time".to_string()))?
            .as_secs() as i64;

        // Define pending condition queries with clear, descriptive names
        const INIT_PENDING_CONDITION: &str =
            "ss1.initiate_tx_hash = '' AND (co.additional_data->>'deadline')::bigint > $4";
        const REDEEM_PENDING_CONDITION: &str =
            "(ss1.initiate_tx_hash != '' AND ss2.refund_tx_hash = '' AND ss2.redeem_block_number = 0 AND ss2.initiate_tx_hash != '' AND ss1.refund_block_number = 0)";
        const REFUND_PENDING_CONDITION: &str =
            "(ss1.initiate_tx_hash != '' AND ss1.redeem_tx_hash = '' AND ss1.refund_block_number = 0 AND ss2.redeem_tx_hash = '')";

        // Build base query
        let mut orders_query = String::from(
            "SELECT
            mo.created_at,
            mo.updated_at,
            mo.deleted_at,
            co.*,
            row_to_json(ss1.*) as source_swap,
            row_to_json(ss2.*) as destination_swap
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
            JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id
            WHERE (lower(co.initiator_source_address) = $1 OR lower(co.user_id) = $1)",
        );

        // Add pending filter conditions if requested
        if let Some(statuses) = &filters.status {
            for status in statuses.iter() {
                match status {
                    OrderStatusVerbose::InProgress => {
                        orders_query.push_str(
                &format!(" AND ({INIT_PENDING_CONDITION} OR {REDEEM_PENDING_CONDITION} OR {REFUND_PENDING_CONDITION})")
                );
                    }
                    OrderStatusVerbose::Completed => {
                        orders_query.push_str(
                            " AND (ss2.redeem_block_number > 0 OR ss1.refund_block_number > 0)",
                        );
                    }
                    _ => {}
                }
            }
        }

        // Add ordering and pagination
        orders_query.push_str(" ORDER BY mo.created_at DESC LIMIT $2 OFFSET $3");

        // Fetch the orders with pagination
        let matched_orders = sqlx::query_as::<_, primitives::MatchedOrderVerbose>(&orders_query)
            .bind(&user_lowercase)
            .bind(filters.per_page())
            .bind(filters.offset())
            .bind(current_time)
            .fetch_all(&mut *conn)
            .await?;

        // Build the count query, similar to orders query but for counting
        let mut count_query = String::from(
            "SELECT COUNT(*)
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
            JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id
            WHERE (lower(co.initiator_source_address) = $1 OR lower(co.user_id) = $1)",
        );

        // For count query, use $2 instead of $4 for the timestamp parameter
        const COUNT_INIT_PENDING_CONDITION: &str =
            "ss1.initiate_tx_hash = '' AND (co.additional_data->>'deadline')::bigint > $2";

        // Add pending filter conditions to count query if requested
        if let Some(statuses) = &filters.status {
            for status in statuses.iter() {
                match status {
                    OrderStatusVerbose::InProgress => {
                        count_query.push_str(
                &format!(" AND ({COUNT_INIT_PENDING_CONDITION} OR {REDEEM_PENDING_CONDITION} OR {REFUND_PENDING_CONDITION})")
                );
                    }
                    OrderStatusVerbose::Completed => {
                        count_query.push_str(
                            " AND (ss2.redeem_block_number > 0 OR ss1.refund_block_number > 0)",
                        );
                    }
                    _ => {}
                }
            }
        }

        // Get the total count for pagination
        let item_count = sqlx::query_scalar::<_, i64>(&count_query)
            .bind(&user_lowercase)
            .bind(current_time)
            .fetch_one(&mut *conn)
            .await?;

        // Return paginated data
        Ok(PaginatedData::new(
            matched_orders,
            filters.page(),
            item_count,
            filters.per_page(),
        ))
    }

    async fn add_instant_refund_sacp(
        &self,
        order_id: &str,
        instant_refund_tx_bytes: &str,
    ) -> Result<(), OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        // Create the JSON payload to update order's additional_data
        let additional_data = json!({
            "instant_refund_tx_bytes": instant_refund_tx_bytes
        });

        // SQL query to update the order using PostgreSQL's JSONB concatenation operator
        const UPDATE_QUERY: &str = "UPDATE create_orders
            SET additional_data = additional_data || $2::jsonb
            WHERE create_id = $1";

        // Execute the query with parameters
        let update_result = sqlx::query(UPDATE_QUERY)
            .bind(order_id)
            .bind(additional_data)
            .execute(conn)
            .await?;

        if update_result.rows_affected() == 0 {
            return Err(OrderbookError::OrderNotFound {
                order_id: order_id.to_string(),
            });
        }

        Ok(())
    }

    async fn add_redeem_sacp(
        &self,
        order_id: &str,
        redeem_tx_bytes: &str,
        redeem_tx_id: &str,
        secret: &str,
    ) -> Result<(), OrderbookError> {
        let mut conn = self.pool.get().await?;

        // Create the JSON payload with the redeem transaction bytes
        let additional_data = json!({
            "redeem_tx_bytes": redeem_tx_bytes
        });

        // First, update the create_orders table with the redeem transaction bytes
        const CREATE_ORDER_UPDATE: &str = "
            UPDATE create_orders
            SET additional_data = additional_data || $2::jsonb
            WHERE create_id = $1";

        let order_update_result = sqlx::query(CREATE_ORDER_UPDATE)
            .bind(order_id)
            .bind(&additional_data)
            .execute(&mut *conn)
            .await?;

        if order_update_result.rows_affected() == 0 {
            return Err(OrderbookError::OrderNotFound {
                order_id: order_id.to_string(),
            });
        }

        // Then, update the corresponding destination swap with the redeem transaction hash and secret
        const DESTINATION_SWAP_UPDATE: &str = "
            UPDATE swaps
            SET redeem_tx_hash = $1, secret = $2
            WHERE swap_id = (
                SELECT destination_swap_id
                FROM matched_orders
                WHERE create_order_id = $3
            )";

        let destination_swap_update_result = sqlx::query(DESTINATION_SWAP_UPDATE)
            .bind(redeem_tx_id)
            .bind(secret)
            .bind(order_id)
            .execute(&mut *conn)
            .await?;

        if destination_swap_update_result.rows_affected() == 0 {
            return Err(OrderbookError::SwapNotFound {
                order_id: order_id.to_string(),
            });
        }

        Ok(())
    }

    async fn update_additional_data(
        &self,
        create_id: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<(), OrderbookError> {
        if data.is_empty() {
            return Ok(());
        }
        let conn = &mut *self.pool.get().await?;

        let json_value =
            serde_json::to_value(data).map_err(|e| OrderbookError::InternalError(e.to_string()))?;

        const UPDATE_QUERY: &str = "UPDATE create_orders
            SET additional_data = COALESCE(additional_data, '{}'::jsonb) || $1::jsonb
            WHERE create_id = $2";

        let update_result = sqlx::query(UPDATE_QUERY)
            .bind(&json_value)
            .bind(create_id)
            .execute(conn)
            .await?;

        if update_result.rows_affected() == 0 {
            return Err(OrderbookError::OrderNotFound {
                order_id: create_id.to_string(),
            });
        }

        Ok(())
    }

    async fn get_filler_pending_orders(
        &self,
        chain_id: &str,
        filler_id: &str,
    ) -> Result<Vec<MatchedOrderVerbose>, OrderbookError> {
        let mut conn = self.pool.get().await?;
        let orders = sqlx::query_as::<_, MatchedOrderVerbose>(
            "SELECT
                mo.created_at,
                mo.updated_at,
                mo.deleted_at,
                row_to_json(ss.*) as source_swap,
                row_to_json(ds.*) as destination_swap,
                co.*
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss ON mo.source_swap_id = ss.swap_id
            JOIN swaps ds ON mo.destination_swap_id = ds.swap_id
            WHERE ((ds.chain = $1) OR (ss.chain = $1))
            AND (lower(ss.redeemer) = $2 OR lower(ds.initiator) = $2)
		    AND
		    (
		    	(ss.initiate_tx_hash != '' AND ss.refund_tx_hash = '' AND ds.initiate_tx_hash = '')
		    	OR
		    	(ds.secret != '' AND ss.redeem_tx_hash = '')
		    	OR
		    	(ds.initiate_tx_hash != '' AND ds.refund_tx_hash = '' AND ds.redeem_tx_hash = '')
		    	OR
		    	(ss.refund_tx_hash = '' AND ss.redeem_tx_hash = '' AND ds.refund_block_number > 0)
		    )
            ORDER BY mo.created_at ASC
            LIMIT 1000",
        )
        .bind(chain_id)
        .bind(filler_id.to_lowercase())
        .fetch_all(&mut *conn)
        .await?;
        Ok(orders)
    }

    async fn get_solver_pending_orders(&self) -> Result<Vec<MatchedOrderVerbose>, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        let orders = sqlx::query_as::<_, MatchedOrderVerbose>(
            "SELECT
                mo.created_at,
                mo.updated_at,
                mo.deleted_at,
                row_to_json(ss.*) as source_swap,
                row_to_json(ds.*) as destination_swap,
                co.*
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss ON mo.source_swap_id = ss.swap_id
            JOIN swaps ds ON mo.destination_swap_id = ds.swap_id
            WHERE (
                (ss.initiate_tx_hash != '' AND ss.refund_tx_hash = '' AND ds.initiate_tx_hash = '')
                OR
                (ds.secret != '' AND ss.redeem_tx_hash = '' AND ss.refund_tx_hash = '')
                OR
                (ds.initiate_tx_hash != '' AND ds.refund_tx_hash = '' AND ds.redeem_tx_hash = '')
                OR
                (ss.refund_tx_hash = '' AND ss.redeem_tx_hash = '' AND ds.refund_block_number > 0)
            )
            ORDER BY mo.created_at ASC
            LIMIT 5000",
        )
        .fetch_all(conn)
        .await?;
        Ok(orders)
    }

    async fn get_matched_order_by_swap_id(
        &self,
        swap_id: &str,
    ) -> Result<Option<MatchedOrderVerbose>, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        const GET_MATCHED_ORDER_BY_SWAP_ID_QUERY: &str = "SELECT
            mo.created_at,
            mo.updated_at,
            mo.deleted_at,
            co.*,
            row_to_json(ss1.*) as source_swap,
            row_to_json(ss2.*) as destination_swap
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
            JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id
            WHERE ss1.swap_id = $1 OR ss2.swap_id = $1";

        let order = sqlx::query_as::<_, MatchedOrderVerbose>(GET_MATCHED_ORDER_BY_SWAP_ID_QUERY)
            .bind(swap_id)
            .fetch_optional(conn)
            .await?;

        Ok(order)
    }

    async fn get_unscreened_orders(&self) -> Result<Vec<MatchedOrderVerbose>, OrderbookError> {
        let conn = &mut *self.pool.get().await?;

        const GET_UNSCREENED_ORDERS_QUERY: &str = "SELECT
            mo.created_at,
            mo.updated_at,
            mo.deleted_at,
            co.*,
            row_to_json(ss1.*) as source_swap,
            row_to_json(ss2.*) as destination_swap
            FROM matched_orders mo
            JOIN create_orders co ON mo.create_order_id = co.create_id
            JOIN swaps ss1 ON mo.source_swap_id = ss1.swap_id
            JOIN swaps ss2 ON mo.destination_swap_id = ss2.swap_id
            WHERE ss1.initiate_block_number IS NOT NULL
                AND (ss1.initiate_block_number) != 0
                AND (ss1.required_confirmations <= 1 OR ss1.current_confirmations >= ss1.required_confirmations)
                AND (co.additional_data->>'is_blacklisted' IS NULL)
            ORDER BY mo.created_at ASC
            LIMIT 500";

        let orders = sqlx::query_as::<_, MatchedOrderVerbose>(GET_UNSCREENED_ORDERS_QUERY)
            .fetch_all(conn)
            .await?;

        Ok(orders)
    }

    async fn update_swap_initiate(
        &self,
        order_id: &str,
        filled_amount: BigDecimal,
        initiate_tx_hash: &str,
        initiate_block_number: i64,
        initiate_timestamp: chrono::DateTime<chrono::Utc>,
        asset: &str,
    ) -> Result<(), OrderbookError> {
        let mut conn = self.pool.get().await?;
        // SQL query to update swap initiation details
        const UPDATE_SWAP_QUERY: &str = "UPDATE swaps
            SET filled_amount = $1,
                initiate_tx_hash = $2,
                initiate_block_number = $3,
                initiate_timestamp = $4
            WHERE swap_id = $5 AND LOWER(asset) = $6";

        // Execute the query and get the result for checking rows affected
        let update_result = sqlx::query(UPDATE_SWAP_QUERY)
            .bind(&filled_amount)
            .bind(initiate_tx_hash)
            .bind(initiate_block_number)
            .bind(initiate_timestamp)
            .bind(order_id)
            .bind(asset.to_lowercase())
            .execute(&mut *conn)
            .await?;

        // Check if any rows were affected by the update
        if update_result.rows_affected() == 0 {
            return Err(OrderbookError::SwapNotFound {
                order_id: order_id.to_string(),
            });
        }

        Ok(())
    }

    async fn update_swap_redeem(
        &self,
        order_id: &str,
        redeem_tx_hash: &str,
        secret: &str,
        redeem_block_number: i64,
        redeem_timestamp: chrono::DateTime<chrono::Utc>,
        asset: &str,
    ) -> Result<(), OrderbookError> {
        let mut conn = self.pool.get().await?;
        const UPDATE_SWAP_QUERY: &str = "UPDATE swaps
            SET redeem_tx_hash = $1,
                secret = $2,
                redeem_block_number = $3,
                redeem_timestamp = $4
            WHERE swap_id = $5 AND LOWER(asset) = $6";

        let res = sqlx::query(UPDATE_SWAP_QUERY)
            .bind(redeem_tx_hash)
            .bind(secret)
            .bind(redeem_block_number)
            .bind(redeem_timestamp)
            .bind(&order_id)
            .bind(asset.to_lowercase())
            .execute(&mut *conn)
            .await?;

        if res.rows_affected() == 0 {
            return Err(OrderbookError::SwapNotFound {
                order_id: order_id.to_string(),
            });
        }
        Ok(())
    }

    async fn update_swap_refund(
        &self,
        order_id: &str,
        refund_tx_hash: &str,
        refund_block_number: i64,
        refund_timestamp: chrono::DateTime<chrono::Utc>,
        asset: &str,
    ) -> Result<(), OrderbookError> {
        let mut conn = self.pool.get().await?;
        const UPDATE_SWAP_QUERY: &str = "UPDATE swaps
            SET refund_tx_hash = $1,
                refund_block_number = $2,
                refund_timestamp = $3
            WHERE swap_id = $4 AND LOWER(asset) = $5";

        let res = sqlx::query(UPDATE_SWAP_QUERY)
            .bind(refund_tx_hash)
            .bind(refund_block_number)
            .bind(refund_timestamp)
            .bind(&order_id)
            .bind(asset.to_lowercase())
            .execute(&mut *conn)
            .await?;

        if res.rows_affected() == 0 {
            return Err(OrderbookError::SwapNotFound {
                order_id: order_id.to_string(),
            });
        }
        Ok(())
    }

    async fn update_confirmations(
        &self,
        chain_identifier: &str,
        latest_block: u64,
    ) -> Result<(), OrderbookError> {
        let mut conn = self.pool.get().await?;
        const UPDATE_SWAP_QUERY: &str =
            "UPDATE swaps
            SET current_confirmations = LEAST(required_confirmations, $1 - initiate_block_number + 1)
            WHERE chain = $2
            AND required_confirmations > current_confirmations
            AND initiate_tx_hash != ''";

        sqlx::query(UPDATE_SWAP_QUERY)
            .bind(latest_block as i64)
            .bind(chain_identifier)
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    async fn get_volume_and_fees(
        &self,
        query: StatsQueryFilters,
        asset_decimals: &HashMap<(String, String), u32>,
    ) -> Result<(BigInt, BigInt), OrderbookError> {
        let mut conn = self.pool.get().await?;
        let start_time = match query.from {
            Some(from) => Some(DateTime::from_timestamp(from, 0).ok_or_else(|| {
                OrderbookError::InvalidTimestamp("Failed to parse from timestamp".to_string())
            })?),
            None => None,
        };

        // End time is optional, if not provided, use current time
        let end_time = match query.to {
            Some(to) => DateTime::from_timestamp(to, 0).ok_or_else(|| {
                OrderbookError::InvalidTimestamp("Failed to parse to timestamp".to_string())
            })?,
            None => Utc::now(),
        };

        const BASE_QUERY: &str = "SELECT
                    ss1.chain as source_chain,
                    ss1.asset as source_asset,
                    ss1.amount as source_amount,
                    ss2.chain as destination_chain, 
                    ss2.asset as destination_asset, 
                    ss2.amount as destination_amount,
                    (co.additional_data->>'input_token_price')::float as source_token_price,
                    (co.additional_data->>'output_token_price')::float as destination_token_price
                FROM matched_orders mo
                JOIN create_orders co ON mo.create_order_id = co.create_id
                JOIN swaps ss1 ON (ss1.swap_id = mo.source_swap_id)
                JOIN swaps ss2 ON (ss2.swap_id = mo.destination_swap_id)
                WHERE ss1.redeem_tx_hash != '' 
                AND ss2.redeem_tx_hash != ''";

        let rows = {
            let mut builder = QueryBuilder::<Postgres>::new(BASE_QUERY);

            builder.add_time_range_filter(start_time, end_time);

            if let Some(ref source_chain) = query.source_chain {
                builder.add_chain_filter("ss1.chain", &source_chain);
            }

            if let Some(ref destination_chain) = query.destination_chain {
                builder.add_chain_filter("ss2.chain", &destination_chain);
            }

            if let Some(ref address) = query.address {
                builder.add_address_filter(&address);
            }

            builder.build().fetch_all(&mut *conn).await?
        };

        let mut total_volume = BigDecimal::from(0);
        let mut total_fees = BigDecimal::from(0);

        for row in rows {
            let source_chain: String = row.get("source_chain");
            let source_asset: String = row.get("source_asset");
            let source_amount: BigDecimal = row.get("source_amount");

            let destination_chain: String = row.get("destination_chain");
            let destination_asset: String = row.get("destination_asset");
            let destination_amount: BigDecimal = row.get("destination_amount");

            let source_token_price: f64 = row.get("source_token_price");
            let destination_token_price: f64 = row.get("destination_token_price");

            let source_asset_decimals =
                match asset_decimals.get(&(source_chain.clone(), source_asset.to_lowercase())) {
                    Some(d) => *d,
                    None => 8,
                };

            let destination_asset_decimals = match asset_decimals
                .get(&(destination_chain.clone(), destination_asset.to_lowercase()))
            {
                Some(d) => *d,
                None => 8,
            };

            let source_divisor = BigDecimal::from(10_u64.pow(source_asset_decimals));
            let destination_divisor = BigDecimal::from(10_u64.pow(destination_asset_decimals));

            let normalized_source_amount = &source_amount / &source_divisor;
            let normalized_destination_amount = &destination_amount / &destination_divisor;

            let source_value = normalized_source_amount
                * BigDecimal::from_f64(source_token_price).ok_or_else(|| {
                    OrderbookError::InternalError(format!(
                        "Failed to parse token price: {}",
                        source_token_price
                    ))
                })?;

            let dest_value = normalized_destination_amount
                * BigDecimal::from_f64(destination_token_price).ok_or_else(|| {
                    OrderbookError::InternalError(format!(
                        "Failed to parse token price: {}",
                        destination_token_price
                    ))
                })?;

            let fee = &source_value - &dest_value;
            total_fees += fee;

            if query.source_chain.is_some() {
                total_volume += &source_value;
            }

            if query.destination_chain.is_some() {
                total_volume += &dest_value;
            }

            if query.source_chain.is_none() && query.destination_chain.is_none() {
                total_volume += &source_value + &dest_value;
            }
        }

        let total_volume_int = total_volume.to_bigint().ok_or_else(|| {
            OrderbookError::InternalError("Failed to convert total volume to bigint".to_string())
        })?;

        let total_fees_int = total_fees.to_bigint().ok_or_else(|| {
            OrderbookError::InternalError("Failed to convert total fees to bigint".to_string())
        })?;

        Ok((total_volume_int, total_fees_int))
    }

    async fn get_volume(
        &self,
        query: StatsQueryFilters,
        asset_decimals: &HashMap<(String, String), u32>,
    ) -> Result<BigDecimal, OrderbookError> {
        let (total_volume, _) = self.get_volume_and_fees(query, asset_decimals).await?;

        Ok(BigDecimal::from(total_volume))
    }

    async fn get_fees(
        &self,
        query: StatsQueryFilters,
        asset_decimals: &HashMap<(String, String), u32>,
    ) -> Result<BigDecimal, OrderbookError> {
        let (_, total_fees) = self.get_volume_and_fees(query, asset_decimals).await?;

        Ok(BigDecimal::from(total_fees))
    }

    async fn get_integrator_fees(&self, integrator: &str) -> Result<Vec<Claim>, OrderbookError> {
        let mut conn = self.pool.get().await?;
        const QUERY: &str = "
            SELECT 
                integrator_name,
                address,
                chain,
                token_address,
                total_earnings,
                claim_signature,
                claim_contract
            FROM affiliate_fees 
            WHERE integrator_name = $1
        ";

        let claims = sqlx::query_as::<_, Claim>(QUERY)
            .bind(integrator)
            .fetch_all(&mut *conn)
            .await?;

        Ok(claims)
    }

    async fn create_matched_order(
        &self,
        order: &MatchedOrderVerbose,
    ) -> Result<(), OrderbookError> {
        let tx_timeout = Duration::from_secs(30);

        let result = timeout(tx_timeout, async {
        let mut tx = self.sqlx_pool.begin().await?;

        // Inserting both swaps in a single query
        const INSERT_SWAPS_QUERY: &str = "
            INSERT INTO swaps (
                created_at, updated_at, deleted_at, swap_id, chain, asset, initiator, redeemer, timelock,
                filled_amount, amount, secret_hash, secret, initiate_tx_hash, redeem_tx_hash, refund_tx_hash,
                initiate_block_number, redeem_block_number, refund_block_number, required_confirmations,
                current_confirmations, htlc_address, token_address
            ) VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23),
                ($24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46)
            ON CONFLICT DO NOTHING
        ";

        let res = sqlx::query(INSERT_SWAPS_QUERY)
            // Source swap
            .bind(&order.source_swap.created_at)
            .bind(&order.source_swap.updated_at)
            .bind(&order.source_swap.deleted_at)
            .bind(&order.source_swap.swap_id)
            .bind(&order.source_swap.chain)
            .bind(&order.source_swap.asset)
            .bind(&order.source_swap.initiator)
            .bind(&order.source_swap.redeemer)
            .bind(&order.source_swap.timelock)
            .bind(&order.source_swap.filled_amount)
            .bind(&order.source_swap.amount)
            .bind(&order.source_swap.secret_hash)
            .bind(&order.source_swap.secret.to_string())
            .bind(&order.source_swap.initiate_tx_hash.to_string())
            .bind(&order.source_swap.redeem_tx_hash.to_string())
            .bind(&order.source_swap.refund_tx_hash.to_string())
            .bind(&order.source_swap.initiate_block_number)
            .bind(&order.source_swap.redeem_block_number)
            .bind(&order.source_swap.refund_block_number)
            .bind(&order.source_swap.required_confirmations)
            .bind(&order.source_swap.current_confirmations)
            .bind(order.source_swap.htlc_address.as_deref().unwrap_or(""))
            .bind(order.source_swap.token_address.as_deref().unwrap_or(""))
            // Destination swap
            .bind(&order.destination_swap.created_at)
            .bind(&order.destination_swap.updated_at)
            .bind(&order.destination_swap.deleted_at)
            .bind(&order.destination_swap.swap_id)
            .bind(&order.destination_swap.chain)
            .bind(&order.destination_swap.asset)
            .bind(&order.destination_swap.initiator)
            .bind(&order.destination_swap.redeemer)
            .bind(&order.destination_swap.timelock)
            .bind(&order.destination_swap.filled_amount)
            .bind(&order.destination_swap.amount)
            .bind(&order.destination_swap.secret_hash)
            .bind(&order.destination_swap.secret.to_string())
            .bind(&order.destination_swap.initiate_tx_hash.to_string())
            .bind(&order.destination_swap.redeem_tx_hash.to_string())
            .bind(&order.destination_swap.refund_tx_hash.to_string())
            .bind(&order.destination_swap.initiate_block_number)
            .bind(&order.destination_swap.redeem_block_number)
            .bind(&order.destination_swap.refund_block_number)
            .bind(&order.destination_swap.required_confirmations)
            .bind(&order.destination_swap.current_confirmations)
            .bind(order.destination_swap.htlc_address.as_deref().unwrap_or(""))
            .bind(order.destination_swap.token_address.as_deref().unwrap_or(""))
            .execute(&mut *tx)
            .await?;

        if res.rows_affected() < 2 {
            tx.rollback().await?;
            return Err(OrderbookError::OrderAlreadyExists(
                order.create_order.create_id.clone(),
            ));
        }

        const INSERT_CREATE_ORDER_QUERY: &str = "
            INSERT INTO create_orders (
                created_at, updated_at, deleted_at, create_id, block_number, source_chain, destination_chain,
                source_asset, destination_asset, initiator_source_address, initiator_destination_address,
                source_amount, destination_amount, fee, nonce, min_destination_confirmations, timelock,
                secret_hash, user_id, affiliate_fees, solver_id,additional_data
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
            )
            ON CONFLICT DO NOTHING
        ";

        let co = &order.create_order;
        let additional_data_json = serde_json::to_value(&co.additional_data)
            .map_err(|e| OrderbookError::InternalError(e.to_string()))?;
        let affiliate_fees_json = serde_json::to_value(&co.affiliate_fees)
            .map_err(|e| OrderbookError::InternalError(e.to_string()))?;
        let res = sqlx::query(INSERT_CREATE_ORDER_QUERY)
            .bind(&co.created_at)
            .bind(&co.updated_at)
            .bind(&co.deleted_at)
            .bind(&co.create_id)
            .bind(&co.block_number)
            .bind(&co.source_chain)
            .bind(&co.destination_chain)
            .bind(&co.source_asset)
            .bind(&co.destination_asset)
            .bind(&co.initiator_source_address)
            .bind(&co.initiator_destination_address)
            .bind(&co.source_amount)
            .bind(&co.destination_amount)
            .bind(&co.fee)
            .bind(&co.nonce)
            .bind(&co.min_destination_confirmations)
            .bind(&co.timelock)
            .bind(&co.secret_hash)
            .bind(&co.user_id)
            .bind(&affiliate_fees_json)
            .bind(&co.solver_id)
            .bind(&additional_data_json)
            .execute(&mut *tx)
            .await?;

        if res.rows_affected() == 0 {
            tx.rollback().await?;
            return Err(OrderbookError::OrderAlreadyExists(co.create_id.clone()));
        }

        const INSERT_MATCHED_ORDER_QUERY: &str = "
            INSERT INTO matched_orders (create_order_id, source_swap_id, destination_swap_id, created_at, updated_at, deleted_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
        ";

        let res = sqlx::query(INSERT_MATCHED_ORDER_QUERY)
            .bind(&co.create_id)
            .bind(&order.source_swap.swap_id)
            .bind(&order.destination_swap.swap_id)
            .bind(&order.create_order.created_at)
            .bind(&order.create_order.updated_at)
            .bind(&order.create_order.deleted_at)
            .execute(&mut *tx)
            .await?;

        if res.rows_affected() == 0 {
            tx.rollback().await?;
            return Err(OrderbookError::OrderAlreadyExists(co.create_id.clone()));
        }

        tx.commit().await?;
        Ok(())
    }).await;

        match result {
            Ok(res) => res,
            Err(_) => Err(OrderbookError::InternalError(
                "Transaction timeout".to_string(),
            )),
        }
    }
}

/// Strategy for matching tx hashes in queries, tried in order from fastest to broadest.
#[derive(Clone, Copy)]
enum TxHashMatchMode {
    /// `LOWER(col) = hash` — uses indexes directly
    Exact,
    /// `LOWER(col) LIKE 'hash%'` — index-friendly, matches Bitcoin `hash:block_number` format
    Prefix,
    /// `LOWER(col) LIKE '%hash%'` — full scan fallback for comma-separated multi-hash values
    Contains,
}

impl TxHashMatchMode {
    const PHASES: [TxHashMatchMode; 3] = [
        TxHashMatchMode::Exact,
        TxHashMatchMode::Prefix,
        TxHashMatchMode::Contains,
    ];
}

trait OrderbookQueryBuilder<'a> {
    fn add_where_clause(&mut self);
    fn add_address_filter(&mut self, address: &'a str);
    fn add_from_owner_filter(&mut self, addresses: &HashSet<String>);
    fn add_to_owner_filter(&mut self, addresses: &HashSet<String>);
    fn add_tx_hash_filter(&mut self, tx_hash: &'a str, mode: TxHashMatchMode);
    fn add_time_range_filter(&mut self, start_time: Option<DateTime<Utc>>, end_time: DateTime<Utc>);
    fn add_chain_filter(&mut self, column: &'a str, chain: &'a str);
    fn add_statuses_filter(&mut self, status: &HashSet<OrderStatusVerbose>);
    fn add_solver_id_filter(&mut self, solver_ids: &HashSet<String>);
    fn add_integrator_filter(&mut self, integrator: &HashSet<String>);
    fn apply_order_filters(
        &mut self,
        filters: &'a OrderQueryFilters,
        tx_hash_mode: TxHashMatchMode,
    );
}

impl<'a> OrderbookQueryBuilder<'a> for QueryBuilder<'a, Postgres> {
    fn add_where_clause(&mut self) {
        if !self.sql().contains("WHERE") {
            self.push(" WHERE ");
        } else {
            self.push(" AND ");
        }
    }

    fn add_address_filter(&mut self, address: &'a str) {
        let lower_address = address.to_lowercase();
        self.add_where_clause();
        self.push("(");
        self.push("LOWER(ss1.initiator) = ");
        self.push_bind(lower_address.clone());
        self.push(" OR LOWER(ss2.initiator) = ");
        self.push_bind(lower_address.clone());
        self.push(" OR LOWER(ss1.redeemer) = ");
        self.push_bind(lower_address.clone());
        self.push(" OR LOWER(ss2.redeemer) = ");
        self.push_bind(lower_address.clone());
        self.push(" OR LOWER(co.user_id) = ");
        self.push_bind(lower_address.clone());
        self.push(" OR (co.additional_data::jsonb->>'bitcoin_optional_recipient' IS NOT NULL AND LOWER(co.additional_data::jsonb->>'bitcoin_optional_recipient') = ");
        self.push_bind(lower_address.clone());
        self.push("))");
    }

    fn add_from_owner_filter(&mut self, addresses: &HashSet<String>) {
        let lowercased_addresses = addresses
            .iter()
            .map(|address| address.to_lowercase())
            .collect::<Vec<String>>();

        self.add_where_clause();
        self.push("LOWER(co.initiator_source_address) = ANY(");
        self.push_bind(lowercased_addresses);
        self.push(")");
    }

    fn add_to_owner_filter(&mut self, addresses: &HashSet<String>) {
        let lowercased_addresses = addresses
            .iter()
            .map(|address| address.to_lowercase())
            .collect::<Vec<String>>();
        self.add_where_clause();
        self.push("LOWER(co.initiator_destination_address) = ANY(");
        self.push_bind(lowercased_addresses);
        self.push(")");
    }

    fn add_solver_id_filter(&mut self, solver_ids: &HashSet<String>) {
        let lowercased_solver_ids = solver_ids
            .iter()
            .map(|solver_id| solver_id.to_lowercase())
            .collect::<Vec<String>>();
        self.add_where_clause();
        self.push("LOWER(co.solver_id) = ANY(");
        self.push_bind(lowercased_solver_ids);
        self.push(")");
    }

    fn add_integrator_filter(&mut self, integrator: &HashSet<String>) {
        let lower_integrator = integrator
            .iter()
            .map(|integrator| integrator.to_lowercase())
            .collect::<Vec<String>>();
        self.add_where_clause();
        self.push("LOWER(COALESCE(co.additional_data->>'integrator', 'Garden')) = ANY(");
        self.push_bind(lower_integrator);
        self.push(")");
    }

    fn add_tx_hash_filter(&mut self, tx_hash: &'a str, mode: TxHashMatchMode) {
        let lowered = tx_hash.to_lowercase();
        let (operator, pattern) = match mode {
            TxHashMatchMode::Exact => ("=", lowered),
            TxHashMatchMode::Prefix => ("LIKE", format!("{}%", lowered)),
            TxHashMatchMode::Contains => ("LIKE", format!("%{}%", lowered)),
        };

        const TX_HASH_COLUMNS: [&str; 6] = [
            "ss1.initiate_tx_hash",
            "ss2.initiate_tx_hash",
            "ss1.refund_tx_hash",
            "ss2.refund_tx_hash",
            "ss1.redeem_tx_hash",
            "ss2.redeem_tx_hash",
        ];

        self.add_where_clause();
        self.push("(");
        for (i, col) in TX_HASH_COLUMNS.iter().enumerate() {
            if i > 0 {
                self.push(" OR ");
            }
            self.push(format!("LOWER({}) {} ", col, operator));
            self.push_bind(pattern.clone());
        }
        self.push(")");
    }

    fn add_time_range_filter(&mut self, from_time: Option<DateTime<Utc>>, to_time: DateTime<Utc>) {
        if let Some(from) = from_time {
            self.add_where_clause();
            self.push("co.created_at >= ");
            self.push_bind(from);
        }
        self.add_where_clause();
        self.push("co.created_at <= ");
        self.push_bind(to_time);
    }

    fn add_chain_filter(&mut self, column: &'a str, chain: &'a str) {
        self.add_where_clause();
        self.push(column);
        self.push(" = ");
        self.push_bind(chain);
    }

    fn add_statuses_filter(&mut self, statuses: &HashSet<OrderStatusVerbose>) {
        let not_initiated = "(co.additional_data->>'deadline')::bigint > EXTRACT(EPOCH FROM NOW())::bigint AND ss1.initiate_tx_hash = ''";
        let in_progress = "ss1.initiate_tx_hash != ''
            AND ss1.redeem_tx_hash = ''
            AND ss1.refund_tx_hash = ''";
        let completed = "ss1.redeem_tx_hash <> '' OR ss1.refund_tx_hash <> ''";
        let expired = "(co.additional_data->>'deadline')::bigint < EXTRACT(EPOCH FROM NOW())::bigint AND ss1.initiate_tx_hash = ''";
        let refunded = "ss1.refund_tx_hash <> ''";

        // Hack: Display all orders except those which are not initiated by LI.FI, Phantom and are not expired
        let display = "(ss1.initiate_tx_hash != '')  OR  (COALESCE(co.additional_data->>'integrator', '') NOT IN ('LI.FI', 'phantom') AND (co.additional_data->>'deadline')::bigint >= EXTRACT(EPOCH FROM NOW())::bigint)";

        self.add_where_clause();
        self.push("("); // <-- open group
        for (i, status) in statuses.iter().enumerate() {
            if i > 0 {
                self.push(" OR ");
            }

            let clause = match status {
                OrderStatusVerbose::NotInitiated => not_initiated,
                OrderStatusVerbose::InProgress => in_progress,
                OrderStatusVerbose::Completed => completed,
                OrderStatusVerbose::Expired => expired,
                OrderStatusVerbose::Refunded => refunded,
                OrderStatusVerbose::Display => display,
            };

            self.push("(");
            self.push(clause);
            self.push(")");
        }
        self.push(")"); // <-- close group
    }

    fn apply_order_filters(
        &mut self,
        filters: &'a OrderQueryFilters,
        tx_hash_mode: TxHashMatchMode,
    ) {
        if let Some(address) = &filters.address {
            self.add_address_filter(address);
        } else {
            if let Some(from) = &filters.from_owner {
                self.add_from_owner_filter(from);
            }
            if let Some(to) = &filters.to_owner {
                self.add_to_owner_filter(to);
            }
        }

        if let Some(tx_hash) = &filters.tx_hash {
            self.add_tx_hash_filter(tx_hash, tx_hash_mode);
        }

        if let Some(from_chain) = &filters.from_chain {
            self.add_chain_filter("ss1.chain", from_chain.as_ref());
        }

        if let Some(to_chain) = &filters.to_chain {
            self.add_chain_filter("ss2.chain", to_chain.as_ref());
        }

        if let Some(statuses) = &filters.status {
            self.add_statuses_filter(statuses);
        }

        if let Some(solver_id) = &filters.solver_id {
            self.add_solver_id_filter(solver_id);
        }

        if let Some(integrator) = &filters.integrator {
            self.add_integrator_filter(integrator);
        }
    }
}
