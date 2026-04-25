## ADDED Requirements

### Requirement: Backtest Job Lifecycle
The system MUST provide a backtest job lifecycle for score strategies, including create, execute, query status, and fetch result summary. Each job SHALL persist immutable strategy parameters and execution metadata to ensure reproducibility.

#### Scenario: Create backtest job with parameters
- **WHEN** a user submits a backtest request with strategy parameters and date range
- **THEN** the system creates a new backtest job record with a unique job identifier and persisted parameter snapshot

#### Scenario: Query running job status
- **WHEN** a user queries a submitted backtest job before completion
- **THEN** the system returns current state, progress metadata, and last update timestamp

#### Scenario: Retrieve completed result summary
- **WHEN** a user queries a completed backtest job
- **THEN** the system returns core performance metrics and references to detailed curve/layer outputs

### Requirement: Time-Consistent Feature Reconstruction
The system MUST reconstruct score inputs using only data available at each rebalance timestamp. The feature builder SHALL prevent look-ahead bias by applying announcement-time availability rules before score calculation.

#### Scenario: Build features on rebalance date
- **WHEN** the engine evaluates symbols at rebalance date T
- **THEN** it only uses financial and market data with availability timestamp less than or equal to T

#### Scenario: Data unavailable at timestamp
- **WHEN** required fields are not available by timestamp T for a symbol
- **THEN** the system excludes that symbol from eligible universe for the current rebalance and records exclusion reason

### Requirement: Portfolio Construction and Rebalance
The system MUST support score-based portfolio construction at configurable rebalance frequency, including top-N or quantile selection with equal-weight allocation. The engine SHALL account for turnover and configurable transaction costs at each rebalance event.

#### Scenario: Quantile portfolio selection
- **WHEN** the strategy mode is configured as top-quantile
- **THEN** the system selects symbols in the configured highest quantile by score and assigns equal weights

#### Scenario: Rebalance with transaction cost
- **WHEN** the portfolio transitions from previous holdings to target holdings
- **THEN** the system computes turnover and deducts configured transaction costs from portfolio return

### Requirement: Performance and Diagnostic Metrics
The system MUST compute and persist backtest evaluation metrics, including annualized return, max drawdown, Sharpe ratio, excess return versus benchmark, turnover, and score predictive diagnostics (IC/RankIC and quantile layer returns).

#### Scenario: Persist core performance metrics
- **WHEN** a backtest job finishes successfully
- **THEN** the system stores required core metrics and benchmark-relative metrics in the result record

#### Scenario: Persist score diagnostics
- **WHEN** layer and IC evaluation is enabled in a job
- **THEN** the system stores quantile return series and IC/RankIC statistics for later query
