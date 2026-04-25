## ADDED Requirements

### Requirement: Trading-Day Driven Backtest Timeline
The system MUST construct backtest rebalance points from historical trading days within the requested date range, rather than relying on snapshot ingestion timestamps.

#### Scenario: Generate rebalance dates from trading calendar
- **WHEN** a user submits a backtest task with start date, end date, and rebalance frequency
- **THEN** the engine derives rebalance dates from historical trading days matching the configured frequency

#### Scenario: Insufficient trading-day coverage
- **WHEN** available trading days in the requested range cannot form at least two rebalance points
- **THEN** the system rejects execution with a clear message including available data date range

### Requirement: As-Of Fundamental Reconstruction
The system MUST reconstruct per-symbol fundamental features at each rebalance date using only records visible on or before that date based on announcement visibility rules.

#### Scenario: Build features using visible financial statements
- **WHEN** the engine evaluates symbol S at rebalance date T
- **THEN** it selects the latest visible financial records where announcement date is less than or equal to T

#### Scenario: Missing visibility fields fallback
- **WHEN** announcement date is unavailable for a financial record
- **THEN** the system applies configured fallback visibility rule and records fallback usage in diagnostics

### Requirement: Historical Price Return Computation
The system MUST compute forward returns from historical price data between rebalance intervals and use those returns for portfolio and benchmark performance evaluation.

#### Scenario: Compute symbol return between two rebalance dates
- **WHEN** a symbol has valid prices at rebalance date T and next rebalance date T+1
- **THEN** the system computes symbol forward return from those historical prices

#### Scenario: Benchmark comparison with same timeline
- **WHEN** portfolio return is computed for a rebalance interval
- **THEN** benchmark return is computed over the same interval and used for excess metrics

### Requirement: Coverage and Exclusion Diagnostics
The system MUST provide diagnostics that explain data coverage and exclusion outcomes, including available date range, sample counts, and exclusion reason distribution.

#### Scenario: Return coverage metadata for successful run
- **WHEN** a backtest run completes successfully
- **THEN** the result includes data coverage range and exclusion diagnostics in structured fields

#### Scenario: Return actionable diagnostics for failed run
- **WHEN** a backtest run fails due to data availability constraints
- **THEN** the error detail includes coverage range and primary exclusion reasons

### Requirement: Frontend Date-Range Guardrail
The system MUST expose backtest data availability range to frontend so task submission can be validated before execution.

#### Scenario: Block frontend submission outside coverage
- **WHEN** user-selected backtest date range falls outside available data range
- **THEN** frontend prevents submission and shows guidance with allowed range

#### Scenario: Allow submission within coverage
- **WHEN** user-selected range is within available data coverage
- **THEN** frontend allows submission and sends task request to backend
