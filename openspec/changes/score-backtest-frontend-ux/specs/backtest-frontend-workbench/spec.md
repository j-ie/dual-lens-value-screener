## ADDED Requirements

### Requirement: Backtest Task Creation Form
The system MUST provide a frontend backtest creation form that allows users to configure strategy parameters, validates inputs before submission, and persists a parameter snapshot through backend API.

#### Scenario: Submit valid backtest configuration
- **WHEN** a user fills required fields with valid values and submits the form
- **THEN** the system creates a backtest task and returns a task identifier for tracking

#### Scenario: Block invalid parameter submission
- **WHEN** a user submits the form with invalid ranges or missing required fields
- **THEN** the system prevents submission and displays field-level validation errors

### Requirement: Backtest Task List and Status Tracking
The system MUST provide a task list view that displays backtest tasks with status, key parameters, created time, and latest progress metadata, and SHALL support status filtering.

#### Scenario: View running tasks
- **WHEN** the user opens the backtest workbench
- **THEN** the system shows running tasks with refreshed status and progress fields

#### Scenario: Filter by task status
- **WHEN** the user selects a status filter such as running, success, or failed
- **THEN** the system shows only tasks matching the selected status

### Requirement: Backtest Result Analysis View
The system MUST provide a task result detail view with core performance metrics, net value versus benchmark curve, and score diagnostics including IC/RankIC and quantile layer results when available.

#### Scenario: Open successful task detail
- **WHEN** the user opens a task detail with success status
- **THEN** the system displays metric cards, curve visualization, and diagnostics sections from backend result payload

#### Scenario: Open failed task detail
- **WHEN** the user opens a task detail with failed status
- **THEN** the system displays failure reason and actionable guidance for rerun

### Requirement: Parameter Replay and Rerun
The system MUST support rerun by copying parameters from an existing task into a new creation flow, while preserving historical task records as immutable.

#### Scenario: Rerun from historical task
- **WHEN** the user clicks rerun on a historical task
- **THEN** the system pre-fills the creation form with historical parameters and creates a new task on submit

#### Scenario: Preserve historical task snapshot
- **WHEN** a rerun is created from an existing task
- **THEN** the original task parameters and results remain unchanged and queryable
