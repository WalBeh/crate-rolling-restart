# Maintenance Windows Feature Implementation

## Overview

This document describes the implementation of the maintenance windows feature for the CrateDB Kubernetes Cluster Manager. This feature provides automated scheduling and control over when cluster restarts can occur, ensuring they only happen during designated maintenance periods.

## Key Features Implemented

### 1. Flexible Scheduling System
- **Weekday-based windows**: Configure maintenance windows for specific days of the week
- **Ordinal day patterns**: Support for complex patterns like "2nd Tuesday", "last Friday"
- **Time ranges**: 24-hour UTC time format with support for midnight-crossing windows
- **Multiple windows per cluster**: Each cluster can have multiple independent maintenance windows

### 2. Configuration Management
- **TOML-based configuration**: Human-readable configuration format
- **Validation and parsing**: Robust parsing with comprehensive error handling
- **Sample generation**: Built-in command to create example configurations

### 3. Decision Logic
- **Current window detection**: Determines if current time falls within maintenance window
- **Wait logic**: Automatically waits for next maintenance window if outside current windows
- **Minimum duration check**: Prevents restarts when insufficient time remains in window
- **Next window calculation**: Finds upcoming maintenance windows up to 35 days ahead

### 4. Operator Override
- **CLI command**: `force-restart` command for emergency overrides
- **Temporal signals**: Emergency override capability via Temporal UI or CLI
- **Signal handling**: Workflows can be forced to proceed outside maintenance windows
- **Audit trail**: All override actions are logged with reasons

### 5. CLI Integration
- **Subcommands**: Dedicated `maintenance` command group for window management
- **Status checking**: Real-time maintenance window status checking
- **Configuration listing**: Display all configured windows in tabular format
- **Integration with restart**: Seamless integration with existing restart workflows

## Architecture

### Core Components

#### MaintenanceWindow Model
```python
class MaintenanceWindow(BaseModel):
    start_time: time
    end_time: time
    weekdays: Optional[Set[str]] = None
    ordinal_days: Optional[List[str]] = None
    description: Optional[str] = None
```

#### MaintenanceWindowChecker
- Central class handling all maintenance window logic
- Parses TOML configuration files
- Implements decision algorithms for window matching
- Handles complex date calculations for ordinal patterns

#### Temporal Integration
- New activity: `check_maintenance_window`
- Enhanced workflow with maintenance window checking
- Signal handlers for operator override
- Periodic checking during wait periods

### Workflow Integration

1. **Pre-restart Check**: Before any restart operation, check maintenance windows
2. **Wait Loop**: If outside window, enter wait loop with 5-minute check intervals
3. **Signal Handling**: Listen for operator override signals during wait
4. **Proceed Logic**: Continue with restart when in window or override received

## Configuration Format

### Basic Structure
```toml
[cluster-name]
timezone = "UTC"
min_window_duration = 30  # Minimum minutes needed

[[cluster-name.windows]]
time = "18:00-22:00"
weekdays = ["mon", "tue", "wed"]
description = "Evening maintenance"
```

### Supported Patterns

#### Weekday Patterns
- Full names: `["monday", "tuesday"]`
- Short names: `["mon", "tue"]`
- Mixed case supported with normalization

#### Ordinal Day Patterns
- Numbered: `["1st mon", "2nd tue", "3rd wed", "4th thu", "5th fri"]`
- Word form: `["first mon", "second tue", "third wed"]`
- Last occurrence: `["last fri", "last mon"]`

#### Time Formats
- Standard: `"09:00-17:00"`
- Midnight crossing: `"23:00-01:00"`
- End of day: `"18:00-24:00"` (converted to 23:59:59)

## CLI Commands

### Maintenance Management
```bash
# Create sample configuration
rr maintenance create-config -o config.toml

# Check maintenance window status
rr maintenance check config.toml cluster-name

# Check at specific time
rr maintenance check config.toml cluster-name --time "2024-01-15T19:30:00"

# List all configured windows
rr maintenance list-windows config.toml
```

### Restart with Maintenance Windows
```bash
# Use maintenance windows
rr restart --context prod --maintenance-config config.toml cluster-name

# Ignore maintenance windows (emergency)
rr restart --context prod --ignore-maintenance-windows cluster-name

# Dry run with maintenance check
rr restart --context prod --maintenance-config config.toml --dry-run cluster-name
```

### Force Restart (Override Maintenance Windows)
```bash
# Find running workflows
rr list-workflows

# Force restart with default reason
rr force-restart <workflow-id>

# Force restart with custom reason
rr force-restart <workflow-id> --reason "Emergency security patch"

# Example with actual workflow ID
rr force-restart restart-cluster-name-2024-01-15T10:00:00+00:00 --reason "Critical issue"
```

## Decision Logic

### Window Matching Algorithm
1. **Time Range Check**: Verify current time falls within start-end range
2. **Weekday Validation**: Match current weekday against configured weekdays
3. **Ordinal Day Validation**: Complex matching for ordinal patterns
4. **Midnight Crossing**: Special handling for windows crossing midnight

### Wait Decision Matrix
| Condition | Action | Reason |
|-----------|--------|---------|
| In maintenance window | Proceed | Current time within configured window |
| Outside window, no upcoming | Wait indefinitely | No future windows found |
| Upcoming window < min_duration | Wait for window | Insufficient time remaining |
| Outside window, future available | Wait for next | Schedule adherence |
| No configuration | Proceed | No restrictions configured |
| Configuration error | Proceed | Fail-safe behavior |

## Error Handling

### Robust Failure Modes
- **Missing config file**: Proceeds without restrictions + warning
- **Invalid TOML syntax**: Clear error messages with line numbers
- **Invalid time formats**: Detailed validation errors
- **Malformed ordinal patterns**: Graceful fallback behavior
- **Activity failures**: Fail-safe to proceed with restart

### Logging Strategy
- **Info level**: Normal operation decisions and window status
- **Warning level**: Configuration issues and fallback behavior
- **Error level**: Critical failures with context
- **Debug level**: Detailed decision logic and calculations

## Testing

### Comprehensive Test Suite
- **31 test cases** covering all functionality
- **Unit tests**: Individual component testing
- **Integration tests**: End-to-end workflow testing
- **Edge cases**: Leap years, month boundaries, timezone handling
- **Error conditions**: Invalid configurations and malformed data

### Test Categories
1. **Model validation**: Pydantic model behavior
2. **Configuration parsing**: TOML parsing and validation
3. **Time logic**: Window matching algorithms
4. **Ordinal calculations**: Complex date pattern matching
5. **Decision logic**: Wait/proceed decision making
6. **CLI integration**: Command-line interface testing

## Performance Considerations

### Efficient Operations
- **Configuration caching**: Load once, use multiple times
- **Minimal date calculations**: Optimized ordinal day matching
- **Bounded searches**: 35-day lookahead limit for next windows
- **Fast weekday matching**: Efficient set-based lookups

### Memory Usage
- **Lazy loading**: Configuration loaded on first use
- **Minimal state**: Stateless checker design
- **Garbage collection friendly**: No long-lived objects

## Security Considerations

### Safe Defaults
- **Fail-safe behavior**: Proceed with restart on configuration errors
- **No sensitive data**: Configuration contains only scheduling information
- **UTC timezone**: Consistent time handling across environments
- **Input validation**: All user inputs validated and sanitized

## Future Enhancements

### Potential Improvements
1. **Timezone support**: Per-cluster timezone configuration
2. **Holiday calendars**: Integration with business calendar systems
3. **Dynamic windows**: API-driven maintenance window updates
4. **Notification integration**: Slack/email notifications for window events
5. **Metrics collection**: Prometheus metrics for maintenance window usage
6. **Window conflicts**: Detection and resolution of overlapping windows

### Backward Compatibility
- All existing functionality preserved
- New parameters are optional
- Graceful degradation when feature not used
- Clear migration path for adoption

## Summary

The maintenance windows feature provides a robust, flexible, and user-friendly system for controlling when cluster restarts can occur. It balances operational safety with emergency override capabilities, ensuring that production systems can be maintained predictably while still allowing for urgent interventions when necessary.

Key benefits:
- **Operational control**: Precise scheduling of disruptive operations
- **Emergency flexibility**: CLI-based and UI-based override capabilities for urgent situations
- **User-friendly**: Intuitive TOML configuration and clear CLI commands
- **Robust implementation**: Comprehensive error handling and testing
- **Production ready**: Fail-safe behavior and comprehensive logging
- **Audit trail**: All override actions logged with reasons for compliance