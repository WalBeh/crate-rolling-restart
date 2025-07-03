"""
Maintenance window management for cluster restarts.

This module handles parsing maintenance window configurations from TOML files
and determining whether cluster operations should proceed based on current time
and configured maintenance windows.
"""

import re
import tomllib
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from calendar import monthrange
from dateutil.relativedelta import relativedelta

from pydantic import BaseModel, Field, validator


class WeekdayType(str, Enum):
    """Enumeration of weekday types."""
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
    
    # Short forms
    MON = "mon"
    TUE = "tue"
    WED = "wed"
    THU = "thu"
    FRI = "fri"
    SAT = "sat"
    SUN = "sun"


class MaintenanceWindow(BaseModel):
    """A single maintenance window definition."""
    
    start_time: time
    end_time: time
    weekdays: Optional[Set[str]] = None  # e.g., {"mon", "tue", "wed"}
    ordinal_days: Optional[List[str]] = None  # e.g., ["2nd tue", "last fri"]
    description: Optional[str] = None
    
    @validator('weekdays', pre=True)
    def normalize_weekdays(cls, v):
        """Normalize weekday names to lowercase."""
        if v is None:
            return None
        if isinstance(v, str):
            v = [day.strip() for day in v.split(',')]
        return {day.lower().strip() for day in v}
    
    @validator('ordinal_days', pre=True)
    def normalize_ordinal_days(cls, v):
        """Normalize ordinal day specifications."""
        if v is None:
            return None
        if isinstance(v, str):
            v = [day.strip() for day in v.split(',')]
        return [day.lower().strip() for day in v]


class ClusterMaintenanceConfig(BaseModel):
    """Maintenance configuration for a cluster."""
    
    cluster_name: str
    windows: List[MaintenanceWindow] = Field(default_factory=list)
    timezone: str = "UTC"
    min_window_duration: int = 30  # Minimum minutes needed for maintenance
    dc_util_timeout: int = 720  # Default timeout for dc_util in seconds
    min_availability: str = "PRIMARIES"  # PRIMARIES, NONE, or FULL


class MaintenanceWindowChecker:
    """Handles maintenance window logic and timing decisions."""
    
    WEEKDAY_MAP = {
        'monday': 0, 'mon': 0,
        'tuesday': 1, 'tue': 1,
        'wednesday': 2, 'wed': 2,
        'thursday': 3, 'thu': 3,
        'friday': 4, 'fri': 4,
        'saturday': 5, 'sat': 5,
        'sunday': 6, 'sun': 6,
    }
    
    ORDINAL_MAP = {
        '1st': 1, 'first': 1,
        '2nd': 2, 'second': 2,
        '3rd': 3, 'third': 3,
        '4th': 4, 'fourth': 4,
        '5th': 5, 'fifth': 5,
        'last': -1,
    }
    
    def __init__(self, config_path: Union[str, Path]):
        """Initialize with path to TOML configuration file."""
        self.config_path = Path(config_path)
        self._configs: Dict[str, ClusterMaintenanceConfig] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load maintenance window configuration from TOML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Maintenance config file not found: {self.config_path}")
        
        with open(self.config_path, 'rb') as f:
            data = tomllib.load(f)
        
        self._configs = {}
        
        for cluster_name, cluster_data in data.items():
            windows = []
            
            if 'windows' in cluster_data:
                for window_data in cluster_data['windows']:
                    # Parse time range
                    time_range = window_data.get('time', '')
                    start_str, end_str = self._parse_time_range(time_range)
                    
                    window = MaintenanceWindow(
                        start_time=self._parse_time(start_str),
                        end_time=self._parse_time(end_str),
                        weekdays=window_data.get('weekdays'),
                        ordinal_days=window_data.get('ordinal_days'),
                        description=window_data.get('description')
                    )
                    windows.append(window)
            
            config = ClusterMaintenanceConfig(
                cluster_name=cluster_name,
                windows=windows,
                timezone=cluster_data.get('timezone', 'UTC'),
                min_window_duration=cluster_data.get('min_window_duration', 30),
                dc_util_timeout=cluster_data.get('dc_util_timeout', 720),
                min_availability=cluster_data.get('min_availability', 'PRIMARIES')
            )
            
            self._configs[cluster_name] = config
    
    def _parse_time_range(self, time_range: str) -> Tuple[str, str]:
        """Parse time range string like '18:00-24:00' or '17:00-21:00'."""
        if '-' not in time_range:
            raise ValueError(f"Invalid time range format: {time_range}")
        
        start_str, end_str = time_range.split('-', 1)
        return start_str.strip(), end_str.strip()
    
    def _parse_time(self, time_str: str) -> time:
        """Parse time string like '18:00' or '24:00'."""
        time_str = time_str.strip()
        
        # Handle 24:00 as end of day
        if time_str == '24:00':
            return time(23, 59, 59)
        
        try:
            hour, minute = map(int, time_str.split(':'))
            return time(hour, minute)
        except ValueError:
            raise ValueError(f"Invalid time format: {time_str}")
    
    def get_cluster_config(self, cluster_name: str) -> Optional[ClusterMaintenanceConfig]:
        """Get maintenance configuration for a cluster."""
        return self._configs.get(cluster_name)
    
    def is_in_maintenance_window(
        self, 
        cluster_name: str, 
        check_time: Optional[datetime] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if current time is within a maintenance window.
        
        Args:
            cluster_name: Name of the cluster
            check_time: Time to check (defaults to current UTC time)
            
        Returns:
            Tuple of (is_in_window, reason)
        """
        if check_time is None:
            check_time = datetime.now(timezone.utc)
        
        config = self.get_cluster_config(cluster_name)
        if not config:
            return False, f"No maintenance configuration found for cluster '{cluster_name}'"
        
        if not config.windows:
            return False, f"No maintenance windows configured for cluster '{cluster_name}'"
        
        for i, window in enumerate(config.windows):
            if self._is_time_in_window(check_time, window):
                desc = window.description or f"window {i+1}"
                return True, f"Current time is within maintenance {desc} ({window.start_time}-{window.end_time})"
        
        return False, f"Current time is outside all maintenance windows for cluster '{cluster_name}'"
    
    def _is_time_in_window(self, check_time: datetime, window: MaintenanceWindow) -> bool:
        """Check if a specific time falls within a maintenance window."""
        current_time = check_time.time()
        current_weekday = check_time.weekday()
        
        # Check if time is within the window range
        if window.end_time <= window.start_time:
            # Window crosses midnight
            if not (current_time >= window.start_time or current_time <= window.end_time):
                return False
        else:
            # Normal window within same day
            if not (window.start_time <= current_time <= window.end_time):
                return False
        
        # Check weekday constraints
        if window.weekdays:
            weekday_names = [name for name, num in self.WEEKDAY_MAP.items() if num == current_weekday]
            if not any(day in window.weekdays for day in weekday_names):
                return False
        
        # Check ordinal day constraints (e.g., "2nd tue", "last fri")
        if window.ordinal_days:
            # For midnight-crossing windows, check both current day and previous day
            if window.end_time <= window.start_time and current_time <= window.end_time:
                # We're in the early hours of a midnight-crossing window
                # Check if the previous day matches the ordinal constraint
                previous_day = check_time - timedelta(days=1)
                if not self._matches_ordinal_days(previous_day, window.ordinal_days):
                    return False
            else:
                # Normal case - check current day
                if not self._matches_ordinal_days(check_time, window.ordinal_days):
                    return False
        
        return True
    
    def _matches_ordinal_days(self, check_time: datetime, ordinal_days: List[str]) -> bool:
        """Check if date matches ordinal day specifications like '2nd tue', 'last fri'."""
        for ordinal_day in ordinal_days:
            if self._matches_ordinal_day(check_time, ordinal_day):
                return True
        return False
    
    def _matches_ordinal_day(self, check_time: datetime, ordinal_day: str) -> bool:
        """Check if date matches a single ordinal day specification."""
        # Parse ordinal day specification
        pattern = r'^(\w+)\s+(\w+)$'
        match = re.match(pattern, ordinal_day.strip())
        if not match:
            return False
        
        ordinal_str, weekday_str = match.groups()
        ordinal_str = ordinal_str.lower()
        weekday_str = weekday_str.lower()
        
        if ordinal_str not in self.ORDINAL_MAP or weekday_str not in self.WEEKDAY_MAP:
            return False
        
        ordinal = self.ORDINAL_MAP[ordinal_str]
        target_weekday = self.WEEKDAY_MAP[weekday_str]
        
        return self._is_nth_weekday_of_month(check_time, target_weekday, ordinal)
    
    def _is_nth_weekday_of_month(self, check_time: datetime, target_weekday: int, ordinal: int) -> bool:
        """Check if date is the nth occurrence of a weekday in the month."""
        year, month = check_time.year, check_time.month
        
        if ordinal == -1:  # Last occurrence
            # Find the last day of the month
            last_day = monthrange(year, month)[1]
            last_date = datetime(year, month, last_day, tzinfo=check_time.tzinfo)
            
            # Go backwards to find the last occurrence of target_weekday
            for day in range(last_day, 0, -1):
                date = datetime(year, month, day, tzinfo=check_time.tzinfo)
                if date.weekday() == target_weekday:
                    return check_time.date() == date.date()
            return False
        
        else:  # nth occurrence (1st, 2nd, 3rd, etc.)
            # Find all occurrences of target_weekday in the month
            occurrences = []
            for day in range(1, monthrange(year, month)[1] + 1):
                date = datetime(year, month, day, tzinfo=check_time.tzinfo)
                if date.weekday() == target_weekday:
                    occurrences.append(date)
            
            if ordinal <= len(occurrences):
                return check_time.date() == occurrences[ordinal - 1].date()
            return False
    
    def get_next_maintenance_window(
        self, 
        cluster_name: str, 
        from_time: Optional[datetime] = None
    ) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Get the next maintenance window start time.
        
        Args:
            cluster_name: Name of the cluster
            from_time: Time to search from (defaults to current UTC time)
            
        Returns:
            Tuple of (next_window_start, reason)
        """
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        
        config = self.get_cluster_config(cluster_name)
        if not config or not config.windows:
            return None, f"No maintenance windows configured for cluster '{cluster_name}'"
        
        # Look ahead up to 35 days to find next window
        search_end = from_time + timedelta(days=35)
        current_time = from_time
        
        while current_time <= search_end:
            for i, window in enumerate(config.windows):
                window_start = self._get_window_start_on_date(current_time.date(), window)
                if window_start:
                    # Ensure both datetimes have the same timezone awareness for comparison
                    compare_from_time = from_time
                    if window_start.tzinfo is not None and compare_from_time.tzinfo is None:
                        # window_start is timezone-aware, from_time is naive - make from_time UTC
                        compare_from_time = from_time.replace(tzinfo=timezone.utc)
                    elif window_start.tzinfo is None and compare_from_time.tzinfo is not None:
                        # window_start is naive, from_time is timezone-aware - make window_start UTC
                        window_start = window_start.replace(tzinfo=timezone.utc)
                    
                    if window_start > compare_from_time:
                        desc = window.description or f"window {i+1}"
                        return window_start, f"Next maintenance {desc} starts at {window_start.strftime('%Y-%m-%d %H:%M UTC')}"
            
            current_time += timedelta(days=1)
        
        return None, f"No upcoming maintenance windows found for cluster '{cluster_name}' in the next 35 days"
    
    def _get_window_start_on_date(self, check_date, window: MaintenanceWindow) -> Optional[datetime]:
        """Get the start time of a maintenance window on a specific date, if applicable."""
        check_datetime = datetime.combine(check_date, window.start_time, tzinfo=timezone.utc)
        
        # Check if this date matches the window constraints
        if self._is_time_in_window(check_datetime, window):
            return check_datetime
        
        return None
    
    def should_wait_for_maintenance_window(
        self, 
        cluster_name: str, 
        current_time: Optional[datetime] = None
    ) -> Tuple[bool, str]:
        """
        Determine if restart should wait for maintenance window.
        
        Args:
            cluster_name: Name of the cluster
            current_time: Current time (defaults to UTC now)
            
        Returns:
            Tuple of (should_wait, reason)
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        config = self.get_cluster_config(cluster_name)
        if not config:
            return False, f"No maintenance configuration found for cluster '{cluster_name}' - proceeding without restrictions"
        
        if not config.windows:
            return False, f"No maintenance windows configured for cluster '{cluster_name}' - proceeding without restrictions"
        
        # Check if we're currently in a maintenance window
        in_window, window_reason = self.is_in_maintenance_window(cluster_name, current_time)
        if in_window:
            return False, f"Proceeding with restart: {window_reason}"
        
        # Check if next maintenance window is soon (within min_window_duration)
        next_window_start, next_reason = self.get_next_maintenance_window(cluster_name, current_time)
        if next_window_start:
            # Ensure both datetimes have the same timezone awareness for comparison
            compare_current_time = current_time
            if next_window_start.tzinfo is not None and compare_current_time.tzinfo is None:
                # next_window_start is timezone-aware, current_time is naive - make current_time UTC
                compare_current_time = current_time.replace(tzinfo=timezone.utc)
            elif next_window_start.tzinfo is None and compare_current_time.tzinfo is not None:
                # next_window_start is naive, current_time is timezone-aware - make next_window_start UTC
                next_window_start = next_window_start.replace(tzinfo=timezone.utc)
            
            time_until_window = next_window_start - compare_current_time
            min_duration = timedelta(minutes=config.min_window_duration)
            
            if time_until_window <= min_duration:
                return True, (
                    f"Waiting for upcoming maintenance window: {next_reason}. "
                    f"Less than {config.min_window_duration} minutes until window opens "
                    f"({time_until_window.total_seconds() / 60:.1f} minutes remaining)"
                )
        
        # If we have maintenance windows but none are active or upcoming soon
        if next_window_start:
            return True, (
                f"Waiting for next maintenance window: {next_reason}. "
                f"Current time is outside all maintenance windows"
            )
        else:
            return True, (
                f"Waiting indefinitely: No upcoming maintenance windows found for cluster '{cluster_name}' "
                f"in the next 35 days"
            )


def create_sample_config(output_path: Union[str, Path]) -> None:
    """Create a sample maintenance windows configuration file."""
    sample_config = '''# Maintenance Windows Configuration
# All times are in UTC

[aqua-darth-vader]
timezone = "UTC"
min_window_duration = 30  # Minimum minutes needed for maintenance

[[aqua-darth-vader.windows]]
time = "18:00-24:00"
weekdays = ["mon", "tue", "wed"]
description = "Evening maintenance window"

[[aqua-darth-vader.windows]]
time = "17:00-21:00"
ordinal_days = ["2nd tue", "3rd mon"]
description = "Monthly maintenance slots"

[tgw-x]
timezone = "UTC"
min_window_duration = 30

[[tgw-x.windows]]
time = "18:00-19:30"
weekdays = ["mon", "tue"]
description = "Short evening window"

[[tgw-x.windows]]
time = "20:00-22:00"
weekdays = ["thu", "fri"]
description = "Late evening window"

[production-cluster]
timezone = "UTC"
min_window_duration = 60

[[production-cluster.windows]]
time = "02:00-04:00"
weekdays = ["sat", "sun"]
description = "Weekend early morning maintenance"

[[production-cluster.windows]]
time = "23:00-01:00"
ordinal_days = ["last fri"]
description = "End of month maintenance"
'''
    
    with open(output_path, 'w') as f:
        f.write(sample_config)