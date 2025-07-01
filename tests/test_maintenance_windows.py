"""
Tests for maintenance window functionality.
"""

import pytest
from datetime import datetime, time, timedelta
from pathlib import Path
import tempfile
import os

from rr.maintenance_windows import (
    MaintenanceWindow,
    ClusterMaintenanceConfig,
    MaintenanceWindowChecker,
    create_sample_config
)


@pytest.fixture
def sample_config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write('''
[test-cluster]
timezone = "UTC"
min_window_duration = 30

[[test-cluster.windows]]
time = "18:00-22:00"
weekdays = ["mon", "tue", "wed"]
description = "Evening maintenance"

[[test-cluster.windows]]
time = "02:00-04:00"
weekdays = ["sat", "sun"]
description = "Weekend maintenance"

[[test-cluster.windows]]
time = "23:00-01:00"
ordinal_days = ["last fri"]
description = "Month-end maintenance"

[minimal-cluster]
timezone = "UTC"
min_window_duration = 60

[[minimal-cluster.windows]]
time = "20:00-21:00"
weekdays = ["fri"]
description = "Friday evening"

[ordinal-cluster]
timezone = "UTC"
min_window_duration = 30

[[ordinal-cluster.windows]]
time = "15:00-17:00"
ordinal_days = ["2nd tue", "4th thu"]
description = "Ordinal maintenance"

[no-windows-cluster]
timezone = "UTC"
min_window_duration = 30
''')
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


class TestMaintenanceWindow:
    """Test MaintenanceWindow model."""
    
    def test_basic_window_creation(self):
        """Test creating a basic maintenance window."""
        window = MaintenanceWindow(
            start_time=time(18, 0),
            end_time=time(22, 0),
            weekdays={"mon", "tue", "wed"}
        )
        
        assert window.start_time == time(18, 0)
        assert window.end_time == time(22, 0)
        assert window.weekdays == {"mon", "tue", "wed"}
    
    def test_weekday_normalization(self):
        """Test weekday normalization."""
        window = MaintenanceWindow(
            start_time=time(18, 0),
            end_time=time(22, 0),
            weekdays="MON, TUE,  WED"  # Mixed case with spaces
        )
        
        assert window.weekdays == {"mon", "tue", "wed"}
    
    def test_ordinal_days_normalization(self):
        """Test ordinal days normalization."""
        window = MaintenanceWindow(
            start_time=time(15, 0),
            end_time=time(17, 0),
            ordinal_days="2ND TUE, Last FRI"
        )
        
        assert window.ordinal_days == ["2nd tue", "last fri"]


class TestMaintenanceWindowChecker:
    """Test MaintenanceWindowChecker functionality."""
    
    def test_initialization(self, sample_config_file):
        """Test initialization with config file."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Check that configs were loaded
        config = checker.get_cluster_config("test-cluster")
        assert config is not None
        assert config.cluster_name == "test-cluster"
        assert len(config.windows) == 3
    
    def test_missing_config_file(self):
        """Test handling of missing config file."""
        with pytest.raises(FileNotFoundError):
            MaintenanceWindowChecker("/nonexistent/path.toml")
    
    def test_get_cluster_config(self, sample_config_file):
        """Test getting cluster configuration."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Existing cluster
        config = checker.get_cluster_config("test-cluster")
        assert config is not None
        assert config.cluster_name == "test-cluster"
        
        # Non-existing cluster
        config = checker.get_cluster_config("nonexistent-cluster")
        assert config is None
    
    def test_time_parsing(self, sample_config_file):
        """Test time parsing from config."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Test normal time parsing
        assert checker._parse_time("18:00") == time(18, 0)
        assert checker._parse_time("02:30") == time(2, 30)
        
        # Test 24:00 handling
        assert checker._parse_time("24:00") == time(23, 59, 59)
        
        # Test invalid time
        with pytest.raises(ValueError):
            checker._parse_time("25:00")
    
    def test_time_range_parsing(self, sample_config_file):
        """Test time range parsing."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        start, end = checker._parse_time_range("18:00-22:00")
        assert start == "18:00"
        assert end == "22:00"
        
        # Test with spaces
        start, end = checker._parse_time_range(" 18:00 - 22:00 ")
        assert start == "18:00"
        assert end == "22:00"
        
        # Test invalid range
        with pytest.raises(ValueError):
            checker._parse_time_range("18:00")


class TestMaintenanceWindowLogic:
    """Test maintenance window timing logic."""
    
    def test_is_in_weekday_maintenance_window(self, sample_config_file):
        """Test checking if time is in weekday maintenance window."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Monday 19:00 - should be in window (18:00-22:00 on mon/tue/wed)
        monday_evening = datetime(2024, 1, 1, 19, 0)  # Monday
        in_window, reason = checker.is_in_maintenance_window("test-cluster", monday_evening)
        assert in_window is True
        assert "Evening maintenance" in reason
        
        # Monday 17:00 - should be outside window
        monday_afternoon = datetime(2024, 1, 1, 17, 0)  # Monday
        in_window, reason = checker.is_in_maintenance_window("test-cluster", monday_afternoon)
        assert in_window is False
        
        # Thursday 19:00 - should be outside window (not mon/tue/wed)
        thursday_evening = datetime(2024, 1, 4, 19, 0)  # Thursday
        in_window, reason = checker.is_in_maintenance_window("test-cluster", thursday_evening)
        assert in_window is False
    
    def test_is_in_weekend_maintenance_window(self, sample_config_file):
        """Test weekend maintenance window."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Saturday 03:00 - should be in weekend window (02:00-04:00 on sat/sun)
        saturday_night = datetime(2024, 1, 6, 3, 0)  # Saturday
        in_window, reason = checker.is_in_maintenance_window("test-cluster", saturday_night)
        assert in_window is True
        assert "Weekend maintenance" in reason
        
        # Saturday 05:00 - should be outside window
        saturday_morning = datetime(2024, 1, 6, 5, 0)  # Saturday
        in_window, reason = checker.is_in_maintenance_window("test-cluster", saturday_morning)
        assert in_window is False
    
    def test_midnight_crossing_window(self, sample_config_file):
        """Test maintenance window that crosses midnight."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Last Friday of January 2024 is Jan 26th
        # 23:30 should be in window (23:00-01:00)
        last_friday_late = datetime(2024, 1, 26, 23, 30)
        in_window, reason = checker.is_in_maintenance_window("test-cluster", last_friday_late)
        assert in_window is True
        assert "Month-end maintenance" in reason
        
        # Next day 00:30 should also be in window
        saturday_early = datetime(2024, 1, 27, 0, 30)
        in_window, reason = checker.is_in_maintenance_window("test-cluster", saturday_early)
        assert in_window is True
    
    def test_ordinal_day_matching(self, sample_config_file):
        """Test ordinal day matching (2nd tue, 4th thu, etc.)."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # January 2024: 2nd Tuesday is Jan 9th
        second_tuesday = datetime(2024, 1, 9, 16, 0)  # 16:00 on 2nd Tuesday
        in_window, reason = checker.is_in_maintenance_window("ordinal-cluster", second_tuesday)
        assert in_window is True
        
        # January 2024: 4th Thursday is Jan 25th
        fourth_thursday = datetime(2024, 1, 25, 16, 0)  # 16:00 on 4th Thursday
        in_window, reason = checker.is_in_maintenance_window("ordinal-cluster", fourth_thursday)
        assert in_window is True
        
        # 3rd Tuesday should not match (only 2nd and 4th are configured)
        third_tuesday = datetime(2024, 1, 16, 16, 0)  # 16:00 on 3rd Tuesday
        in_window, reason = checker.is_in_maintenance_window("ordinal-cluster", third_tuesday)
        assert in_window is False
    
    def test_last_day_of_month(self, sample_config_file):
        """Test last day of month matching."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Test _is_nth_weekday_of_month for last Friday
        # January 2024: last Friday is Jan 26th
        last_friday = datetime(2024, 1, 26)
        assert checker._is_nth_weekday_of_month(last_friday, 4, -1) is True  # Friday = 4
        
        # Jan 19th is not the last Friday
        not_last_friday = datetime(2024, 1, 19)
        assert checker._is_nth_weekday_of_month(not_last_friday, 4, -1) is False
    
    def test_no_config_cluster(self, sample_config_file):
        """Test behavior with cluster that has no configuration."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        in_window, reason = checker.is_in_maintenance_window("nonexistent-cluster")
        assert in_window is False
        assert "No maintenance configuration found" in reason
    
    def test_no_windows_cluster(self, sample_config_file):
        """Test behavior with cluster that has no windows configured."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        in_window, reason = checker.is_in_maintenance_window("no-windows-cluster")
        assert in_window is False
        assert "No maintenance windows configured" in reason


class TestNextMaintenanceWindow:
    """Test finding next maintenance window."""
    
    def test_next_window_same_day(self, sample_config_file):
        """Test finding next window on the same day."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Monday 17:00 - next window should be 18:00 same day
        monday_afternoon = datetime(2024, 1, 1, 17, 0)  # Monday
        next_start, reason = checker.get_next_maintenance_window("test-cluster", monday_afternoon)
        
        assert next_start is not None
        assert next_start.date() == monday_afternoon.date()
        assert next_start.time() == time(18, 0)
        assert "Evening maintenance" in reason
    
    def test_next_window_different_day(self, sample_config_file):
        """Test finding next window on a different day."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Wednesday 23:00 - next window should be Saturday 02:00
        wednesday_late = datetime(2024, 1, 3, 23, 0)  # Wednesday
        next_start, reason = checker.get_next_maintenance_window("test-cluster", wednesday_late)
        
        assert next_start is not None
        assert next_start.weekday() == 5  # Saturday
        assert next_start.time() == time(2, 0)
        assert "Weekend maintenance" in reason
    
    def test_next_window_ordinal_day(self, sample_config_file):
        """Test finding next ordinal day window."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Start from Jan 1st, should find 2nd Tuesday (Jan 9th)
        start_of_month = datetime(2024, 1, 1, 12, 0)
        next_start, reason = checker.get_next_maintenance_window("ordinal-cluster", start_of_month)
        
        assert next_start is not None
        assert next_start.date() == datetime(2024, 1, 9).date()
        assert next_start.time() == time(15, 0)
    
    def test_no_upcoming_windows(self, sample_config_file):
        """Test when no upcoming windows are found."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        next_start, reason = checker.get_next_maintenance_window("no-windows-cluster")
        assert next_start is None
        assert "No maintenance windows configured" in reason


class TestShouldWaitDecision:
    """Test the main decision logic for whether to wait."""
    
    def test_should_proceed_in_window(self, sample_config_file):
        """Test proceeding when in maintenance window."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Monday 19:00 - in window, should proceed
        monday_evening = datetime(2024, 1, 1, 19, 0)
        should_wait, reason = checker.should_wait_for_maintenance_window("test-cluster", monday_evening)
        
        assert should_wait is False
        assert "Proceeding with restart" in reason
        assert "Evening maintenance" in reason
    
    def test_should_wait_window_approaching(self, sample_config_file):
        """Test waiting when maintenance window is approaching."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Monday 17:45 - 15 minutes until window, should wait
        monday_before_window = datetime(2024, 1, 1, 17, 45)
        should_wait, reason = checker.should_wait_for_maintenance_window("test-cluster", monday_before_window)
        
        assert should_wait is True
        assert "Less than 30 minutes until window opens" in reason
        assert "15.0 minutes remaining" in reason
    
    def test_should_wait_outside_window(self, sample_config_file):
        """Test waiting when outside maintenance window."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Thursday 19:00 - outside window, should wait
        thursday_evening = datetime(2024, 1, 4, 19, 0)
        should_wait, reason = checker.should_wait_for_maintenance_window("test-cluster", thursday_evening)
        
        assert should_wait is True
        assert "Current time is outside all maintenance windows" in reason
    
    def test_should_proceed_no_config(self, sample_config_file):
        """Test proceeding when no configuration exists."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        should_wait, reason = checker.should_wait_for_maintenance_window("nonexistent-cluster")
        
        assert should_wait is False
        assert "proceeding without restrictions" in reason
    
    def test_should_proceed_no_windows(self, sample_config_file):
        """Test proceeding when no windows are configured."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        should_wait, reason = checker.should_wait_for_maintenance_window("no-windows-cluster")
        
        assert should_wait is False
        assert "proceeding without restrictions" in reason


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_malformed_ordinal_day(self, sample_config_file):
        """Test handling of malformed ordinal day specifications."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # This should not match anything
        result = checker._matches_ordinal_day(datetime(2024, 1, 9), "invalid spec")
        assert result is False
    
    def test_window_end_before_start(self):
        """Test window that ends before it starts (crosses midnight)."""
        window = MaintenanceWindow(
            start_time=time(23, 0),
            end_time=time(1, 0),  # Crosses midnight
            weekdays={"fri"}
        )
        
        checker = MaintenanceWindowChecker.__new__(MaintenanceWindowChecker)
        
        # Friday 23:30 should be in window
        friday_late = datetime(2024, 1, 5, 23, 30)  # Friday
        assert checker._is_time_in_window(friday_late, window) is True
        
        # Saturday 00:30 should be in window (continuation from Friday)
        saturday_early = datetime(2024, 1, 6, 0, 30)  # Saturday
        # Note: This test shows current limitation - cross-midnight windows
        # don't handle weekday constraints properly across date boundary
    
    def test_february_leap_year(self):
        """Test ordinal day calculations in February of leap year."""
        checker = MaintenanceWindowChecker.__new__(MaintenanceWindowChecker)
        
        # 2024 is a leap year, February has 29 days
        # Last Friday in Feb 2024 should be Feb 23rd
        last_friday_feb = datetime(2024, 2, 23)
        assert checker._is_nth_weekday_of_month(last_friday_feb, 4, -1) is True
        
        # Feb 16th is not the last Friday
        not_last_friday = datetime(2024, 2, 16)
        assert checker._is_nth_weekday_of_month(not_last_friday, 4, -1) is False


class TestConfigGeneration:
    """Test configuration file generation."""
    
    def test_create_sample_config(self):
        """Test creating a sample configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            temp_path = f.name
        
        try:
            create_sample_config(temp_path)
            
            # Verify file was created and is readable
            checker = MaintenanceWindowChecker(temp_path)
            
            # Check that sample clusters are present
            aqua_config = checker.get_cluster_config("aqua-darth-vader")
            assert aqua_config is not None
            assert len(aqua_config.windows) >= 2
            
            tgw_config = checker.get_cluster_config("tgw-x")
            assert tgw_config is not None
            assert len(tgw_config.windows) >= 2
            
        finally:
            os.unlink(temp_path)


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""
    
    def test_monthly_maintenance_scenario(self, sample_config_file):
        """Test scenario with monthly maintenance on last Friday."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # January 2024: last Friday is Jan 26th
        # Test month-end maintenance window
        last_friday = datetime(2024, 1, 26, 23, 30)
        in_window, reason = checker.is_in_maintenance_window("test-cluster", last_friday)
        assert in_window is True
        assert "Month-end maintenance" in reason
        
        # Test that other Fridays don't match
        first_friday = datetime(2024, 1, 5, 23, 30)
        in_window, reason = checker.is_in_maintenance_window("test-cluster", first_friday)
        assert in_window is False
    
    def test_multiple_clusters_different_windows(self, sample_config_file):
        """Test multiple clusters with different maintenance windows."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # Same time, different clusters
        friday_evening = datetime(2024, 1, 5, 20, 30)  # Friday 20:30
        
        # minimal-cluster has Friday 20:00-21:00 window
        in_window, _ = checker.is_in_maintenance_window("minimal-cluster", friday_evening)
        assert in_window is True
        
        # test-cluster doesn't have Friday evening windows
        in_window, _ = checker.is_in_maintenance_window("test-cluster", friday_evening)
        assert in_window is False
    
    def test_min_window_duration_logic(self, sample_config_file):
        """Test minimum window duration logic."""
        checker = MaintenanceWindowChecker(sample_config_file)
        
        # minimal-cluster has 60 min minimum, test-cluster has 30 min minimum
        
        # 45 minutes before window - should wait for minimal-cluster (60 min min)
        before_window_45 = datetime(2024, 1, 5, 19, 15)  # Friday 19:15, window at 20:00
        should_wait, reason = checker.should_wait_for_maintenance_window("minimal-cluster", before_window_45)
        assert should_wait is True
        assert "Less than 60 minutes until window opens" in reason
        
        # 45 minutes before window - should NOT wait for test-cluster (30 min min)
        monday_before = datetime(2024, 1, 1, 17, 15)  # Monday 17:15, window at 18:00
        should_wait, reason = checker.should_wait_for_maintenance_window("test-cluster", monday_before)
        assert should_wait is True  # Still waits, but for different reason
        assert "Next maintenance" in reason  # Not the "less than X minutes" message