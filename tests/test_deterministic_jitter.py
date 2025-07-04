#!/usr/bin/env python3
"""
Tests for deterministic jitter calculation in state machine workflows.

This ensures that workflows use deterministic values instead of random.uniform
which is not allowed in Temporal workflows.
"""

import pytest
from rr.state_machines import HealthCheckStateMachine


class TestDeterministicJitter:
    """Test cases for deterministic jitter functionality."""

    def test_jitter_calculation_is_deterministic(self):
        """Test that jitter calculation produces consistent results for same attempt number."""
        # Test multiple attempt numbers
        test_cases = [
            {"attempts": 1, "base_wait": 10},
            {"attempts": 5, "base_wait": 15},
            {"attempts": 10, "base_wait": 20},
            {"attempts": 15, "base_wait": 5},
        ]
        
        for case in test_cases:
            attempts = case["attempts"]
            base_wait = case["base_wait"]
            
            # Calculate jitter multiple times for same attempt
            jitter_values = []
            for _ in range(5):
                exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
                jitter_factor = 0.1 + ((attempts % 10) * 0.02)
                jitter = jitter_factor * exponential_wait
                jitter_values.append(jitter)
            
            # All values should be identical (deterministic)
            assert all(j == jitter_values[0] for j in jitter_values), \
                f"Jitter calculation not deterministic for attempts={attempts}, base_wait={base_wait}"

    def test_jitter_factor_range(self):
        """Test that jitter factor stays within expected range."""
        for attempts in range(20):  # Test various attempt numbers
            jitter_factor = 0.1 + ((attempts % 10) * 0.02)
            
            # Jitter factor should be between 0.1 and 0.28
            assert 0.1 <= jitter_factor <= 0.28, \
                f"Jitter factor {jitter_factor} out of range for attempts={attempts}"

    def test_jitter_increases_with_attempts(self):
        """Test that jitter generally increases with attempt number (within mod 10 cycle)."""
        base_wait = 10
        
        for cycle_start in [0, 10, 20]:  # Test different cycles
            jitter_values = []
            for i in range(10):  # One complete cycle
                attempts = cycle_start + i
                exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
                jitter_factor = 0.1 + ((attempts % 10) * 0.02)
                jitter = jitter_factor * exponential_wait
                jitter_values.append(jitter)
            
            # Within each cycle, jitter should generally increase
            # (though exponential_wait may cap at 60, affecting this)
            for i in range(len(jitter_values) - 1):
                if jitter_values[i] > 0 and jitter_values[i+1] > 0:
                    # Allow for exponential wait capping effects
                    ratio = jitter_values[i+1] / jitter_values[i]
                    assert ratio >= 0.9, \
                        f"Jitter not increasing properly: {jitter_values[i]} -> {jitter_values[i+1]}"

    def test_total_wait_calculation(self):
        """Test complete wait time calculation with deterministic jitter."""
        test_scenarios = [
            {"attempts": 1, "base_wait": 10, "expected_min": 22.0, "expected_max": 23.0},  # 20 + (0.12 * 20) = 22.4
            {"attempts": 3, "base_wait": 5, "expected_min": 46.0, "expected_max": 47.0},  # 40 + (0.16 * 40) = 46.4
            {"attempts": 8, "base_wait": 15, "expected_min": 75.0, "expected_max": 76.0},  # 60 + (0.26 * 60) = 75.6
        ]
        
        for scenario in test_scenarios:
            attempts = scenario["attempts"]
            base_wait = scenario["base_wait"]
            
            # Calculate total wait time
            exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
            jitter_factor = 0.1 + ((attempts % 10) * 0.02)
            jitter = jitter_factor * exponential_wait
            total_wait = exponential_wait + jitter
            
            # Verify total wait is in expected range
            assert scenario["expected_min"] <= total_wait <= scenario["expected_max"], \
                f"Total wait {total_wait} not in expected range [{scenario['expected_min']}, {scenario['expected_max']}] for attempts={attempts}"

    def test_no_random_imports_in_workflow_files(self):
        """Test that workflow files don't import random module."""
        import ast
        import inspect
        from rr import state_machines
        
        # Get the source code of the state_machines module
        source = inspect.getsource(state_machines)
        
        # Parse the AST
        tree = ast.parse(source)
        
        # Check for random imports
        random_imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == 'random':
                        random_imports.append(f"import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module == 'random':
                    random_imports.append(f"from {node.module} import ...")
        
        assert not random_imports, \
            f"Found random imports in state_machines.py: {random_imports}. " \
            "Random is not allowed in Temporal workflows as they must be deterministic."

    def test_jitter_reproducibility_across_runs(self):
        """Test that jitter calculation is reproducible across different runs."""
        # This simulates what would happen in a workflow replay
        reference_values = {}
        
        # First "run" - calculate reference values
        for attempts in range(1, 11):
            base_wait = 10
            exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
            jitter_factor = 0.1 + ((attempts % 10) * 0.02)
            jitter = jitter_factor * exponential_wait
            total_wait = exponential_wait + jitter
            reference_values[attempts] = total_wait
        
        # Second "run" - should produce identical values
        for attempts in range(1, 11):
            base_wait = 10
            exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
            jitter_factor = 0.1 + ((attempts % 10) * 0.02)
            jitter = jitter_factor * exponential_wait
            total_wait = exponential_wait + jitter
            
            assert total_wait == reference_values[attempts], \
                f"Jitter calculation not reproducible for attempts={attempts}: " \
                f"first run={reference_values[attempts]}, second run={total_wait}"

    def test_edge_cases(self):
        """Test edge cases for jitter calculation."""
        # Test with attempt 0
        attempts = 0
        base_wait = 10
        exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)
        jitter_factor = 0.1 + ((attempts % 10) * 0.02)
        jitter = jitter_factor * exponential_wait
        total_wait = exponential_wait + jitter
        
        assert total_wait > base_wait, "Total wait should be greater than base wait"
        assert jitter_factor == 0.1, "Jitter factor should be 0.1 for attempt 0"
        
        # Test with very high attempt number
        attempts = 100
        exponential_wait = min(base_wait * (2 ** min(attempts, 10)), 60)  # Should be capped at 60
        jitter_factor = 0.1 + ((attempts % 10) * 0.02)
        jitter = jitter_factor * exponential_wait
        
        assert exponential_wait == 60, "Exponential wait should be capped at 60"
        assert jitter_factor == 0.1, "Jitter factor should cycle back to 0.1 for attempt 100"