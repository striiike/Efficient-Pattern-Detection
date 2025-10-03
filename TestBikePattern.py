"""
Test suite for bike hot path pattern detection.

This module tests the bike trip hot path detection patterns using the OpenCEP framework.
It imports the pattern definitions from bike_hot_path_pattern.py and tests them with
synthetic and real data.
"""

from AnalyzeData import analyze_bike_data
from test.testUtils import DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS
from stream.FileStream import FileOutputStream
from CEP import CEP
from bike.BikeStream import BikeCSVInputStream, TestBikeInputStream  # Import from bike_stream.py
from bike.BikeHotPathPattern import (
    create_bike_hot_path_pattern,
    get_pattern_info
)
from bike.BikeData import BikeDataFormatter
from datetime import datetime
import os
import sys
sys.path.append(os.path.dirname(__file__))


def test_pattern(pattern, pattern_name):
    """
    Test a specific pattern with given test data.

    Args:
        pattern: The pattern to test
        pattern_name: Name for reporting
        test_data_type: Type of test data to use

    Returns:
        tuple: (number_of_matches, execution_time)
    """


    print(f"\n{'='*60}")
    print(f"TESTING: {pattern_name}")
    print(f"{'='*60}")

    input_stream = TestBikeInputStream()

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(output_dir, exist_ok=True)

    safe_name = pattern_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
    output_file = f"{safe_name}_matches.txt"
    output_stream = FileOutputStream(output_dir, output_file)

    data_formatter = BikeDataFormatter()

    cep_engine = CEP(
        patterns=[pattern],
        eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS
    )

    print(f"Pattern structure: {pattern.full_structure}")
    print(f"Time window: {pattern.window}")
    print("Running pattern detection...")

    # Run detection
    execution_time = cep_engine.run(input_stream, output_stream, data_formatter)

    # Check results
    output_file_path = os.path.join(output_dir, output_file)
    match_count = 0

    if os.path.exists(output_file_path):
        with open(output_file_path, 'r') as f:
            content = f.read().strip()

        if content:
            # Split by double newlines to get groups (pattern matches)
            groups = [group.strip()
                      for group in content.split('\n\n') if group.strip()]
            match_count = len(groups)
            print(
                f"✅ Found {match_count} pattern matches in {execution_time:.3f}s")

            # Show first few pattern matches with their events
            for i, group in enumerate(groups[:3], 1):
                events = [event.strip()
                          for event in group.split('\n') if event.strip()]
                print(f"   Pattern Match {i} ({len(events)} events):")
                for j, event in enumerate(events):
                    print(f"     Event {j}: {event}")


        else:
            print("❌ No matches found")
    else:
        print("❌ Output file not created")

    return match_count, execution_time


def analyze_real_data(file_path: str, max_events: int = 20):
    """
    Analyze real bike trip data for hot path patterns.
    
    Args:
        file_path: Path to the CSV file
        max_events: Maximum number of events to process
    """
    print("=" * 60)
    print(f"BIKE HOT PATH ANALYSIS - REAL DATA (First {max_events} events)")
    print("=" * 60)
    print(f"Data source: {file_path}")
    print(f"Processing first {max_events} events only")
    print()
    
    top_end_stations = analyze_bike_data(file_path=file_path, max_lines=max_events, top_count=3)['top_end_stations']
    top_end_stations = {station for station, count in top_end_stations}
    # top_end_stations = {426, 3002, 462}
    pattern = create_bike_hot_path_pattern(target_stations=top_end_stations, time_window_hours=1)

    data_formatter = BikeDataFormatter()
    
    cep = CEP([pattern])

    output_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(output_dir, exist_ok=True)

    safe_name = "real_data".lower().replace(" ", "_").replace("(", "").replace(")", "")
    output_file = f"{safe_name}_matches.txt"
    output_file_path = os.path.join(output_dir, output_file)

    input_stream = BikeCSVInputStream(file_path, max_events=max_events)
    output_stream = FileOutputStream(output_dir, output_file)


    execution_time = cep.run(input_stream, output_stream, data_formatter)

    if os.path.exists(output_file_path):
        with open(output_file_path, 'r') as f:
            content = f.read().strip()

        if content:
            # Split by double newlines to get groups (pattern matches)
            groups = [group.strip() for group in content.split('\n\n') if group.strip()]
            match_count = len(groups)
            print(f"✅ Found {match_count} pattern matches in {execution_time:.3f}s")

            # Show first few pattern matches with their events
            for i, group in enumerate(groups[:3], 1):
                events = [event.strip() for event in group.split('\n') if event.strip()]
                print(f"   Pattern Match {i} ({len(events)} events):")
                for j, event in enumerate(events):
                    print(f"     Event {j}: {event}")
        



if __name__ == "__main__":
    # pattern = create_bike_hot_path_pattern(target_stations={426, 3002, 462}, time_window_hours=1)
    # test_pattern(pattern, "Bike Hot Path (Kleene Closure)")

    analyze_real_data("data/201804-citibike-tripdata_2.csv", max_events=1000)