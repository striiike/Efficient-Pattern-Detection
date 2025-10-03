"""
Simple timing test for OpenCEP event processing detection.
Shows exactly when each event starts being processed and the gaps from system start.
Uses the enhanced BikeStream classes with integrated timing capabilities.
"""

import time
from datetime import datetime
from CEP import CEP
from bike.BikeStream import BikeCSVInputStream, TimingBikeOutputStream, TimingBikeInputStream
from bike.BikeData import BikeDataFormatter
from bike.BikeHotPathPattern import create_bike_hot_path_pattern
from test.testUtils import DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS
from stream.FileStream import FileOutputStream
import os


def run_simple_timing_test():
    """Main timing test function using enhanced BikeStream classes."""
    print("🔍 OpenCEP Event Processing Timing Test")
    print("=" * 70)
    print("Purpose: Detect when OpenCEP starts processing each input event")
    print("Data: Synthetic bike trip data from BikeCSVInputStream")
    print()
    
    # Create pattern for bike trips ending at station 426
    pattern = create_bike_hot_path_pattern(target_stations={426}, time_window_hours=1)
    print("Pattern: Detecting bike hot paths ending at station 426 (1-hour window)")
    print()
    
    input_stream = TimingBikeInputStream(
        file_path=None,  # Use test data
        use_test_data=True
    )
    
    output_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(output_dir, exist_ok=True)
    
    output_stream = TimingBikeOutputStream(
        input_stream=input_stream,
        base_path=output_dir,
        file_name="timing_test_matches.txt",
    )
    
    # Setup CEP engine
    cep_engine = CEP([pattern], DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS)
    data_formatter = BikeDataFormatter()
    
    # Start timing measurement
    overall_start = time.perf_counter()
    print("Starting CEP.run()...")
    print()
    
    try:
        # Run the CEP engine
        execution_time = cep_engine.run(input_stream, output_stream, data_formatter)
        
        overall_end = time.perf_counter()
        total_time_ms = (overall_end - overall_start) * 1000
        
        # Print results
        print("=" * 70)
        print("TIMING ANALYSIS RESULTS")
        print("=" * 70)
        print(f"Total events processed: {input_stream.event_count}")
        print(f"Pattern matches found: {len(output_stream.matches)}")
        print(f"CEP reported execution time: {execution_time:.6f} seconds")
        print(f"Total measured time: {total_time_ms:.2f} milliseconds")
        print(f"Results saved to: {output_dir}/timing_test_matches.txt")
        
        if input_stream.event_count > 0:
            avg_time_per_event = total_time_ms / input_stream.event_count
            print(f"Average processing time per event: {avg_time_per_event:.2f}ms")
        
        print()
        print("EVENT PROCESSING TIMELINE:")
        print("-" * 50)
        for timing in input_stream.event_timings:
            print(f"Event {timing['event_number']:2d}: {timing['gap_from_start_ms']:8.2f}ms from system start")

        
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()



def run_csv_timing_test(csv_file_path: str, max_events: int = 100):
    """Test with real CSV data and timing analysis."""
    print("OpenCEP CSV Data Timing Test")
    print("=" * 70)
    print(f"CSV File: {csv_file_path}")
    print(f"Max Events: {max_events}")
    print()
    
    # Create pattern
    pattern = create_bike_hot_path_pattern(target_stations={426, 3002, 462}, time_window_hours=1)
    print("Pattern: Detecting bike hot paths ending at target stations (1-hour window)")
    print()
    
    input_stream = TimingBikeInputStream(
        file_path=csv_file_path,  # Use test data
        max_events=max_events
    )
    
    # Setup file output
    output_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Create timing output stream
    output_stream = TimingBikeOutputStream(
        input_stream=input_stream,
        base_path=output_dir,
        file_name="csv_timing_test_matches.txt",
    )
    
    # Setup CEP engine
    cep_engine = CEP([pattern], DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS)
    data_formatter = BikeDataFormatter()
    
    try:
        # Run the test
        execution_time = cep_engine.run(input_stream, output_stream, data_formatter)
        
        print(f"✅ CSV test completed in {execution_time:.6f} seconds")
        print(f"Results saved to: {output_dir}/csv_timing_test_matches.txt")
        
    except Exception as e:
        print(f"❌ Error during CSV test: {e}")



if __name__ == "__main__":
    # Run test with synthetic data
    run_simple_timing_test()
    
    print("\n" + "=" * 80)
    
    # Uncomment the following line to test with real CSV data:
    run_csv_timing_test('data/201804-citibike-tripdata_2.csv', max_events=1000)