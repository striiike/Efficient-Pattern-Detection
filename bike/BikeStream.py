"""
Stream adapters for bike data - both CSV files and synthetic test data.
Creates input streams for bike trip pattern testing with timing measurement capabilities.
"""

import csv
import time
from datetime import datetime, timedelta
from stream.FileStream import FileOutputStream
from stream.Stream import InputStream, OutputStream


class BikeCSVInputStream(InputStream):
    """
    Reads bike trip data from a CSV file and creates an input stream.
    Supports both file reading and synthetic test data generation.
    Includes timing measurement capabilities for performance analysis.
    """
    def __init__(self, file_path: str = None, max_events: int = None, use_test_data: bool = False):
        """
        Initialize the bike input stream.
        
        Args:
            file_path: Path to the CSV file (None for test data)
            max_events: Maximum number of events to read (None for all)
            use_test_data: If True, generate synthetic test data instead of reading file
            enable_timing: If True, enable timing measurement for each event
        """
        super().__init__()
        
        if use_test_data or file_path is None:
            self._create_test_data()
        else:
            self._load_csv_data(file_path, max_events)
    
    def _load_csv_data(self, file_path: str, max_events: int):
        """Load data from CSV file."""
        event_count = 0
        
        try:
            with open(file_path, "r", encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                
                # Skip the header row
                header = next(csv_reader, None)
                if header is None:
                    print("Warning: Empty CSV file")
                    self.close()
                    return
                
                print(f"CSV Header: {header}")
                
                # Read data rows
                for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 since we skipped header
                    if max_events and event_count >= max_events:
                        break
                        
                    if len(row) < 12:  # Ensure we have the minimum required columns
                        print(f"Warning: Skipping malformed row {row_num}: {row}")
                        continue
                    
                    # Convert row to comma-separated string for the data formatter
                    csv_line = ','.join(row)
                    self._stream.put(csv_line)
                    event_count += 1
                    
                    if event_count % 1000 == 0:
                        print(f"Loaded {event_count} events...")
                
                print(f"Successfully loaded {event_count} bike trip events from {file_path}")
                
        except FileNotFoundError:
            print(f"Error: File {file_path} not found")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
        finally:
            self.close()
    
    def _create_test_data(self):
        """Creates synthetic test data for bike pattern validation."""
        base_time = datetime(2018, 4, 27, 8, 0, 0)
        
        # Valid Pattern 1: Bike 100 - chained trips to station 426 (WITHIN 1 HOUR)
        print("✓ Pattern 1: Bike 100 → 200 → 300 → 426 (target) - fits within 1h window")
        trips1 = [
            self._create_trip(base_time, 0, 10, 100, 200, 100),   # 8:00-8:10: 100→200
            self._create_trip(base_time, 15, 25, 200, 300, 100),  # 8:15-8:25: 200→300 (chained)
            self._create_trip(base_time, 30, 50, 300, 426, 100),  # 8:30-8:50: 300→426 (target!) - WITHIN 1h
        ]
        
        # INVALID Pattern 2: Bike 200 - chained trips to station 3002 (EXCEEDS 1 HOUR)
        print("✗ Pattern 2: Bike 200 → 500 → 3002 (target) - EXCEEDS 1h window (should be rejected)")
        trips2 = [
            self._create_trip(base_time, 0, 55, 500, 600, 200),   # 8:00-8:55: 500→600
            self._create_trip(base_time, 56, 70, 600, 3002, 200)   # 8:56-9:10: 600→3002 (EXCEEDS 1h!)
        ]
        
        # Invalid Pattern 3: Different bikes
        print("✗ Invalid: Different bikes (300 → 400)")
        trips3 = [
            self._create_trip(base_time, 70, 80, 700, 800, 300),   # bike 300 - MOVED to avoid overlap
            self._create_trip(base_time, 85, 95, 800, 462, 400)   # bike 400 (different!)
        ]
        
        # Invalid Pattern 4: Not chained
        print("✗ Invalid: Gap in stations (950 ≠ 1000)")
        trips4 = [
            self._create_trip(base_time, 100, 110, 900, 950, 500),  # 900→950 - MOVED to avoid overlap
            self._create_trip(base_time, 115, 125, 1000, 426, 500)  # 1000→426 (not chained!)
        ]
        
        # Invalid Pattern 5: Time window violation (exceeds 1 hour)
        print("✗ Invalid: Time window violation (starts at 8:00, ends at 9:30 - exceeds 1h)")
        trips5 = [
            self._create_trip(base_time, 0, 10, 1100, 1200, 600),   # 8:00-8:10: 1100→1200
            self._create_trip(base_time, 90, 100, 1200, 426, 600)   # 9:30-9:40: 1200→426 (EXCEEDS 1h window!)
        ]

        print("✓ Additional Valid Pattern: Bike 100 - chained trips to station 426 (WITHIN 1 HOUR)")
        trips6 = [
            self._create_trip(base_time, 0, 10, 100, 100, 190),   # 8:00-8:10: 100→100
            self._create_trip(base_time, 15, 25, 100, 100, 190),  # 8:15-8:25: 100→100 (chained)
            self._create_trip(base_time, 30, 50, 100, 426, 190),  # 8:30-8:50: 100→426 (target!) - WITHIN 1h
        ]

        all_trips = trips1 + trips2 + trips3 + trips4 + trips5
        all_trips += trips6
        for trip in all_trips:
            self._stream.put(trip)
        
        print(f"\nTotal trips: {len(all_trips)}")
    
    def _create_trip(self, base_time, start_min, end_min, start_station, end_station, bike_id):
        """Create a bike trip CSV line."""
        start_time = base_time + timedelta(minutes=start_min)
        end_time = base_time + timedelta(minutes=end_min)
        duration = (end_time - start_time).total_seconds()
        
        return f"{int(duration)},{start_time.strftime('%Y-%m-%d %H:%M:%S.000')},{end_time.strftime('%Y-%m-%d %H:%M:%S.000')},{start_station},Station {start_station},40.75,-73.99,{end_station},Station {end_station},40.75,-73.99,{bike_id},Subscriber,1990,1"
    

# Legacy alias for backwards compatibility
TestBikeInputStream = BikeCSVInputStream

class TimingBikeInputStream(BikeCSVInputStream):
    def __init__(self, file_path: str = None, max_events: int = None, use_test_data: bool = False, enable_timing: bool = True):
        super().__init__(file_path, max_events, use_test_data)
        self.enable_timing = enable_timing
        self.system_start_time = None
        self.event_count = 0
        self.event_timings = []
        self.event_processing_times = {}  # Store when each event was processed

    def __iter__(self):
        """Iterate through events while optionally measuring timing."""
        if self.enable_timing:
            if self.system_start_time is None:
                self.system_start_time = time.perf_counter()
                print(f"✓ System start time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                print("=" * 70)
        
        # Process events from the internal stream
        while not self._stream.empty():
            event = self._stream.get()
            if event is None:  # Handle end of stream
                break
                
            if self.enable_timing:
                current_time = time.perf_counter()
                gap_ms = (current_time - self.system_start_time) * 1000
                
                # Store timing info
                self.event_timings.append({
                    'event_number': self.event_count,
                    'gap_from_start_ms': gap_ms,
                    'timestamp': datetime.now()
                })
                
                # Extract info from event data and store processing time by event key
                try:
                    parts = event.split(',')
                    bike_id = parts[11] if len(parts) > 11 else "Unknown"
                    start_station = parts[3] if len(parts) > 3 else "Unknown"
                    end_station = parts[7] if len(parts) > 7 else "Unknown"
                    start_time = parts[1] if len(parts) > 1 else "Unknown"
                    
                    # Create a unique key for this event (bike_id + start_time)
                    event_key = f"{bike_id}_{start_time}"
                    self.event_processing_times[event_key] = gap_ms
                    
                    # print(f"Event {self.event_count:2d}: OpenCEP starts processing at {gap_ms:8.2f}ms")
                    # print(f"         Data: Bike {bike_id}, {start_station}→{end_station}, {start_time[11:19]}")
                    # print(f"         Event key: {event_key}")
                    
                    # Calculate inter-event gap
                    if self.event_count > 0:
                        prev_gap = self.event_timings[self.event_count - 1]['gap_from_start_ms']
                        inter_gap = gap_ms - prev_gap
                        # print(f"         Gap from previous event: {inter_gap:6.2f}ms")
                    
                    # print()
                    
                except Exception as e:
                    print(f"Event {self.event_count}: Error parsing data - {e}")
            
            self.event_count += 1
            yield event



class TimingBikeOutputStream(FileOutputStream):
    """
    Output stream that tracks when matches are found and calculates processing delays.
    Compatible with file output while adding timing analysis.
    """
    def __init__(self, input_stream, base_path=None, file_name=None, enable_timing=True):
        """
        Initialize the timing output stream.
        
        Args:
            input_stream: Reference to input stream for timing data
            file_output_stream: Optional file output stream for writing results
            enable_timing: Whether to enable timing measurements
        """
        super().__init__(base_path=base_path, file_name=file_name)
        self.matches = []
        self.match_start_time = None
        self.input_stream = input_stream
        self.enable_timing = enable_timing
    
    def add_item(self, item):
        """Add a pattern match with timing analysis."""
        super().add_item(item)

        if not self.enable_timing:
            return
            
        if self.match_start_time is None:
            self.match_start_time = time.perf_counter()
        
        current_time = time.perf_counter()
        gap_ms = (current_time - self.match_start_time) * 1000
        match_captured_time = (current_time - self.input_stream.system_start_time) * 1000 if hasattr(self.input_stream, 'system_start_time') and self.input_stream.system_start_time else 0
        
        # Extract actual start time from PatternMatch
        match_start_times = []
        match_info = ""
        match_processing_delay = 0
        
        try:
            # Use regex to extract times and bike info from PatternMatch string representation
            item_str = str(item)
            import re
            time_matches = re.findall(r"'starttime': '([^']+)'", item_str)
            bike_matches = re.findall(r"'bike': (\d+)", item_str)
            start_matches = re.findall(r"'start': (\d+)", item_str)
            end_matches = re.findall(r"'end': (\d+)", item_str)
            
            if time_matches and bike_matches:
                match_start_times = time_matches
                bike_id = bike_matches[0] if bike_matches else "Unknown"
                
                # Calculate processing delay for this match
                # Find the earliest event processing time for events in this match
                earliest_event_processing_time = float('inf')
                
                if hasattr(self.input_stream, 'event_processing_times'):
                    for i, start_time in enumerate(time_matches):
                        event_key = f"{bike_id}_{start_time}"
                        if event_key in self.input_stream.event_processing_times:
                            event_processing_time = self.input_stream.event_processing_times[event_key]
                            if event_processing_time < earliest_event_processing_time:
                                earliest_event_processing_time = event_processing_time
                
                if earliest_event_processing_time != float('inf'):
                    match_processing_delay = match_captured_time - earliest_event_processing_time
                
                # Get unique stations in sequence
                stations = []
                if start_matches:
                    stations.append(start_matches[0])
                if end_matches:
                    stations.extend(end_matches)
                
                # Remove duplicates while preserving order
                unique_stations = []
                for station in stations:
                    if station not in unique_stations:
                        unique_stations.append(station)
                
                station_path = "->".join(unique_stations) if unique_stations else "Unknown path"
                time_range = f"{time_matches[0][11:19]} to {time_matches[-1][11:19]}" if len(time_matches) > 1 else time_matches[0][11:19]
                
                match_info = f"Bike {bike_id}: {station_path} ({time_range})"
            else:
                match_info = f"PatternMatch with {len(hasattr(item, 'events') and item.events or [])} events"
                
        except Exception as e:
            match_info = f"Error parsing PatternMatch: {e}"
        
        self.matches.append({
            'match_number': len(self.matches),
            'gap_from_first_match_ms': gap_ms,
            'match_captured_time_ms': match_captured_time,
            'match_processing_delay_ms': match_processing_delay,
            'timestamp': datetime.now(),
            'data': item,
            'match_start_times': match_start_times,
            'match_info': match_info
        })
    
    def close(self):
        """Close the output stream and any file outputs."""
        super().close()

        if self.enable_timing and self.matches:
            latency_path = self._FileOutputStream__output_path_latency
            latency_file = open(latency_path, 'w')
            for item in self.matches:
                latency_file.write(f"Match {item['match_number']}: Found at {item['gap_from_first_match_ms']:8.2f}ms from first match\n")
                latency_file.write(f"          Data: {item['match_info']}\n")
                latency_file.write(f"          Processing Delay: {item['match_processing_delay_ms']:8.2f}ms\n")
                latency_file.write(f"          Captured at: {item['match_captured_time_ms']:8.2f}ms from system start\n")
                if item['match_start_times']:
                    latency_file.write(f"          Event times: {', '.join([t[11:19] for t in item['match_start_times']])}\n")
                latency_file.write("\n")
            latency_file.close()


        # Print timing summary if enabled
        if self.enable_timing and self.matches:
            delays = [m['match_processing_delay_ms'] for m in self.matches if m['match_processing_delay_ms'] > 0]
            if delays:
                print()
                print("PROCESSING DELAY SUMMARY:")
                print("-" * 40)
                print(f"Average processing delay: {sum(delays)/len(delays):8.2f}ms")
                print(f"Min processing delay:     {min(delays):8.2f}ms")
                print(f"Max processing delay:     {max(delays):8.2f}ms")