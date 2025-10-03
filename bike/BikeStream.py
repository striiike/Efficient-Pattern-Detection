"""
Stream adapters for bike data - both CSV files and synthetic test data.
Creates input streams for bike trip pattern testing.
"""

import csv
from datetime import datetime, timedelta
from stream.Stream import InputStream


class BikeCSVInputStream(InputStream):
    """
    Reads bike trip data from a CSV file and creates an input stream.
    Skips the CSV header and handles data parsing.
    """
    def __init__(self, file_path: str, max_events: int = None):
        """
        Initialize the CSV input stream.
        
        Args:
            file_path: Path to the CSV file
            max_events: Maximum number of events to read (None for all)
        """
        super().__init__()
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


class TestBikeInputStream(InputStream):
    """
    Creates synthetic test data for bike pattern validation.
    """
    def __init__(self, test_type="comprehensive"):
        super().__init__()
        
        base_time = datetime(2018, 4, 27, 8, 0, 0)
        self._create_test_data(base_time)
        self.close()
    
    def _create_test_data(self, base_time):
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
            self._create_trip(base_time, 56, 60, 600, 3002, 200)   # 8:56-9:10: 600→3002 (EXCEEDS 1h!)
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

        trips6 = [
            self._create_trip(base_time, 0, 10, 100, 100, 100),   # 8:00-8:10: 100→200
            self._create_trip(base_time, 15, 25, 100, 100, 100),  # 8:15-8:25: 200→300 (chained)
            self._create_trip(base_time, 30, 50, 100, 426, 100),  # 8:30-8:50: 300→426 (target!) - WITHIN 1h
        ]

        all_trips = trips1 + trips2 + trips3 + trips4 + trips5
        all_trips = trips6
        for trip in all_trips:
            self._stream.put(trip)
        
        print(f"\nTotal trips: {len(all_trips)}")

    
    def _create_trip(self, base_time, start_min, end_min, start_station, end_station, bike_id):
        """Create a bike trip CSV line."""
        start_time = base_time + timedelta(minutes=start_min)
        end_time = base_time + timedelta(minutes=end_min)
        duration = (end_time - start_time).total_seconds()
        
        return f"{int(duration)},{start_time.strftime('%Y-%m-%d %H:%M:%S.000')},{end_time.strftime('%Y-%m-%d %H:%M:%S.000')},{start_station},Station {start_station},40.75,-73.99,{end_station},Station {end_station},40.75,-73.99,{bike_id},Subscriber,1990,1"