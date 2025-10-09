import csv
import time
import random
from datetime import datetime, timedelta
from collections import Counter
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bike.BikeHotPathPattern import BikeHotPathPatternConfig

from bike.BikeHotPathPattern import DEFAULT_TARGET_STATIONS
from bike.EventUtilityScorer import BikeEventUtilityScorer
from stream.FileStream import FileOutputStream
from stream.Stream import InputStream, OutputStream


class BikeCSVInputStream(InputStream):

    def __init__(self, file_path: str = None, max_events: int = None, use_test_data: bool = False):

        super().__init__()
        
        if use_test_data or file_path is None:
            self._create_test_data()
        else:
            self._load_csv_data(file_path, max_events)
    
    def _load_csv_data(self, file_path: str, max_events: int):
        # Load data from CSV file.
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
                        
                    if len(row) < 12:
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
        # Creates synthetic test data for bike pattern validation.
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
            self._create_trip(base_time, 70, 80, 700, 800, 300),   # bike 300 
            self._create_trip(base_time, 85, 95, 800, 462, 400)   # bike 400
        ]
        
        # Invalid Pattern 4: Not chained
        print("✗ Invalid: Gap in stations (950 ≠ 1000)")
        trips4 = [
            self._create_trip(base_time, 100, 110, 900, 950, 500),  # 900→950
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
        start_time = base_time + timedelta(minutes=start_min)
        end_time = base_time + timedelta(minutes=end_min)
        duration = (end_time - start_time).total_seconds()
        
        return f"{int(duration)},{start_time.strftime('%Y-%m-%d %H:%M:%S.000')},{end_time.strftime('%Y-%m-%d %H:%M:%S.000')},{start_station},Station {start_station},40.75,-73.99,{end_station},Station {end_station},40.75,-73.99,{bike_id},Subscriber,1990,1"
    

# Legacy alias for backwards compatibility
TestBikeInputStream = BikeCSVInputStream

class TimingBikeInputStream(BikeCSVInputStream):
    def __init__(self, file_path: str = None, max_events: int = None, use_test_data: bool = False, enable_timing: bool = True, shed_when_overloaded: bool = False, base_drop_prob: float = 0.0, event_sleep_ms: float = 0.0, burst_every: int = 0, burst_sleep_ms: float = 0.0, shed_mode: str = "event", counters: Optional[Counter] = None):
        super().__init__(file_path, max_events, use_test_data)
        if not 0.0 <= base_drop_prob <= 1.0:
            raise ValueError('base_drop_prob must be between 0 and 1')
        if event_sleep_ms < 0.0:
            raise ValueError('event_sleep_ms cannot be negative')
        if burst_every < 0:
            raise ValueError('burst_every cannot be negative')
        if burst_sleep_ms < 0.0:
            raise ValueError('burst_sleep_ms cannot be negative')
        if shed_mode not in {'event', 'hybrid'}:
            raise ValueError("shed_mode must be 'event' or 'hybrid'")

        self.enable_timing = enable_timing
        self.system_start_time = None
        self.event_count = 0
        self.event_timings = []
        self.event_processing_times = {}  # Store when each event was processed
        self.shed_when_overloaded = shed_when_overloaded
        self.base_drop_prob = base_drop_prob
        self.event_sleep_ms = event_sleep_ms
        self.burst_every = burst_every
        self.burst_sleep_ms = burst_sleep_ms
        self.shed_mode = shed_mode
        self.overload_detector = None  
        self._pattern_config: Optional['BikeHotPathPatternConfig'] = None
        self._utility_scorer = BikeEventUtilityScorer(target_stations=DEFAULT_TARGET_STATIONS)
        self.dropped_events = 0
        self.total_events_seen = 0
        self.last_drop_probability = 0.0
        self._last_yield_timestamp = None
        self._last_reported_cap = None
        self.counters = counters
        if self.counters is not None:
            for key in ('events_ingested', 'events_dropped', 'matches_completed', 'partial_pruned', 'partial_evicted'):
                self.counters.setdefault(key, 0)

    def _increment_counter(self, key: str, amount: int = 1) -> None:
        if self.counters is not None:
            self.counters[key] += amount

    @property
    def pattern_config(self) -> Optional['BikeHotPathPatternConfig']:
        return self._pattern_config

    @pattern_config.setter
    def pattern_config(self, config: Optional['BikeHotPathPatternConfig']) -> None:
        self._pattern_config = config
        self._configure_utility_scorer()

    def _configure_utility_scorer(self) -> None:
        if self._utility_scorer is None or self._pattern_config is None:
            return
        targets = getattr(self._pattern_config, "target_stations", None)
        if targets:
            self._utility_scorer.update_targets(targets)
        window = getattr(self._pattern_config, "time_window", None)
        if window is not None:
            self._utility_scorer.update_window(window)

    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_time(value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return datetime.strptime(stripped, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return None

    def _parse_event_payload(self, raw_event: str):
        parts = raw_event.strip().split(',')
        bike_id = parts[11].strip() if len(parts) > 11 and parts[11] else None
        start_station = self._safe_int(parts[3]) if len(parts) > 3 else None
        end_station = self._safe_int(parts[7]) if len(parts) > 7 else None
        start_time_str = parts[1].strip() if len(parts) > 1 else None
        end_time_str = parts[2].strip() if len(parts) > 2 else None
        start_time = self._parse_time(start_time_str)
        end_time = self._parse_time(end_time_str)
        return {
            'parts': parts,
            'bike_id': bike_id,
            'start_station': start_station,
            'end_station': end_station,
            'start_time': start_time,
            'end_time': end_time,
            'start_time_str': start_time_str,
            'end_time_str': end_time_str,
        }


    def __iter__(self):
        if self.enable_timing:
            if self.system_start_time is None:
                self.system_start_time = time.perf_counter()
                print(f"✓ System start time: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                print("=" * 70)
        
        # Process events from the internal stream
        while not self._stream.empty():
            resume_time = time.perf_counter()
            event = self._stream.get()
            if event is None:  # Handle end of stream
                break

            self.total_events_seen += 1
            self._increment_counter('events_ingested')
            overshoot = 0.0
            if self.overload_detector:
                if self._last_yield_timestamp is not None:
                    event_processing_ms = (resume_time - self._last_yield_timestamp) * 1000.0
                    if event_processing_ms >= 0:
                        self.overload_detector.note_event_latency(event_processing_ms)
                overshoot = self.overload_detector.overshoot() if hasattr(self.overload_detector, 'overshoot') else 0.0

            drop_probability = min(0.9, self.base_drop_prob + 0.5 * overshoot)
            drop_probability = max(0.0, drop_probability)
            self.last_drop_probability = drop_probability

            payload = self._parse_event_payload(event)
            classification = "supporting"
            scorer = self._utility_scorer
            if scorer is not None:
                score, classification = scorer.score_event(
                    payload.get('bike_id'),
                    payload.get('start_station'),
                    payload.get('end_station'),
                    payload.get('start_time'),
                    payload.get('end_time'),
                )
            else:
                score = None

            payload['utility_score'] = score
            payload['utility_label'] = classification

            if self.pattern_config is not None:
                base_cap = self.pattern_config.initial_max_kleene_size
                target_cap = base_cap
                if self.shed_mode == 'hybrid' and self.shed_when_overloaded:
                    if overshoot > 0.0:
                        shrink = 1 + int(overshoot * 2)
                        target_cap = max(2, base_cap - shrink)
                current_cap = self.pattern_config.max_kleene_size
                if target_cap != current_cap:
                    if target_cap < current_cap:
                        self._increment_counter('partial_evicted', current_cap - target_cap)
                    self.pattern_config.max_kleene_size = target_cap
                if (
                    self.shed_mode == 'hybrid'
                    and self._last_reported_cap != target_cap
                    and (self._last_reported_cap is not None or target_cap < base_cap)
                ):
                    print("[HybridShedding] Kleene max size adjusted to {}".format(target_cap))
                self._last_reported_cap = target_cap
            elif self._last_reported_cap is not None:
                self._last_reported_cap = None

            should_drop = False
            drop_chance = 0.0
            if self.shed_when_overloaded and drop_probability > 0.0:
                if classification == "non_critical":
                    drop_chance = drop_probability
                elif classification == "supporting" and overshoot > 0.6:
                    drop_chance = drop_probability * min(1.0, overshoot)
                else:
                    drop_chance = 0.0
                if drop_chance > 0.0 and random.random() < drop_chance:
                    should_drop = True

            if should_drop:
                self.dropped_events += 1
                self._increment_counter('events_dropped')
                if scorer is not None:
                    scorer.note_event(
                        payload.get('bike_id'),
                        payload.get('start_station'),
                        payload.get('end_station'),
                        payload.get('start_time'),
                        payload.get('end_time'),
                        accepted=False,
                    )
                continue

            if self.event_sleep_ms > 0.0 or (self.burst_every and self.burst_sleep_ms > 0.0):
                sleep_seconds = 0.0
                if self.event_sleep_ms > 0.0:
                    sleep_seconds += self.event_sleep_ms / 1000.0
                if self.burst_every and self.burst_sleep_ms > 0.0:
                    if self.total_events_seen % self.burst_every == 0:
                        sleep_seconds += self.burst_sleep_ms / 1000.0
                if sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)

            if scorer is not None:
                scorer.note_event(
                    payload.get('bike_id'),
                    payload.get('start_station'),
                    payload.get('end_station'),
                    payload.get('start_time'),
                    payload.get('end_time'),
                    accepted=True,
                )

            if self.enable_timing:
                current_time = time.perf_counter()
                gap_ms = (current_time - self.system_start_time) * 1000
                
                # Store timing info
                self.event_timings.append({
                    'event_number': self.event_count,
                    'gap_from_start_ms': gap_ms,
                    'timestamp': datetime.now()
                })
                
                # Extract info from event data and store processing time
                try:
                    bike_id = payload.get('bike_id') or "Unknown"
                    start_station = payload.get('start_station')
                    end_station = payload.get('end_station')
                    start_time = payload.get('start_time_str') or "Unknown"

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
            
            self._last_yield_timestamp = time.perf_counter()
            self.event_count += 1
            yield event



class TimingBikeOutputStream(FileOutputStream):

    def __init__(self, input_stream, base_path=None, file_name=None, enable_timing=True):

        super().__init__(base_path=base_path, file_name=file_name)
        self.matches = []
        self.match_start_time = None
        self.input_stream = input_stream
        self.enable_timing = enable_timing
        self.counters = getattr(input_stream, "counters", None)
        if self.counters is not None:
            self.counters.setdefault('matches_completed', 0)
    
    def _increment_counter(self, key: str, amount: int = 1) -> None:
        if self.counters is not None:
            self.counters[key] += amount


    def add_item(self, item):
        super().add_item(item)
        self._increment_counter('matches_completed')

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
        match_processing_delay_ms = 0.0
        match_projection = None
        
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
                    match_processing_delay_ms = match_captured_time - earliest_event_processing_time
                    if hasattr(self, 'overload_detector') and self.overload_detector:
                        self.overload_detector.note_latency(match_processing_delay_ms)
                
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

                if start_matches and end_matches:
                    try:
                        a1_start = int(start_matches[0])
                        b_end = int(end_matches[-1])
                        last_a_end = int(end_matches[-2]) if len(end_matches) >= 2 else b_end
                        match_projection = (a1_start, last_a_end, b_end)
                    except ValueError:
                        match_projection = None
            else:
                match_info = f"PatternMatch with {len(hasattr(item, 'events') and item.events or [])} events"
                
        except Exception as e:
            match_info = f"Error parsing PatternMatch: {e}"
        
        self.matches.append({
            'match_number': len(self.matches),
            'gap_from_first_match_ms': gap_ms,
            'match_captured_time_ms': match_captured_time,
            'match_processing_delay_ms': match_processing_delay_ms,
            'timestamp': datetime.now(),
            'data': item,
            'match_start_times': match_start_times,
            'match_info': match_info,
            'projection': match_projection
        })
    
    def close(self):
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


        # Print timing summary
        if self.enable_timing and self.matches:
            delays = [m['match_processing_delay_ms'] for m in self.matches if m['match_processing_delay_ms'] > 0]
            if delays:
                print()
                print("PROCESSING DELAY SUMMARY:")
                print("-" * 40)
                print(f"Average processing delay: {sum(delays)/len(delays):8.2f}ms")
                print(f"Min processing delay:     {min(delays):8.2f}ms")
                print(f"Max processing delay:     {max(delays):8.2f}ms")

