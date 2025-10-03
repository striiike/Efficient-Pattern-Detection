"""
Analyze bike trip data to show statistics including line count, top stations, and bikes.
"""

import os
import sys
sys.path.append(os.path.dirname(__file__))

from bike.BikeData import BikeDataFormatter
from collections import Counter


def analyze_bike_data(file_path, max_lines=None, top_count=10):
    """
    Analyze the bike data to show statistics.
    
    Args:
        file_path (str): Path to the CSV file to analyze
        max_lines (int, optional): Maximum number of lines to process. If None, process all lines
        top_count (int): Number of top items to show for each category
    
    Returns:
        dict: Analysis results containing line count, top stations, and bikes
    """
    if not os.path.exists(file_path):
        print(f"Error: Input file not found: {file_path}")
        return None
    
    data_formatter = BikeDataFormatter()
    start_stations = Counter()
    end_stations = Counter()
    bikes = Counter()
    
    try:
        with open(file_path, 'r') as f:
            total_lines = sum(1 for _ in f)
        
        with open(file_path, 'r') as f:
            # Skip header
            header = f.readline()
            data_lines = 0
            
            for line_num, line in enumerate(f, start=1):
                # Stop if we've reached the maximum number of lines to process
                if max_lines is not None and data_lines >= max_lines:
                    break
                    
                line = line.strip()
                if line:
                    try:
                        event = data_formatter.parse_event(line)
                        start_stations[event['start']] += 1
                        end_stations[event['end']] += 1
                        bikes[event['bike']] += 1
                        data_lines += 1
                    except Exception as e:
                        print(f"Error parsing line {line_num}: {e}")
                        continue
    
        # Collect results
        results = {
            'total_lines': total_lines,
            'data_lines': data_lines,
            'top_start_stations': start_stations.most_common(top_count),
            'top_end_stations': end_stations.most_common(top_count),
            'top_bikes': bikes.most_common(top_count),
            'unique_start_stations': len(start_stations),
            'unique_end_stations': len(end_stations),
            'unique_bikes': len(bikes)
        }
        print(f"\nMost common END stations in first {max_lines} lines : {results['top_end_stations']}")

        return results

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None



if __name__ == "__main__":
    results = analyze_bike_data('data/201804-citibike-tripdata_2.csv', max_lines=1000, top_count=10)
