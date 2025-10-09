import os
import sys
import pandas as pd
import time
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import csv

# Add project root to path
sys.path.append(os.path.dirname(__file__))
from bike.BikeData import BikeDataFormatter

def find_hot_end_stations(csv_file, top_n=30, sample_size=50000):
    # Find the most popular end stations.
    data_formatter = BikeDataFormatter()
    end_stations = Counter()
    
    with open(csv_file, 'r') as f:
        header = f.readline()
        processed = 0
        
        for line in f:
            if processed >= sample_size:
                break
            line = line.strip()
            if line:
                try:
                    event = data_formatter.parse_event(line)
                    end_stations[event['end']] += 1
                    processed += 1
                except:
                    continue
    
    hot_stations = set()
    top_end_stations = end_stations.most_common(top_n)
    
    for station, _ in top_end_stations:
        hot_stations.add(station)
    
    return hot_stations

def find_chain_trip_lines(csv_file, hot_end_stations, time_window_hours=2, max_chain_hours=1.0):
    # Find all trip lines that form continuous chains ending at hot stations.
    data_formatter = BikeDataFormatter()
    trips = []
    
    with open(csv_file, 'r') as f:
        header = f.readline().strip()
        
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    event = data_formatter.parse_event(line)
                    trip = {
                        'line_num': line_num,
                        'bike_id': event['bike'],
                        'start_station': event['start'],
                        'end_station': event['end'],
                        'start_time': pd.to_datetime(event['starttime']),
                        'end_time': pd.to_datetime(event['stoptime']),
                    }
                    trips.append(trip)
                except:
                    continue
    
    df = pd.DataFrame(trips)
    df = df.sort_values(['bike_id', 'start_time'])
    
    chain_line_numbers = set()
    grouped = df.groupby('bike_id')
    
    for bike_id, bike_trips in grouped:
        bike_trips = bike_trips.sort_values('start_time').reset_index(drop=True)
        
        i = 0
        while i < len(bike_trips):
            chain_indices = [i]
            current_trip = bike_trips.iloc[i]
            chain_start_time = current_trip['start_time']
            current_end_station = current_trip['end_station']
            
            j = i + 1
            while j < len(bike_trips):
                next_trip = bike_trips.iloc[j]
                
                if next_trip['start_time'] - chain_start_time > timedelta(hours=time_window_hours):
                    break
                
                if next_trip['start_station'] == current_end_station:
                    chain_indices.append(j)
                    current_end_station = next_trip['end_station']
                    j += 1
                else:
                    break
            
            if current_end_station in hot_end_stations and len(chain_indices) >= 2:
                # Check if total chain duration is less than max_chain_hours
                first_trip = bike_trips.iloc[chain_indices[0]]
                last_trip = bike_trips.iloc[chain_indices[-1]]
                chain_duration = last_trip['end_time'] - first_trip['start_time']
                
                if chain_duration <= timedelta(hours=max_chain_hours):
                    for idx in chain_indices:
                        line_num = bike_trips.iloc[idx]['line_num']
                        chain_line_numbers.add(line_num)
            
            i = chain_indices[0] + 1
    
    return chain_line_numbers, header

def extract_chain_data(csv_file, chain_line_numbers, header, output_file, sorted_output=True):
    """Extract chain trip data to output file."""
    extracted_data = []
    
    with open(csv_file, 'r') as infile:
        infile.readline() 
        
        for line_num, line in enumerate(infile, 1):
            if line_num in chain_line_numbers:
                extracted_data.append(line.strip())
    
    if sorted_output:
        # Sort by starttime
        data_formatter = BikeDataFormatter()
        trip_data = []
        
        for line in extracted_data:
            try:
                event = data_formatter.parse_event(line)
                trip_data.append((event['starttime'], line))
            except:
                trip_data.append(('1900-01-01 00:00:00', line))
        
        trip_data.sort(key=lambda x: x[0])
        
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            outfile.write(header + '\n')
            for _, line in trip_data:
                outfile.write(line + '\n')
    else:
        # Original order
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            outfile.write(header + '\n')
            for line in extracted_data:
                outfile.write(line + '\n')
    
    return len(extracted_data)

def analyze_top_stations(sorted_file, limit=1000, top_n=10):
    """Analyze top stations from sorted data."""
    df = pd.read_csv(sorted_file)
    
    # Use first 1000 records
    df_subset = df.head(limit).copy()
    df_subset['starttime'] = pd.to_datetime(df_subset['starttime'])
    
    station_stats = defaultdict(lambda: {'chains': 0, 'total_length': 0, 'lengths': []})
    
    for bike_id, bike_trips in df_subset.groupby('bikeid'):
        bike_trips = bike_trips.sort_values('starttime').reset_index(drop=True)
        
        i = 0
        while i < len(bike_trips):
            chain_indices = [i]
            current_end = bike_trips.iloc[i]['end station id']
            
            j = i + 1
            while j < len(bike_trips):
                next_trip = bike_trips.iloc[j]
                if next_trip['start station id'] == current_end:
                    chain_indices.append(j)
                    current_end = next_trip['end station id']
                    j += 1
                else:
                    break
            
            if len(chain_indices) >= 2:
                chain_length = len(chain_indices)
                station_stats[current_end]['chains'] += 1
                station_stats[current_end]['total_length'] += chain_length
                station_stats[current_end]['lengths'].append(chain_length)
            
            i = chain_indices[0] + 1
    
    # Convert to results list
    results = []
    for station, stats in station_stats.items():
        avg_length = stats['total_length'] / stats['chains'] if stats['chains'] > 0 else 0
        max_length = max(stats['lengths']) if stats['lengths'] else 0
        results.append({
            'station': station,
            'chains': stats['chains'],
            'avg_length': avg_length,
            'max_length': max_length,
            'total_trips': stats['total_length']
        })
    
    # Sort by chains count and return top N
    results.sort(key=lambda x: x['chains'], reverse=True)
    return results[:top_n]

if __name__ == "__main__":
    INPUT_FILE = 'data/201804-citibike-tripdata_2.csv'
    OUTPUT_ORIGINAL = 'data/chain_trips_subset_1h.csv'
    OUTPUT_SORTED = 'data/chain_trips_subset_1h_sorted.csv'

    if not os.path.exists(OUTPUT_ORIGINAL) and not os.path.exists(OUTPUT_SORTED):
        hot_end_stations = find_hot_end_stations(INPUT_FILE, top_n=30)
        chain_line_numbers, header = find_chain_trip_lines(
            INPUT_FILE, hot_end_stations, time_window_hours=2.0, max_chain_hours=1.0
        )

        extract_chain_data(
            INPUT_FILE, chain_line_numbers, header, OUTPUT_ORIGINAL, sorted_output=False
        )
        extract_chain_data(
            INPUT_FILE, chain_line_numbers, header, OUTPUT_SORTED, sorted_output=True
        )

    top_stations = analyze_top_stations(OUTPUT_SORTED, limit=1000, top_n=10)
    top_stations = { int(station['station']) for station in top_stations }
    
    print(top_stations)