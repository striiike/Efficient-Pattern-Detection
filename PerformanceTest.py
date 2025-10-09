#!/usr/bin/env python3
import os
import sys
import time
import json
import multiprocessing
import psutil
import threading
from pathlib import Path
from collections import Counter
from ChainAnalysis import analyze_top_stations

sys.path.append(os.path.dirname(__file__))
from CEP import CEP
from bike.BikeData import BikeDataFormatter
from bike.BikeHotPathPattern import create_bike_hot_path_pattern
from bike.BikeStream import TimingBikeInputStream, TimingBikeOutputStream
from parallel.ParallelExecutionParameters import DataParallelExecutionParametersRIPAlgorithm

def monitor_cpu_usage(stop_event, interval=0.1):
    """Monitor CPU usage until stop_event is set"""
    cpu_samples = []
    memory_samples = []
    
    while not stop_event.is_set():
        cpu_percent = psutil.cpu_percent(interval=None)
        memory_info = psutil.virtual_memory()
        memory_mb = memory_info.used / (1024 * 1024)
        
        cpu_samples.append(cpu_percent)
        memory_samples.append(memory_mb)
        time.sleep(interval)
    
    return {
        'cpu_avg': sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
        'cpu_max': max(cpu_samples) if cpu_samples else 0,
        'cpu_min': min(cpu_samples) if cpu_samples else 0,
        'memory_avg_mb': sum(memory_samples) / len(memory_samples) if memory_samples else 0,
        'memory_max_mb': max(memory_samples) if memory_samples else 0,
        'samples_count': len(cpu_samples)
    }

def test_performance(csv_path, max_events, cpu_cores, shed_enabled=False, base_drop_prob=0.0, shed_mode="event"):
    """Test CEP performance with given parameters"""
    start_time = time.time()
    
    top_stations = analyze_top_stations(csv_path, limit=max_events, top_n=10)
    target_stations = { int(station['station']) for station in top_stations }
    print(target_stations)
    
    # Setup pattern
    pattern, pattern_cfg = create_bike_hot_path_pattern(
        target_stations=target_stations,
        time_window_hours=1,
        max_kleene_size=3
    )
    
    # Setup parallel execution if using multiple cores
    parallel_params = None
    if cpu_cores > 1:
        parallel_params = DataParallelExecutionParametersRIPAlgorithm(units_number=cpu_cores)
    
    # Setup streams with shed mode configuration
    run_counters = Counter()
    inp = TimingBikeInputStream(
        file_path=csv_path,
        max_events=max_events,
        use_test_data=False,
        enable_timing=False,
        shed_when_overloaded=shed_enabled,
        base_drop_prob=base_drop_prob,
        shed_mode=shed_mode,
        counters=run_counters
    )
    out = TimingBikeOutputStream(
        inp,
        base_path="bike/temp",
        file_name="test",
        enable_timing=False
    )
    
    # Start CPU monitoring in background thread
    cpu_monitor_result = {}
    stop_monitoring = threading.Event()
    
    def cpu_monitor_worker():
        nonlocal cpu_monitor_result
        cpu_monitor_result = monitor_cpu_usage(stop_monitoring)
    
    # Start monitoring before CEP execution
    cpu_monitor_thread = threading.Thread(target=cpu_monitor_worker, daemon=True)
    cpu_monitor_thread.start()
    
    # Run CEP with parallel execution parameters
    cep_start_time = time.time()
    engine = CEP([pattern], parallel_execution_params=parallel_params)
    engine.run(inp, out, BikeDataFormatter())
    cep_execution_time = time.time() - cep_start_time
    
    # Stop CPU monitoring
    stop_monitoring.set()
    if cpu_monitor_thread and cpu_monitor_thread.is_alive():
        cpu_monitor_thread.join(timeout=2.0)
    
    execution_time = time.time() - start_time
    events_per_second = max_events / execution_time if execution_time > 0 else 0
    
    # Calculate shedding statistics
    total_events_seen = getattr(inp, 'total_events_seen', max_events)
    dropped_events = getattr(inp, 'dropped_events', 0)
    drop_rate = (dropped_events / total_events_seen) if total_events_seen > 0 else 0.0
    
    return {
        'events': max_events,
        'cores': cpu_cores,
        'shed_enabled': shed_enabled,
        'base_drop_prob': base_drop_prob,
        'shed_mode': shed_mode,
        'time_s': execution_time,
        'cep_time_s': cep_execution_time,
        'events_per_sec': events_per_second,
        'matches': run_counters.get('matches_completed', 0),
        'total_events_seen': total_events_seen,
        'dropped_events': dropped_events,
        'drop_rate': drop_rate,
        'cpu_usage': cpu_monitor_result
    }

def run_scaling_test(csv_path='data/chain_trips_subset_1h_sorted_noisy.csv'):
    """Run performance scaling test with shed mode comparison"""
    results = []
       
    event_counts = [500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    core_counts = [1, 2, 4, 8, 16, 32]
    
    # Shed mode configurations: no-shed and shed with 50% capacity
    shed_configs = [
        {'shed_enabled': False, 'base_drop_prob': 0.0, 'shed_mode': 'event', 'name': 'no-shed'},
        {'shed_enabled': True, 'base_drop_prob': 0.5, 'shed_mode': 'event', 'name': 'shed-50%'},
    ]
    
    print(f"Testing with {csv_path}")
    print(f"Event counts: {event_counts}")
    print(f"Core counts: {core_counts}")
    print(f"Shed configurations: {[c['name'] for c in shed_configs]}")
    print()
    
    for events in event_counts:
        for cores in core_counts:
            for shed_config in shed_configs:
                config_name = shed_config['name']
                print(f"Testing {events} events, {cores} cores, {config_name}...")
                try:
                    result = test_performance(
                        csv_path=csv_path, 
                        max_events=events, 
                        cpu_cores=cores,
                        shed_enabled=shed_config['shed_enabled'],
                        base_drop_prob=shed_config['base_drop_prob'],
                        shed_mode=shed_config['shed_mode']
                    )
                    result['config_name'] = config_name
                    results.append(result)
                    
                    cpu_avg = result.get('cpu_usage', {}).get('cpu_avg', 0)
                    samples = result.get('cpu_usage', {}).get('samples_count', 0)
                    drop_rate = result.get('drop_rate', 0) * 100
                    
                    print(f"  {result['time_s']:.2f}s, {result['events_per_sec']:.0f} eps, "
                          f"CPU={cpu_avg:.1f}% ({samples} samples), "
                          f"dropped={drop_rate:.1f}%, matches={result['matches']}")
                          
                except Exception as e:
                    print(f"  Failed: {e}")
    
    return results

def analyze_results(results):
    """Analyze and display results"""
    print("\nPerformance Results:")
    print("Events  Cores  Config      Time(s)  Events/sec  Matches  CPU%   Memory(MB)  Drop%")
    print("-" * 85)
    
    for r in results:
        cpu_avg = r.get('cpu_usage', {}).get('cpu_avg', 0)
        memory_avg = r.get('cpu_usage', {}).get('memory_avg_mb', 0)
        config_name = r.get('config_name', 'unknown')
        drop_rate = r.get('drop_rate', 0) * 100
        
        print(f"{r['events']:6d}  {r['cores']:5d}  {config_name:10s}  {r['time_s']:7.2f}  {r['events_per_sec']:10.0f}  {r['matches']:7d}  {cpu_avg:5.1f}  {memory_avg:8.0f}  {drop_rate:5.1f}")
    
    print("\nDetailed Analysis by Configuration:")
    
    # Group by configuration
    by_config = {}
    for r in results:
        config = r.get('config_name', 'unknown')
        if config not in by_config:
            by_config[config] = []
        by_config[config].append(r)
    
    for config in sorted(by_config.keys()):
        print(f"\n=== {config.upper()} ===")
        config_results = by_config[config]
        
        # Group by events to see scaling per event count
        by_events = {}
        for r in config_results:
            events = r['events']
            if events not in by_events:
                by_events[events] = []
            by_events[events].append(r)
        
        for events in sorted(by_events.keys()):
            print(f"\n{events} events:")
            configs = sorted(by_events[events], key=lambda x: x['cores'])
            
            if len(configs) > 1:
                baseline = configs[0]  # 1 core baseline
                
                for config in configs:
                    speedup = baseline['time_s'] / config['time_s'] if config['time_s'] > 0 else 0
                    efficiency = speedup / config['cores'] if config['cores'] > 0 else 0
                    cpu_avg = config.get('cpu_usage', {}).get('cpu_avg', 0)
                    drop_rate = config.get('drop_rate', 0) * 100
                    print(f"  {config['cores']} cores: {config['time_s']:.2f}s, speedup={speedup:.2f}x, efficiency={efficiency:.2f}, CPU={cpu_avg:.1f}%, drop={drop_rate:.1f}%")
            else:
                config = configs[0]
                cpu_avg = config.get('cpu_usage', {}).get('cpu_avg', 0)
                drop_rate = config.get('drop_rate', 0) * 100
                print(f"  {config['cores']} cores: {config['time_s']:.2f}s, CPU={cpu_avg:.1f}%, drop={drop_rate:.1f}%")
    
    # Compare configurations
    print("\n=== CONFIGURATION COMPARISON ===")
    
    # Find best configurations for each event/core combination
    combinations = {}
    for r in results:
        key = (r['events'], r['cores'])
        if key not in combinations:
            combinations[key] = []
        combinations[key].append(r)
    
    print("Events  Cores  Best Configuration (by throughput)")
    print("-" * 50)
    
    for (events, cores), configs in sorted(combinations.items()):
        best = max(configs, key=lambda x: x['events_per_sec'])
        best_config = best.get('config_name', 'unknown')
        best_throughput = best['events_per_sec']
        best_drop = best.get('drop_rate', 0) * 100
        
        print(f"{events:6d}  {cores:5d}  {best_config:15s} ({best_throughput:.0f} eps, {best_drop:.1f}% drop)")
        
        # Show comparison with other configs for this combination
        for config in configs:
            if config != best:
                config_name = config.get('config_name', 'unknown')
                throughput = config['events_per_sec']
                drop_rate = config.get('drop_rate', 0) * 100
                relative_perf = (throughput / best_throughput) * 100 if best_throughput > 0 else 0
                print(f"              vs {config_name:15s} ({throughput:.0f} eps, {drop_rate:.1f}% drop, {relative_perf:.1f}% of best)")
    
    # Overall best performance
    print("\n=== OVERALL BEST PERFORMANCE ===")
    best_throughput = max(results, key=lambda x: x['events_per_sec'])
    print(f"Best throughput: {best_throughput['events_per_sec']:.0f} eps")
    print(f"  Configuration: {best_throughput.get('config_name', 'unknown')}")
    print(f"  Setup: {best_throughput['events']} events, {best_throughput['cores']} cores")
    print(f"  Time: {best_throughput['time_s']:.2f}s")
    print(f"  Matches: {best_throughput['matches']}")
    print(f"  Drop rate: {best_throughput.get('drop_rate', 0) * 100:.1f}%")
    
    cpu_info = best_throughput.get('cpu_usage', {})
    if cpu_info:
        print(f"  CPU usage: {cpu_info.get('cpu_avg', 0):.1f}% avg, {cpu_info.get('cpu_max', 0):.1f}% max")
        print(f"  Memory usage: {cpu_info.get('memory_avg_mb', 0):.0f}MB avg, {cpu_info.get('memory_max_mb', 0):.0f}MB max")

def save_results(results, output_file='bike/temp/performance_results_shed_comparison.json'):
    """Save results to JSON file"""
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output_file}")

if __name__ == "__main__":
    csv_path = 'data/chain_trips_subset_1h_sorted_noisy.csv'
    
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    if not Path(csv_path).exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)
    
    results = run_scaling_test(csv_path)
    analyze_results(results)
    save_results(results)