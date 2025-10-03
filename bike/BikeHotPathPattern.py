"""
Bike Hot Path Pattern Detection Module

Implements the complete bike trip hot path detection pattern:
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {426,3002,462}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)

"""

from datetime import timedelta, datetime
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure, KleeneClosureOperator
from condition.Condition import Variable, SimpleCondition, BinaryCondition
from condition.BaseRelationCondition import EqCondition
from condition.CompositeCondition import AndCondition
from condition.KCCondition import KCIndexCondition

DEFAULT_TARGET_STATIONS = {426, 3002, 462}

def create_bike_hot_path_pattern(pattern_id=1, target_stations=None, time_window_hours=1):
    """
    Create the bike hot path detection pattern.
    
    Args:
        pattern_id: Unique identifier for the pattern
        target_stations: Set of target station IDs (defaults to {426, 3002, 462})
        time_window_hours: Time window in hours (default: 1)
    
    Returns:
        Pattern: The configured bike hot path pattern
    """
    if target_stations is None:
        target_stations = DEFAULT_TARGET_STATIONS
    
    # Structure: SEQ(BikeTrip+ a[], BikeTrip b)
    pattern_structure = SeqOperator(
        KleeneClosureOperator(
            PrimitiveEventStructure("BikeTrip", "a"),
            min_size=1,  # Reverted: Allow single trips to test time window properly
            max_size=3  # Reasonable limit to prevent excessive matches
        ),
        PrimitiveEventStructure("BikeTrip", "b")
    )
    
    conditions = []
    
    # Condition 1: a[i+1].bike = a[i].bike (consecutive trips use same bike)
    bike_consistency = KCIndexCondition(
        names={'a'},
        getattr_func=lambda event: event["bike"],
        relation_op=lambda x, y: x == y,
        offset=1
    )
    conditions.append(bike_consistency)
    
    # Condition 2: a[i+1].start = a[i].end (trips are spatially chained)
    trip_chaining = KCIndexCondition(
        names={'a'},
        getattr_func=lambda event: (event["start"], event["end"]),
        relation_op=lambda pair1, pair2: pair2[0] == pair1[1],  # next_start == current_end
        offset=1
    )
    conditions.append(trip_chaining)
    
    # Condition 3: b.end in target_stations (ends at hot path stations)
    target_station_check = SimpleCondition(
        Variable("b", lambda event: event["end"]),
        relation_op=lambda station: station in target_stations
    )
    conditions.append(target_station_check)
    
    # Condition 4: a[last].bike = b.bike (same bike used throughout)
    same_bike_final = EqCondition(
        Variable("a", lambda events: events[-1]["bike"] if events else None),
        Variable("b", lambda event: event["bike"])
    )
    conditions.append(same_bike_final)
    
    # Condition 5: EXPLICIT TIME CONSTRAINT - total sequence time <= 1 hour
    # This ensures the entire sequence from first trip start to last trip end is within time window
    def time_constraint_check(start_events, end_event):
        if not start_events:
            return True
        try:
            # Parse timestamps from the event payload
            first_start_str = start_events[0]["starttime"]
            last_end_str = end_event["stoptime"]
            
            # Parse to datetime objects (handle microseconds)
            if '.' in first_start_str:
                first_start = datetime.strptime(first_start_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                first_start = datetime.strptime(first_start_str, '%Y-%m-%d %H:%M:%S')
                
            if '.' in last_end_str:
                last_end = datetime.strptime(last_end_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                last_end = datetime.strptime(last_end_str, '%Y-%m-%d %H:%M:%S')
            
            total_duration = last_end - first_start
            max_duration = timedelta(hours=time_window_hours)
            return total_duration <= max_duration
        except (ValueError, KeyError):
            return False
    
    time_span_constraint = BinaryCondition(
        Variable("a", lambda events: events),
        Variable("b", lambda event: event),
        relation_op=time_constraint_check
    )
    conditions.append(time_span_constraint)


    # Condition 6: a[last].end = b.end (the last trip in a[] and b is the final trip)
    same_bike_final = EqCondition(
        Variable("a", lambda events: events[-1]["end"] if events else None),
        Variable("b", lambda event: event["end"])
    )
    conditions.append(same_bike_final)
    
    # Combine all conditions with AND
    pattern_condition = AndCondition(*conditions)
    
    # Create pattern with time window
    pattern = Pattern(
        pattern_structure=pattern_structure,
        pattern_matching_condition=pattern_condition,
        time_window=timedelta(hours=time_window_hours),
        pattern_id=pattern_id
    )
    
    return pattern


def create_fixed_length_pattern(pattern_id=2, target_stations=None, sequence_length=3):
    """
    Create a fixed-length bike pattern for cleaner results.
    
    PATTERN SEQ(BikeTrip a1, BikeTrip a2, ..., BikeTrip b)
    WHERE ai.bike = ai+1.bike AND ai.end = ai+1.start AND b.end in target_stations
    
    Args:
        pattern_id: Unique identifier for the pattern
        target_stations: Set of target station IDs
        sequence_length: Number of trips in the sequence (default: 3)
    
    Returns:
        Pattern: The configured fixed-length pattern
    """
    if target_stations is None:
        target_stations = {426, 3002, 462}
    
    if sequence_length < 2:
        raise ValueError("Sequence length must be at least 2")
    
    # Create sequence of trip events
    trip_events = []
    for i in range(sequence_length):
        if i == sequence_length - 1:
            # Last trip is the final destination
            trip_events.append(PrimitiveEventStructure("BikeTrip", "b"))
        else:
            # Intermediate trips
            trip_events.append(PrimitiveEventStructure("BikeTrip", f"a{i+1}"))
    
    pattern_structure = SeqOperator(*trip_events)
    
    conditions = []
    
    # Same bike throughout and chained trips
    for i in range(sequence_length - 1):
        current_var = f"a{i+1}"
        next_var = "b" if i == sequence_length - 2 else f"a{i+2}"
        
        # Same bike condition
        same_bike = EqCondition(
            Variable(current_var, lambda event: event["bike"]),
            Variable(next_var, lambda event: event["bike"])
        )
        conditions.append(same_bike)
        
        # Chained trips condition
        chained = EqCondition(
            Variable(current_var, lambda event: event["end"]),
            Variable(next_var, lambda event: event["start"])
        )
        conditions.append(chained)
    
    # Final destination check
    target_check = SimpleCondition(
        Variable("b", lambda event: event["end"]),
        relation_op=lambda station: station in target_stations
    )
    conditions.append(target_check)
    
    pattern_condition = AndCondition(*conditions)
    
    pattern = Pattern(
        pattern_structure=pattern_structure,
        pattern_matching_condition=pattern_condition,
        time_window=timedelta(hours=1),
        pattern_id=pattern_id
    )
    
    return pattern


def get_pattern_info():
    """
    Get information about the available patterns.
    
    Returns:
        dict: Information about pattern types and their characteristics
    """
    return {
        "kleene_pattern": {
            "name": "Bike Hot Path (Kleene Closure)",
            "description": "Uses BikeTrip+ to find variable-length trip sequences",
            "structure": "SEQ(BikeTrip+ a[], BikeTrip b)",
            "characteristics": "Finds all possible sub-sequences, may produce many matches",
            "use_case": "Complete pattern detection as specified in requirements"
        },
        "fixed_pattern": {
            "name": "Bike Hot Path (Fixed Length)",
            "description": "Uses fixed number of trips for cleaner results",
            "structure": "SEQ(BikeTrip a1, BikeTrip a2, ..., BikeTrip b)",
            "characteristics": "Produces fewer, more interpretable matches",
            "use_case": "Practical hot path detection with specific sequence lengths"
        }
    }


