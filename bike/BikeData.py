from datetime import datetime
from base.DataFormatter import DataFormatter, EventTypeClassifier
import csv


class BikeEventTypeClassifier(EventTypeClassifier):

    BIKE_TRIP_TYPE = "BikeTrip"

    def get_event_type(self, event_payload: dict):
        return self.BIKE_TRIP_TYPE


class BikeDataFormatter(DataFormatter):

    def __init__(self, event_type_classifier: EventTypeClassifier = BikeEventTypeClassifier()):
        super().__init__(event_type_classifier)

    def parse_event(self, raw_data: str):

        # Handle both comma-separated strings and already parsed lists
        if isinstance(raw_data, str):
            # Use csv.reader to properly handle CSV format
            row = next(csv.reader([raw_data]))
        else:
            row = raw_data
            
        if len(row) < 12:  # Ensure we have the essential fields
            raise ValueError(f"Invalid CSV row: expected at least 12 columns, got {len(row)}")
            
        try:
            bike_payload = {
                "tripduration": int(row[0]) if row[0].strip() else 0,
                "starttime": row[1].strip(),
                "stoptime": row[2].strip(), 
                "start": int(float(row[3])) if row[3].strip() else 0,  # start station id
                "start_name": row[4].strip(),
                "start_lat": float(row[5]) if row[5].strip() else 0.0,
                "start_lng": float(row[6]) if row[6].strip() else 0.0,
                "end": int(float(row[7])) if row[7].strip() else 0,    # end station id
                "end_name": row[8].strip(),
                "end_lat": float(row[9]) if row[9].strip() else 0.0,
                "end_lng": float(row[10]) if row[10].strip() else 0.0,
                "bike": int(row[11]) if row[11].strip() else 0,  # bike id
                "usertype": row[12].strip() if len(row) > 12 else "",
                "birth_year": int(row[13]) if len(row) > 13 and row[13].strip() else 0,
                "gender": int(row[14]) if len(row) > 14 and row[14].strip() else 0
            }
        except (ValueError, IndexError) as e:
            raise ValueError(f"Error parsing CSV row: {e}")
            
        return bike_payload

    def get_event_timestamp(self, event_payload: dict):

        timestamp_str = event_payload["starttime"]
        try:
            # Handle microseconds if present
            if '.' in timestamp_str:
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Fallback for other formats
            return datetime.strptime(timestamp_str[:19], '%Y-%m-%d %H:%M:%S')