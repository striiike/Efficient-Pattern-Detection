# Efficient Pattern Detection over Data Streams

> CS-E4780 Scalable Systems and Data Management Course Project 1

Group Number: 7

Group Members: Baiyan Che, Amirreza Jafariandehkordi

Datas should be put under `data/`, preferably chosen as nyc 2018 April_2 from `https://s3.amazonaws.com/tripdata/index.html`, which is the old format with `Bike ID`.
Outputs are under `test_output`.

Python version >= 3.13


`TestBikePattern.py` is for testing the correctness of kleene closure for 
```sql
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)
```

Adding condition `a[last].end = b.end` and `b.end_time - a[1].start_time < 1h` for robustness.

Amended Query Sql:
```sql
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {hot_end_stations}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
AND a[last].end = b.end
AND b.end_time - a[1].start_time <= 1h
WITHIN 1h
RETURN a, b
```

Since `RETURN` is not available in OpenCEP, custom handler will be implemented later, because it doesn't affect much.


## Usage

```python
from CEP import CEP
from bike.BikeStream import BikeCSVInputStream, TimingBikeInputStream, TimingBikeOutputStream
from bike.BikeData import BikeDataFormatter
from bike.BikeHotPathPattern import create_bike_hot_path_pattern
from test.testUtils import DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS
import os

if __name__ == "__main__":
    pattern = create_bike_hot_path_pattern(
        target_stations={426, 3002, 462}, time_window_hours=1, max_kleene_size=3)

    input_stream = TimingBikeInputStream(
        use_test_data=True,
    )

    output_dir = "test_output"
    os.makedirs(output_dir, exist_ok=True)

    output_stream = TimingBikeOutputStream(
        input_stream=input_stream,
        base_path=output_dir,
        file_name="example.txt",
    )

    cep_engine = CEP([pattern], DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS)
    data_formatter = BikeDataFormatter()

    cep_engine.run(input_stream, output_stream, data_formatter)

```