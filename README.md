# Efficient Pattern Detection over Data Streams

> CS-E4780 Scalable Systems and Data Management Course Project 1

Group Number: 7
Group Members: Baiyan Che, Amirreza Jafariandehkordi

Datas should be put under `data/`, preferably chosen as nyc 2018 April_2 from `https://s3.amazonaws.com/tripdata/index.html`.
Outputs are under `test_output`.

`TestBikePattern.py` is for testing the correctness of kleene closure for 
```sql
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)
```

Adding condition `a[last].end = b.end` and `b.end_time - a[1].start_time < 1h` for robustness.

Since `RETURN` is not available in OpenCEP, custom handler will be implemented later, because it doesn't affect much.