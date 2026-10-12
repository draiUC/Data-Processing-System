# Assignment 6: Multi-threaded Data Processing System (Java and Go)

A producer enqueues tasks; several workers take them from a shared queue, "process" them
(simulated delay + SHA-256 fingerprint), and save results to a shared result store (in-memory
list + CSV file). Both implementations log thread start/finish/errors, retry transient failures,
shut down cleanly, and finish with an integrity check (no missing or duplicate tasks).

## Requirements
* Java 17 or newer (developed on 21)
* Go 1.21 or newer (developed on 1.22)

## Java
```
cd java
mkdir -p out && javac -d out src/dps/*.java
java -cp out dps.Main                 # defaults: 4 workers, 30 tasks
java -cp out dps.Main 3 15            # 3 workers, 15 tasks
java -cp out dps.Main 2 3 /proc/nope/results.csv   # demo: unwritable output path (IOException fallback)
```

## Go
```
cd go
go run .                              # defaults: 4 workers, 30 tasks
go run . -workers 3 -tasks 15
go run . -workers 2 -tasks 3 -out /proc/nope/results.csv   # demo: unwritable output path
go run -race . -workers 8 -tasks 200  # race detector
```


