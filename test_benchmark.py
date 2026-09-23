import time
import psutil

from backend.services.sentiment.sentiment_service import (
    analyze_sentiment
)
from backend.database.supabase import (
    create_benchmark
)
from backend.database.supabase import (
    get_sample_complaints
)

# ----------------------------------
# WHICH MODEL THIS RUN IS BENCHMARKING
# ----------------------------------
# Set this explicitly and re-run the whole script once per model
# ("roberta", then "distilbert"). Do NOT rely on analyze_sentiment's
# default — that was the bug: it silently defaulted to "roberta"
# regardless of what this script claimed it was benchmarking.

MODEL_NAME = "roberta"  # change to "roberta" for the other run


complaints = get_sample_complaints(
    limit=100
)

# Current Python process
process = psutil.Process()

# Memory before benchmark
memory_before = (
    process.memory_info().rss
) / (1024 * 1024)

results = []

# Start benchmark timer
start = time.time()

for complaint in complaints:

    result = analyze_sentiment(
        complaint,
        model_name=MODEL_NAME
    )

    results.append(result)

# End benchmark timer
end = time.time()

# Memory after benchmark
memory_after = (
    process.memory_info().rss
) / (1024 * 1024)

# Metrics
total_time = end - start

average_latency = (
    total_time / len(complaints)
) * 1000

throughput = (
    len(complaints) / total_time
)

memory_used = (
    memory_after - memory_before
)

# ----------------------------------
# OUTPUT
# ----------------------------------

print("\n========== BENCHMARK REPORT ==========\n")

print(f"Model: {MODEL_NAME}")

print(f"Complaints Processed: {len(complaints)}")

print(f"Total Runtime: {total_time:.2f} seconds")

print(f"Average Latency: {average_latency:.2f} ms")

print(
    f"Throughput: {throughput:.2f} complaints/sec"
)

print(
    f"Memory Used During Benchmark: "
    f"{memory_used:.2f} MB"
)

print(
    f"Total Process Memory: "
    f"{memory_after:.2f} MB"
)


print("\n========== PREDICTIONS ==========\n")

for i, result in enumerate(results, start=1):

    print(
        f"{i}. "
        f"{result.sentiment} "
        f"({result.confidence:.4f})"
    )

print("\n======================================")

create_benchmark(
    model_name=MODEL_NAME,
    complaints_processed=len(complaints),
    average_latency_ms=average_latency,
    throughput=throughput,
    memory_mb=memory_after
)

print(f"\nBenchmark for '{MODEL_NAME}' saved successfully!")