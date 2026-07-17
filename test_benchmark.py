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
        complaint
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
from backend.database.supabase import create_benchmark

create_benchmark(
    model_name="distilbert",
    complaints_processed=len(complaints),
    average_latency_ms=average_latency,
    throughput=throughput,
    memory_mb=memory_after
)

print("\nBenchmark saved successfully!")