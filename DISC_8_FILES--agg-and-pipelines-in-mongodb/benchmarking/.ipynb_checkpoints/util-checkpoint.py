# Sanity check - hello world
def hello_world():
    print("Hello World")
    

# Function to benchmark a simple query
def benchmark_query(query, time, benchmark_data, collection, index_name=None):
    if index_name:
        print(f"\nQuerying with index on {index_name}...")
    else:
        print("\nQuerying without index...")

    start_time = time.time()
    results = list(collection.find(query))
    end_time = time.time()

    duration = end_time - start_time
    result_count = len(results)

    print(f"Query took {duration:.6f} seconds")
    print(f"Number of results: {result_count}")

    benchmark_data.append({
        "query": query,
        "index_name": index_name,
        "duration": duration,
        "result_count": result_count
    })
    
    return benchmark_data