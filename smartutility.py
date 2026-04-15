import time
import random
from multiprocessing import Pool
import matplotlib.pyplot as plt

# 1. The Core Billing Logic
def calculate_bill(units):

    for _ in range(100):
        x = units * 0.218

    bill = 0
    if units <= 200:
        bill = units * 0.218
    elif units <= 300:
        bill = (200 * 0.218) + ((units - 200) * 0.334)
    elif units <= 600:
        bill = (200 * 0.218) + (100 * 0.334) + ((units - 300) * 0.516)
    else:
        bill = (200 * 0.218) + (100 * 0.334) + (300 * 0.516) + ((units - 600) * 0.546)
    return round(bill, 2)

# 2. Worker function for Parallel Processing
def process_household(house_data):
    house_id, units, area = house_data
    bill_amount = calculate_bill(units)
    return {"id": house_id, "area": area, "bill": bill_amount, "units": units}

if __name__ == "__main__":
    # --- DATA GENERATION ---
    print("Generating data for 1,000,000 households...")
    households = []
    for i in range(1000000):
        # Assigning random units and area (Urban vs Rural)
        area = "Urban" if random.random() > 0.5 else "Rural"
        units = random.randint(300, 1200) if area == "Urban" else random.randint(50,600) # Random consumption
        households.append((i, units, area))

    # --- SEQUENTIAL PROCESSING (Slow) ---
    print("\nStarting Sequential Processing...")
    start_seq = time.time()
    sequential_results = [process_household(h) for h in households]
    end_seq = time.time()
    seq_time = end_seq - start_seq

    # --- PART B: COMPARING CORE COUNTS ---
    print("\n--- Testing Parallel Performance ---")
    core_counts = [2, 4] # Testing 2 cores and 4 cores
    parallel_times = []

    for cores in core_counts:
        print(f"Running with {cores} cores...")
        start_p = time.time()
        with Pool(processes=cores) as pool:
            results = pool.map(process_household, households)
        duration = time.time() - start_p
        parallel_times.append(duration)
        print(f"Time for {cores} cores: {duration:.4f}s")

    # --- FINAL REPORT ---
    best_parallel = min(parallel_times)
    print("\n" + "="*30)
    print("FINAL STATISTICAL REPORT")
    print("="*30)
    
    urban_bills = [r['bill'] for r in results if r['area'] == "Urban"]
    rural_bills = [r['bill'] for r in results if r['area'] == "Rural"]
    
    print(f"Sequential Time: {seq_time:.4f}s")
    print(f"Best Parallel Time: {best_parallel:.4f}s")
    print(f"Speed Improvement: {((seq_time - best_parallel) / seq_time) * 100:.2f}% faster")
    print(f"Urban Avg Bill: RM {sum(urban_bills)/len(urban_bills):.2f}")
    print(f"Rural Avg Bill: RM {sum(rural_bills)/len(rural_bills):.2f}")

    # --- PART C: VISUALIZATION ---
    print("\nGenerating Graphs...")
    
    # Graph 1: Performance Comparison
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    labels = ['Sequential', '2 Cores', '4 Cores']
    times = [seq_time, parallel_times[0], parallel_times[1]]
    plt.bar(labels, times, color=['red', 'orange', 'green'])
    plt.title('Processing Speed (Lower is Better)')
    plt.ylabel('Seconds')

    # Graph 2: Urban vs Rural
    plt.subplot(1, 2, 2)
    plt.pie([sum(urban_bills), sum(rural_bills)], labels=['Urban Revenue', 'Rural Revenue'], autopct='%1.1f%%')
    plt.title('Revenue Distribution')

    plt.tight_layout()
    plt.show()