import time
import random
from multiprocessing import Pool, cpu_count
import matplotlib.pyplot as plt

# 1. THE CORE BILLING LOGIC
def calculate_bill(units):
    """Calculates bill based on tiered tariff rates."""
    # Artificial load to simulate heavy processing
    for _ in range(1000):
        _ = units * 0.218

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

# 2. WORKER FUNCTION FOR PARALLELISM
def process_household(house_data):
    house_id, units, area = house_data
    bill_amount = calculate_bill(units)
    return {"id": house_id, "area": area, "bill": bill_amount, "units": units}

def run_billing_system():
    # --- USER INPUT ---
    try:
        total_houses = int(input("\nEnter number of households to calculate: "))
        
        print("\nChoose Area Distribution: [1] Mostly Urban [2] Mostly Rural [3] Balanced")
        dist_choice = input("Choice: ")

        print("\nChoose Target Average Bill: [1] < RM 70 [2] > RM 200 [3] > RM 500")
        bill_choice = input("Choice: ")
    except ValueError:
        print("Invalid input. Please enter numbers only.")
        return

    # --- DATA GENERATION ---
    print(f"\n[Step 1] Generating data for {total_houses:,} households...")
    households = []
    for i in range(total_houses):
        # Determine Area
        if dist_choice == '1': area_type = "Urban" if random.random() > 0.2 else "Rural"
        elif dist_choice == '2': area_type = "Urban" if random.random() > 0.8 else "Rural"
        else: area_type = "Urban" if random.random() > 0.5 else "Rural"
        
        # Determine Units based on average bill choice
        if bill_choice == '1':   # Less than RM 70
            units = random.randint(50, 250)
        elif bill_choice == '2': # More than RM 200
            units = random.randint(550, 800)
        elif bill_choice == '3': # More than RM 500
            units = random.randint(1200, 2000)
        else:
            units = random.randint(300, 1200) if area_type == "Urban" else random.randint(50, 600)

        households.append((i, units, area_type))

    # --- SEQUENTIAL PROCESSING ---
    print("[Step 2] Starting Sequential Processing...")
    start_seq = time.time()
    _ = [process_household(h) for h in households] 
    seq_duration = time.time() - start_seq
    print(f"Sequential Time: {seq_duration:.4f}s")

    # --- PARALLEL PROCESSING ---
    print(f"[Step 3] Starting Parallel Processing (Using {cpu_count()} CPU Cores)...")
    start_par = time.time()
    with Pool() as pool:
        parallel_results = pool.map(process_household, households)
    par_duration = time.time() - start_par
    print(f"Parallel Time: {par_duration:.4f}s")

    # --- STATISTICAL REPORT ---
    urban_bills = [r['bill'] for r in parallel_results if r['area'] == "Urban"]
    rural_bills = [r['bill'] for r in parallel_results if r['area'] == "Rural"]
    all_bills = [r['bill'] for r in parallel_results]
    avg_total = sum(all_bills)/len(all_bills)
    
    print("\n" + "="*40)
    print("      SYSTEM PERFORMANCE REPORT")
    print("="*40)
    print(f"Total Households    : {total_houses:,}")
    print(f"Global Average Bill : RM {avg_total:.2f}")
    print(f"Sequential Duration : {seq_duration:.4f}s")
    print(f"Parallel Duration   : {par_duration:.4f}s")
    print(f"Speed Improvement   : {((seq_duration - par_duration) / seq_duration) * 100:.2f}%")
    print("="*40)

    # --- VISUALIZATION ---
    print("\nDisplaying graphs... Close graph window to continue.")
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.bar(['Sequential', 'Parallel'], [seq_duration, par_duration], color=['red', 'green'])
    plt.title('Performance: Sequential vs Parallel')
    plt.ylabel('Time (Seconds)')

    plt.subplot(1, 2, 2)
    plt.pie([sum(urban_bills), sum(rural_bills)], labels=['Urban', 'Rural'], autopct='%1.1f%%', colors=['#3498db', '#e67e22'])
    plt.title('Revenue Distribution by Area Type')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("WELCOME TO THE PARALLELIZED SMART BILLING SYSTEM")
    while True:
        run_billing_system()
        cont = input("\nDo you want to continue calculate? (yes/no): ").lower()
        if cont != 'yes' and cont != 'y':
            print("Exiting system. Thank you!")
            break