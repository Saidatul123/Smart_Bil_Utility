import time
import random
import threading
import csv
from multiprocessing import Pool, cpu_count
import matplotlib.pyplot as plt

# 1. THE CORE BILLING LOGIC
def calculate_bill(kwh_usage):
    """Calculates electricity bill based on tiered tariff rates."""
    try:
        if kwh_usage < 0: return 0.0
        
        # --- CALIBRATION PART ---
        # Mathematical load to challenge Parallel processing (Multiprocessing)
        count = 0
        for i in range(10000):
            count += i
            
        # Small delay to allow Threading to context switch (Concurrency demo)
        time.sleep(0.0005) 

        bill = 0
        if kwh_usage <= 200:
            bill = kwh_usage * 0.218
        elif kwh_usage <= 300:
            bill = (200 * 0.218) + ((kwh_usage - 200) * 0.334)
        elif kwh_usage <= 600:
            bill = (200 * 0.218) + (100 * 0.334) + ((kwh_usage - 300) * 0.516)
        else:
            bill = (200 * 0.218) + (100 * 0.334) + (300 * 0.516) + ((kwh_usage - 600) * 0.546)
        return round(bill, 2)
    except Exception:
        return 0.0

# 2. WORKER FUNCTION
def process_household(house_data):
    house_num, kwh_usage, area = house_data
    bill_amount = calculate_bill(kwh_usage)
    return {
        "house_number": house_num + 1, 
        "area": area, 
        "usage_kwh": kwh_usage, 
        "total_bill": bill_amount
    }

def get_valid_input(prompt, options=None, is_int=False):
    while True:
        user_input = input(prompt).strip()
        if is_int:
            try:
                val = int(user_input)
                if val > 0: return val
                print("(!) Please enter a positive number.")
            except ValueError: print("(!) Invalid input. Please enter a number.")
        elif options:
            if user_input.lower() in options: return user_input.lower()
            print(f"(!) Invalid choice. Please choose from {options}.")
        else: return user_input

def show_visuals(seq_duration, th_duration, par_duration, urban_count, rural_count):
    plt.figure(figsize=(12, 5))
    
    # Execution Time Comparison Chart
    plt.subplot(1, 2, 1)
    plt.bar(['Sequential', 'Threading', 'Parallel'], [seq_duration, th_duration, par_duration], color=['#e74c3c', '#f1c40f', '#2ecc71'])
    plt.title('Execution Time Comparison (Seconds)')
    plt.ylabel('Time (s)')

    # Household Distribution Chart
    plt.subplot(1, 2, 2)
    plt.pie([urban_count, rural_count], labels=['Urban', 'Rural'], autopct='%1.1f%%', startangle=140, colors=['#3498db', '#f39c12'])
    plt.title('Household Distribution')
    
    plt.tight_layout()
    print("\n[System] Displaying graphs... Close the window to return to the menu.")
    plt.show()

def run_billing_system():
    # Recommended: Use 10,000 households or more to clearly see the Parallel speedup
    total_houses = get_valid_input("\nEnter number of households to process (Recommended: 10000): ", is_int=True)
    
    cores = cpu_count()
    print(f"[System Information] Detected {cores} CPU cores.")
    print(f"[System Information] Parallel processing will utilize all available cores for maximum speed.")
    
    print("\nSelect Area Distribution:")
    print("[1] Mostly Urban (80% Urban)")
    print("[2] Mostly Rural (80% Rural)")
    print("[3] Balanced (50/50 Split)")
    dist_choice = get_valid_input("Choice: ", options=['1', '2', '3'])

    # --- STEP 1: DATA GENERATION ---
    print(f"\n[1/4] Generating raw household data...")
    households = []
    for i in range(total_houses):
        if dist_choice == '1': area_type = "Urban" if random.random() > 0.2 else "Rural"
        elif dist_choice == '2': area_type = "Urban" if random.random() > 0.8 else "Rural"
        else: area_type = "Urban" if random.random() > 0.5 else "Rural"
        
        # Logic: Rural users consume less kWh than Urban users on average
        kwh = random.randint(50, 650) if area_type == "Rural" else random.randint(651, 1200)
        households.append((i, kwh, area_type))

    # --- STEP 2: PROCESSING (COMPARING TECHNIQUES) ---
    print("[2/4] Calculating bills using three different techniques...")
    
    # --- Sequential Processing ---
    start_seq = time.time()
    _ = [process_household(h) for h in households]
    seq_duration = time.time() - start_seq

    # --- Threading (Concurrent) ---
    start_th = time.time()
    th_results = []
    def thread_worker(chunk):
        for item in chunk: th_results.append(process_household(item))
    
    num_threads = 4
    chunk_size = len(households) // num_threads
    threads = []
    for i in range(num_threads):
        chunk = households[i*chunk_size : (i+1)*chunk_size] if i < num_threads-1 else households[i*chunk_size:]
        t = threading.Thread(target=thread_worker, args=(chunk,))
        threads.append(t)
        t.start()
    for t in threads: t.join()
    th_duration = time.time() - start_th

    # --- Multiprocessing (Parallel) ---
    start_par = time.time()
    with Pool() as pool:
        final_results = pool.map(process_household, households)
    par_duration = time.time() - start_par

    # --- STEP 3: EXPORT TO CSV ---
    print("[3/4] Exporting results to 'Utility_Bills_Report.csv'...")
    urban_list = sorted([r for r in final_results if r['area'] == "Urban"], key=lambda x: x['house_number'])
    rural_list = sorted([r for r in final_results if r['area'] == "Rural"], key=lambda x: x['house_number'])
    
    with open('Utility_Bills_Report.csv', mode='w', newline='') as file:
        fieldnames = ["house_number", "area", "usage_kwh", "total_bill"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        file.write("# URBAN CATEGORY\n")
        writer.writerows(urban_list)
        file.write("\n# RURAL CATEGORY\n")
        writer.writerows(rural_list)

    # --- STEP 4: REPORT SUMMARY ---
    total_revenue = sum(r['total_bill'] for r in final_results)
    avg_urban = sum(r['total_bill'] for r in urban_list)/len(urban_list) if urban_list else 0
    avg_rural = sum(r['total_bill'] for r in rural_list)/len(rural_list) if rural_list else 0

    print("\n" + "="*60)
    print("           SMART BILLING SYSTEM: SUMMARY REPORT")
    print("="*60)
    print(f"Total Households Processed      : {total_houses:,}")
    print(f"Average Bill (Urban)            : RM {avg_urban:.2f}")
    print(f"Average Bill (Rural)            : RM {avg_rural:.2f}")
    print(f"TOTAL REVENUE COLLECTED         : RM {total_revenue:,.2f}")
    print("-" * 60)
    print(f"Sequential Duration             : {seq_duration:.4f}s")
    print(f"Threading Duration              : {th_duration:.4f}s")
    print(f"Parallel Duration               : {par_duration:.4f}s")
    print(f"Optimization Speedup (Parallel) : {((seq_duration-par_duration)/seq_duration)*100:.2f}%")
    print("="*60)

    # --- INTERACTIVE MENU ---
    while True:
        print("\n[MAIN MENU] Options: [1] View Visual Graphs [2] New Calculation [3] Exit")
        choice = get_valid_input("Enter your choice: ", options=['1', '2', '3'])
        if choice == '1': 
            show_visuals(seq_duration, th_duration, par_duration, len(urban_list), len(rural_list))
        elif choice == '2': 
            return True # Restart the loop
        elif choice == '3': 
            print("Shutting down system. Goodbye!")
            return False

if __name__ == "__main__":
    print("============================================================")
    print("           PARALLELIZED SMART BILLING SYSTEM")
    print("============================================================")
    is_running = True
    while is_running: 
        is_running = run_billing_system()