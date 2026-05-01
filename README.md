# ⚡ Parallelized Smart Utility Billing System ⚡

<p align="center">
  <img src="logo.png" width="350" title="Logo Projek">
</p>

### 📝 *ITT440 - INDIVIDUAL ASSIGNMENT*

**👨‍🎓 NAME : SITI SAIDATUL SYAHIRAH BINTI HISAMUDIN**

**🎓 STUDENT ID : 2025427336**

**👥 GROUP : M3CS2554C**

**YOUTUBE LINK : [youtube](https://youtu.be/ibz1cFCJ8Lo)**

---

## 📝 Project Overview
This project is an advanced simulation of a utility billing system designed to demonstrate the performance differences between **Sequential**, **Multi-threading**, and **Parallel Processing** (Multiprocessing). It generates large-scale household data, calculates electricity bills based on tiered tariffs, and exports comprehensive reports.

---

## 💻 System Requirements
To run this application smoothly, ensure your system meets the following:
* **Language:** Python 3.8 or higher.
* **Operating System:** Windows, macOS, or Linux.
* **Hardware:** Multi-core CPU (Minimum 4 cores recommended for Parallel gains).
* **Libraries:** `matplotlib` (for data visualization)
    * `multiprocessing` (built-in)
    * `threading` (built-in)

---

## 🛠️ Installation Steps

### 1. Clone the Repository:
```bash
git clone [https://github.com/YourUsername/Smart_Bil_Utility.git](https://github.com/YourUsername/Smart_Bil_Utility.git)
cd Smart_Bil_Utility
```

### 2. Install Dependencies:
The project uses `matplotlib` for generating performance charts. Install it via the terminal:
```bash
pip install matplotlib
```

### 3. Run the Application: 
```bash
python smart_utility_bil.py
```

---

## 🚀 How to Run the Program

Follow these steps to interact with the billing simulation:

1.  **Launch the Script:**
    Open your terminal in VS Code and run:
    ```bash
    python smart_utility_bil.py
    ```

2.  **Enter Input Volume:**
    When prompted, enter the number of households. To see the best performance from Parallel processing, try:
    * `100000` (Small test)
    * `500000` (Medium load)
    * `1000000` (Stress test)

3.  **Configure Distribution:**
    Select how the data is generated:
    * `1` - Mostly Urban (Higher consumption)
    * `2` - Mostly Rural (Lower consumption)
    * `3` - Balanced Split

4.  **Analyze the Summary Report:**
    The system will output a table showing the **Sequential**, **Threading**, and **Parallel** durations.

5.  **Navigate the Interactive Menu:**
    * Enter `1`: Opens a window displaying the **Execution Time Comparison** (Bar Chart) and **Household Distribution** (Pie Chart).
    * Enter `2`: Safely exits the system.

---

## 📊 Performance Benchmarks (Sample I/O)

The data below represents actual test runs. Note the significant reduction in time when moving from Sequential to Parallel execution.

| Households | Sequential Time | Threading Time | Parallel Time | Speedup (%) |
| :--- | :---: | :---: | :---: | :---: |
| **100,000** | 2.18s | 1.55s | 0.96s | **56%** |
| **500,000** | 10.96s | 8.01s | 4.88s | **55%** |
| **1,000,000** | 22.30s | 16.51s | 9.94s | **55%** |

---

## 📸 Screenshots

Below are the execution results for the three test categories, showing the significant performance boost as the data volume increases.

### 🔹 Category 1: 100,000 Households
**Processing Summary:** Optimized speedup of ~56%.
| Console Output | Visual Analytics |
| :--- | :--- |
| ![OUTPUT 100K](OUTPUT_100K.jpeg) | ![GRAPH 100K](GRAPH_100K.jpeg) |

---

### 🔹 Category 2: 500,000 Households
**Processing Summary:** Efficiency remains high at ~55% speedup.
| Console Output | Visual Analytics |
| :--- | :--- |
| ![OUTPUT 500K](OUTPUT_500K.jpeg) | ![GRAPH 500K](GRAPH_500K.jpeg) |

---

### 🔹 Category 3: 1,000,000 Households
**Processing Summary:** Handling massive data with ~55% optimization.
| Console Output | Visual Analytics |
| :--- | :--- |
| ![OUTPUT 1J](OUTPUT_1J.jpeg) | ![GRAPH 1J](GRAPH_1J.jpeg) |

---

## 📂 Source Code Highlight

The core of this system is the **Multiprocessing Pool**, which utilizes the full power of your CPU cores.


<<<<<<< HEAD
* [**smart_utility_bil.py**](smart_utility_bil.py)
=======
* [**smart_utility_bil.py**](smart_utility_bil.py)
>>>>>>> b0615af6be52cb24536fe10feadeb06cd90f3bce