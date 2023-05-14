import matplotlib.pyplot as plt

# Memory usage data
memory_roja = [332.36, 409.43, 484.02, 558.10, 635.28]
memory_soja = [292.09, 292.03, 291.06, 292.62, 324.21]

# Elapsed time data
time_roja = [26.64, 29.37, 30.60, 30.51, 30.88]
time_soja = [14.49, 13.60, 15.02, 13.73, 14.64]

# Row count data
row_count = [10000, 20000, 30000, 40000, 50000]

# Plot Execution time vs Table (R) Size
plt.plot(row_count, time_roja, label='ROJA', color='blue')
plt.plot(row_count, time_soja, label='SOJA', color='red')
plt.xlabel('Table (R) Size (number of rows)')
plt.ylabel('Execution Time (seconds)')
plt.title('Execution Time vs Table (R) Size')
plt.legend()
plt.show()

# Plot memory usage vs Table (R) Size
plt.plot(row_count, memory_roja, label='ROJA', color='blue')
plt.plot(row_count, memory_soja, label='SOJA', color='red')
plt.xlabel('Table (R) Size (number of rows)')
plt.ylabel('Memory Usage (MB)')
plt.title('Memory Usage vs Table (R) Size')
plt.legend()
plt.show()

# Memory usage data
memory_roja = [469.00, 487.58, 498.54, 508.96, 516.69, 521.32]
memory_soja = [288.53, 296.27, 292.68, 292.97, 288.55, 288.56]

# Elapsed time data
time_roja = [29.77, 30.77, 29.49, 32.38, 32.40, 28.79]
time_soja = [14.43, 13.64, 14.26, 13.78, 13.94, 14.21]

# Selectivity ratio data
selectivity_ratio = [50, 60, 70, 80, 90, 100]


# Plot memory usage
plt.plot(selectivity_ratio, memory_roja, label='ROJA', color='blue')
plt.plot(selectivity_ratio, memory_soja, label='SOJA', color='red')
plt.xlabel('Selectivity Ratio')
plt.ylabel('Memory Usage (MB)')
plt.title('Memory Usage vs Selectivity Ratio')
plt.legend()
plt.show()

# Plot Execution time
plt.plot(selectivity_ratio, time_roja, label='ROJA', color='blue')
plt.plot(selectivity_ratio, time_soja, label='SOJA', color='red')
plt.xlabel('Selectivity Ratio')
plt.ylabel('Execution time (s)')
plt.title('Memory Usage vs Selectivity Ratio')
plt.legend()
plt.show()