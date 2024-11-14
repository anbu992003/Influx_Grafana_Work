import time

# Record the start time
start_time = time.time()
print("Start Time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))

# Simulate a process (e.g., code block to measure)
time.sleep(2)  # Replace with your code block

# Record the end time
end_time = time.time()
print("End Time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))

# Calculate the elapsed time
elapsed_time = end_time - start_time
print("Elapsed Time:", elapsed_time, "seconds")
