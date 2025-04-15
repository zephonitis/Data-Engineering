# Step 1: Read data from the source file
with open(r'C:\Users\ashwi\OneDrive\Desktop\DE\NewNames.txt', 'r') as source_file:
    data = source_file.readlines()

# Step 2: Process the data (filter names starting with 'A')
filtered_data = [name.strip() for name in data if name.startswith('A')]

# Step 3: Write the filtered data to the destination file
with open(r'C:\Users\ashwi\OneDrive\Desktop\DE\filtered_names.txt', 'w') as destination_file:
    for name in filtered_data:
        destination_file.write(name + '\n')

print("Pipeline executed successfully! Check 'filtered_names.txt'.")