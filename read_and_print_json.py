import json

with open("sample_data.json", "r") as file:
    data = json.load(file)
    
for row in data:
    print(row['name'],'-',row['role'])