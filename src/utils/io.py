# src/utils/io.py

import csv

def load_csv(file_path):
    with open(file_path, mode='r') as file:
        return [row for row in csv.DictReader(file)]

def save_csv(file_path, data, fieldnames):
    with open(file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
