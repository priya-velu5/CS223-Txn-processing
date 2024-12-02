# src/nodes.py
import pandas as pd 
# Node 1: Frequently accessed tables
node1_data = {
    "students": [],
    "professors": [],
    "advisor": []
}

# Node 2: Class-related tables
node2_data = {
    "classes": []
}

# Node 3: Status-related tables
node3_data = {
    "status": [],
}

# Function to initialize data from CSV

def initialize_nodes():
    node1_data["students"] = pd.read_csv("data/students.csv")
    #print(node1_data["students"]['student_id'])
    node2_data["classes"] = pd.read_csv("data/classes.csv")
    #node2_data["students"] = pd.read_csv("data/students.csv")
    node3_data["status"] = pd.read_csv("data/status.csv")
    node1_data["professors"] = pd.read_csv("data/professors.csv")
    node1_data["advisor"] = pd.read_csv("data/advisor.csv")
