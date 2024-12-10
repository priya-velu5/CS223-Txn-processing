# src/nodes.py
import pandas as pd 
import numpy as np

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

# Function to process ids
def process_ids(ids):
    if type(ids) == float:
        return []
    elif ';' in str(ids):
        return str(ids).split(';')
    else:
        return [str(ids)]

def get_node_for_resource(resource):
    if "students" in resource or "advisor" in resource:
        return "node1"
    elif "classes" in resource:
        return "node2"
    elif "status" in resource or "professors" in resource:
        return "node3"
    else:
        raise ValueError(f"Unknown resource: {resource}")

# Function to initialize data from CSV
def initialize_nodes():
    # process students data
    students_df = pd.read_csv("data/students.csv")
    students_df['class_id'] = students_df['class_id'].apply(process_ids)
    students_df['advisor_id'] = students_df['advisor_id'].apply(process_ids)
    node1_data["students"] = students_df.to_dict(orient="records")

    # process classes data
    classes_df = pd.read_csv("data/classes.csv")
    node2_data["classes"] = classes_df.to_dict(orient="records")
   
    # process status data
    status_df = pd.read_csv("data/status.csv")
    node3_data["status"]= status_df.to_dict(orient="records")

    # process professors data
    professors_df = pd.read_csv("data/professors.csv")
    professors_df['class_id'] = professors_df['class_id'].apply(process_ids)
    node1_data["professors"] = professors_df.to_dict(orient="records")

    # process advisor data
    advisor_df = pd.read_csv("data/advisor.csv")
    advisor_df['professor_id'] = advisor_df['professor_id'].apply(process_ids)
    node1_data["advisor"]= advisor_df.to_dict(orient="records")