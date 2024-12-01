from src.nodes import node1_data, node2_data, node3_data



"""
Transaction 1: Enroll a new student
"""

# def enroll_student(s, c):
#     def hop1():
#         # Node2: Add student
#         if any(student["student_id"] == s for student in node2_data["students"]):
#             print(f"Student {s} already exists.")
#             return False
#         node2_data["students"].append({"student_id": s})
#         return True

#     def hop2():
#         # Node2: Update class enrollment
#         cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == c), None)
#         if not cls:
#             print(f"Class {c} does not exist.")
#             return False
#         cls["enrolled"] += 1
#         return True

#     def hop3():
#         # Node3: Update status
#         node3_data["status"].append({"student_id": s, "class_id": c, "status": "enrolled"})
#         return True

#     return [hop1, hop2, hop3]

def enroll_student(s, c):
    return [
        {"resource": f"students-{s}", "action": lambda: add_student(s)},
        {"resource": f"classes-{c}", "action": lambda: update_class(c)},
        {"resource": f"status-{s}-{c}", "action": lambda: update_status(s, c)},
    ]

def add_student(s):
    if any(student["student_id"] == s for student in node1_data["students"]):
        print(f"Student {s} already exists.")
        return False
    node1_data["students"].append({"student_id": s})
    return True

def update_class(c):
    cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == c), None)
    if not cls:
        print(f"Class {c} does not exist.")
        return False
    cls["enrolled"] += 1
    return True

def update_status(s, c):
    node3_data["status"].append({"student_id": s, "class_id": c, "status": "enrolled"})
    return True

"""
Transaction 2: Assign advisor to student
"""

def assign_advisor(s, advisor_name):
    def hop1():
        # Node1: Add student
        node1_data["students"].append({"student_id": s})
        return True

    def hop2():
        # Node3: Find professor ID
        professor = next((p for p in node3_data["professors"] if p["name"] == advisor_name), None)
        if not professor:
            print(f"Advisor {advisor_name} not found.")
            return False
        return professor["professor_id"]

    def hop3(professor_id):
        # Node1: Add to advisor table
        node1_data["advisor"].append({"student_id": s, "professor_id": professor_id})
        return True

    return [hop1, hop2, hop3]

"""
Transaction 3: Add professor
"""
def add_professor(p_id, p_name, m):
    def hop1():
        # Node3: Add professor
        node3_data["professors"].append({"professor_id": p_id, "name": p_name})
        return True

    def hop2():
        # Node1: Assign advisee
        node1_data["advisor"].append({"student_id": m, "professor_id": p_id})
        return True

    return [hop1, hop2]

"""
Transaction 4: Add student if class is open
"""
def add_student_if_class_open(s, c):
    def hop1():
        # Node2: Check class status
        cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == c), None)
        if not cls or cls["status"] != "open":
            print(f"Class {c} is not open.")
            return False
        return True

    def hop2():
        # Node1: Add student
        node1_data["students"].append({"student_id": s})
        return True

    return [hop1, hop2]

