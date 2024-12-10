from nodes import node1_data, node2_data, node3_data



"""
Transaction 1: Enroll a new student
"""
def enroll_student(student_id, student_name, class_id, advisor_id):
    return [
        {"resource": f"students-{student_id}", "action": lambda: insert_student(student_id, student_name, class_id, advisor_id)},
        {"resource": f"classes-{class_id}", "action": lambda: update_class(class_id)},
        {"resource": f"status-{student_id}-{class_id}", "action": lambda: insert_status(student_id, class_id)},
    ]

def insert_student(student_id, name, class_id, advisor_id):
    if any(student["student_id"] == student_id for student in node1_data["students"]):
        print(f"Student {student_id} already exists.")
        return False
    node1_data["students"].append({"student_id": student_id, "name": name, "class_id": class_id, "advisor_id": advisor_id})
    return True

def update_class(class_id):
    cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == class_id), None)
    if not cls:
        print(f"Class {class_id} does not exist.")
        return False
    cls["enrolled"] += 1
    return True

def insert_status(student_id, class_id):
    if class_id != []:
        node3_data["status"].append({"student_id": student_id, "class_id": class_id, "status": "enrolled"})
    return True



"""
Transaction 2: Assign advisor to student
"""
def assign_advisor(student_id, student_name, class_id, advisor_id, professor_name):
    professor_id = read_professors(professor_name)
    return [
        {"resource": f"students-{student_id}", "action": lambda: insert_student(student_id, student_name, class_id, advisor_id)},
        {"resource": f"advisor-{student_id}-{professor_id}", "action": lambda: insert_advisor(student_id, professor_id)}
    ]

### insert_student defined in T1

def read_professors(professor_name):
    prof = next((prof for prof in node1_data["professors"] if prof["name"] == professor_name), None)
    if not prof:
        print(f"Professor {professor_name} does not exist.")
        return False
    return prof["professor_id"]

def insert_advisor(student_id, professor_id):
    node1_data["advisor"].append({"student_id": student_id, "professor_id": professor_id})
    return True



"""
Transaction 3: Add professor
"""
def add_professor(professor_id, professor_name, class_id, advisee_id):
    return [
        {"resource": f"professors-{professor_id}", "action": lambda: insert_professors(professor_id, professor_name, class_id)},
        {"resource": f"advisor-{advisee_id}-{professor_id}", "action": lambda: insert_advisor(advisee_id, professor_id)}
    ]

def insert_professors(professor_id, professor_name, class_id):
    node1_data["professors"].append({"professor_id": professor_id, "name": professor_name, "class_id": class_id})
    return True



"""
Transaction 4: Add student if class is open
"""
def add_student(class_id, student_id, student_name, classes_id, advisor_id):
    if read_classes(class_id):
        return [
            {"resource": f"students-{student_id}", "action": lambda: insert_student(student_id, student_name, classes_id, advisor_id)}
        ]
    return []

def read_classes(class_id):
    cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == class_id and cls["status"] == "open"), None)
    if not cls:
        print(f"Class {class_id} does not exist/not open.")
        return False
    return True


"""
Transaction 5: Enroll a waitlisted student
"""
def enroll_existing_student(student_id, class_id):
    return [
        {"resource": f"students-{student_id}", "action": lambda: update_student(student_id, class_id)},
        {"resource": f"classes-{class_id}", "action": lambda: update_class2(class_id)},
        {"resource": f"status-{student_id}-{class_id}", "action": lambda: update_status(student_id, class_id)},
    ]

def update_student(student_id, class_id):
    student = next((student for student in node1_data["students"] if student["student_id"] == student_id), None)
    if not student:
        print(f"Student {student_id} does not exist.")
        return False
    student["class_id"].append(class_id)
    return True

def update_class2(class_id):
    cls = next((cls for cls in node2_data["classes"] if cls["class_id"] == class_id), None)
    if not cls:
        print(f"Class {class_id} does not exist.")
        return False
    cls["enrolled"] += 1
    cls["waitlist"] += 1
    return True

def update_status(student_id, class_id):
    status = next((status for status in node3_data["status"] if status["student_id"] == student_id and status["class_id"] == class_id), None)
    if not status:
        print(f"Status {student_id}-{class_id} does not exist.")
        return False
    status["status"] = "enrolled"
    return True