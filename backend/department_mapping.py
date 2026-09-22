# ==================================
# DEPARTMENT -> EMAIL MAPPING
# ==================================

DEPARTMENT_TO_EMAIL = {

    # Current clustering departments
    "Customer Success":
        "kssinnarkar@gmail.com",

    "Engineering":
        "kssinnarkar@gmail.com",

    "Billing & Accounts":
        "kssinnarkar@gmail.com",

    "Product & Engineering":
        "kssinnarkar@gmail.com",

    "Product":
        "kssinnarkar@gmail.com",

    # Existing / rule-based departments
    "Network":
        "kssinnarkar@gmail.com",

    "Payments":
        "kssinnarkar@gmail.com",

    "Authentication":
        "kssinnarkar@gmail.com",

    "Billing":
        "kssinnarkar@gmail.com",

    "Delivery":
        "kssinnarkar@gmail.com",

    "General Support":
        "kssinnarkar@gmail.com",
}


# ==================================
# GET DEPARTMENT EMAIL
# ==================================

def get_department_email(department):

    return DEPARTMENT_TO_EMAIL.get(
        department,
        DEPARTMENT_TO_EMAIL["General Support"]
    )