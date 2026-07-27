# ==================================
# DEPARTMENT -> EMAIL MAPPING
# ==================================

DEPARTMENT_TO_EMAIL = {

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
    "kssinnarkar@gmail.com"

}


# ==================================
# GET DEPARTMENT EMAIL
# ==================================

def get_department_email(department):

    return DEPARTMENT_TO_EMAIL.get(

        department,

        DEPARTMENT_TO_EMAIL["General Support"]

    )