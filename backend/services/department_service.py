"""
Department Assignment Service

Responsible for assigning the appropriate department
to newly discovered BERTopic topics.
"""

DEPARTMENT_KEYWORDS = {

    "Engineering": [
        "bug",
        "crash",
        "error",
        "exception",
        "failure",
        "battery",
        "charging",
        "performance",
        "lag",
        "freeze",
        "android",
        "ios",
        "update",
    ],

    "Authentication": [
        "login",
        "password",
        "otp",
        "authentication",
        "signin",
        "signup",
        "verification",
        "token",
    ],

    "Payments": [
        "payment",
        "refund",
        "billing",
        "upi",
        "transaction",
        "invoice",
        "wallet",
        "subscription",
    ],

    "Customer Success": [
        "support",
        "agent",
        "response",
        "ticket",
        "service",
        "customer",
        "help",
    ],

    "Platform": [
        "dashboard",
        "analytics",
        "report",
        "export",
        "integration",
        "api",
    ],
}
def assign_department_by_keywords(keywords: list[str]) -> str:
    """
    Assign department using rule-based keyword matching.

    Parameters
    ----------
    keywords : list[str]

    Returns
    -------
    str
        Department name.
    """

    keywords = [k.lower() for k in keywords]

    for department, triggers in DEPARTMENT_KEYWORDS.items():

        for keyword in keywords:

            if keyword in triggers:
                return department

    return "General"
def assign_department(keywords: list[str]) -> str:
    """
    Public department assignment function.
    Currently uses keyword rules.
    Later this will call the Groq LLM.
    """
    return assign_department_by_keywords(keywords)