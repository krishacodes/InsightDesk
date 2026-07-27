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
        "internet",
        "network",
        "connection",
        "slow",
        "latency",
        "loading",
        "server",
        "downtime",
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
        "authenticate",
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
        "money",
        "charged",
        "deducted",
    ],

    "Customer Success": [
        "support",
        "agent",
        "response",
        "ticket",
        "service",
        "customer",
        "help",
        "chat",
        "assistance",
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
    Assign department using keyword scoring.

    Parameters
    ----------
    keywords : list[str]

    Returns
    -------
    str
        Department name.
    """

    keywords = [

        keyword.lower()

        for keyword in keywords

    ]


    scores = {

        department: 0

        for department in DEPARTMENT_KEYWORDS

    }


    # -------------------------------
    # Calculate department scores
    # -------------------------------

    for keyword in keywords:

        for department, triggers in (
            DEPARTMENT_KEYWORDS.items()
        ):

            if keyword in triggers:

                scores[department] += 1


    # -------------------------------
    # No matches found
    # -------------------------------

    highest_score = max(
        scores.values()
    )

    if highest_score == 0:

        return "General"


    # -------------------------------
    # Return best matching department
    # -------------------------------

    for department, score in (
        scores.items()
    ):

        if score == highest_score:

            return department
def assign_department(keywords: list[str]) -> str:
    """
    Public department assignment function.
    Currently uses keyword rules.
    Later this will call the Groq LLM.
    """
    return assign_department_by_keywords(keywords)