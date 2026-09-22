import os
import smtplib

from dotenv import load_dotenv

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from backend.database.supabase import (
    get_case,
    get_topic,
    get_rca
)



from backend.department_mapping import (

    get_department_email

)


# ==================================
# LOAD ENVIRONMENT VARIABLES
# ==================================

load_dotenv()


EMAIL_ID = os.getenv(
    "EMAIL_ID"
)

EMAIL_PASSWORD = os.getenv(
    "EMAIL_PASSWORD"
)

SMTP_SERVER = os.getenv(
    "SMTP_SERVER"
)

SMTP_PORT = int(
    os.getenv(
        "SMTP_PORT"
    )
)


# ==================================
# SHOULD ESCALATE ?
# ==================================

def should_escalate(
        severity:str
):
    """
    Determines whether the case
    should be escalated.

    Only HIGH and CRITICAL
    severity cases are escalated.
    """

    severity = severity.upper()

    return severity in (
        "MEDIUM",
        
        "HIGH",

        "CRITICAL"

    )


# ==================================
# BUILD EMAIL
# ==================================

def build_email(

        case,
        rca,
        department

):
    """
    Builds the email subject
    and email body.
    """

    subject = (

        f"[{rca['severity']}] "

        f"InsightDesk Escalation "

        f"| Case ID : {case['case_id']}"

    )


    source_evidence = "\n".join(

        [

            f"- {item}"

            for item in

            rca["source_evidence"]

        ]

    )


    body = f"""

------------------------------------------------

INSIGHTDESK

AUTOMATED EMAIL ESCALATION

------------------------------------------------


CASE ID

{case["case_id"]}


------------------------------------------------


DEPARTMENT

{department}


------------------------------------------------


SEVERITY

{rca["severity"]}


------------------------------------------------


CONFIDENCE SCORE

{rca["confidence"]*100:.0f}%


------------------------------------------------


REPRESENTATIVE COMPLAINT

{case["representative_text"]}


------------------------------------------------


PROBABLE CAUSE

{rca["probable_cause"]}


------------------------------------------------


RECOMMENDED ACTION

{rca["recommended_action"]}


------------------------------------------------


SOURCE EVIDENCE

{source_evidence}


------------------------------------------------


MODEL USED

{rca["model_used"]}


------------------------------------------------


GENERATED AT

{rca["generated_at"]}


------------------------------------------------


Generated Automatically by

InsightDesk V2

AI Complaint Intelligence Platform


------------------------------------------------


"""

    return subject, body
# ==================================
# SEND EMAIL
# ==================================

def send_email(
        receiver,
        subject,
        body
):
    """
    Sends email using SMTP.
    """

    try:

        message = MIMEMultipart()

        message["From"] = EMAIL_ID

        message["To"] = receiver

        message["Subject"] = subject


        message.attach(

            MIMEText(

                body,

                "plain"

            )

        )


        server = smtplib.SMTP(

            SMTP_SERVER,

            SMTP_PORT

        )

        server.starttls()


        server.login(

            EMAIL_ID,

            EMAIL_PASSWORD

        )


        server.sendmail(

            EMAIL_ID,

            receiver,

            message.as_string()

        )


        server.quit()


        print(

            "Email Sent Successfully."

        )

        return True


    except Exception as e:

        print(

            f"Email Failed : {e}"

        )

        return False



# ==================================
# MAIN ORCHESTRATOR
# ==================================

def escalate_case(
        case_id: int
):
    """
    Performs automated
    email escalation.
    """

    # ------------------------
    # FETCH CASE
    # ------------------------

    case = get_case(

        case_id

    )

    if not case:

        print(

            "Case not found."

        )

        return False


    # ------------------------
    # FETCH RCA
    # ------------------------

    rca = get_rca(

        case_id

    )

    if not rca:

        print(

            "No RCA available."

        )

        return False


    # ------------------------
    # CHECK SEVERITY
    # ------------------------

    if not should_escalate(

        rca["severity"]

    ):

        print(

            "Severity below escalation threshold."

        )

        return False


    # ------------------------
    # FETCH TOPIC
    # ------------------------
    print("\nCASE OBJECT\n")

    print(case)

    print("\nTOPIC ID\n")

    print(

        case.get(
            "topic_id"
        )

    )
    topic = get_topic(

        case["topic_id"]

    )

    if not topic:

        print(

            "Topic not found."

        )

        return False


    # ------------------------
    # FETCH DEPARTMENT
    # ------------------------

    department = topic[

        "department"

    ]


    # ------------------------
    # FETCH DEPARTMENT EMAIL
    # ------------------------

    receiver = get_department_email(

        department

    )

    if not receiver:

        print(

            "Department email not found."

        )

        return False


    # ------------------------
    # BUILD EMAIL
    # ------------------------

    subject, body = build_email(

        case,
        rca,
        department

    )


    # ------------------------
    # SEND EMAIL
    # ------------------------

    return send_email(

        receiver,
        subject,
        body

    )
# ==================================
# TESTING
# ==================================

if __name__=="__main__":

    case = get_case(

        71

    )

    print("\nCASE OBJECT\n")

    print(case)

    print("\n")

    print(

        "TOPIC ID :",

        case.get(

            "topic_id"

        )

    )