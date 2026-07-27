import smtplib

from email.mime.text import MIMEText

from email.mime.multipart import MIMEMultipart

from backend.department_mapping import (
    get_department_email
)

from backend.database.supabase import (

    get_cases,
    get_rca

)
# ==================================
# SHOULD ESCALATE ?
# ==================================

def should_escalate(

    severity

):

    """
    Determines whether an email
    should be escalated.

    Only HIGH and CRITICAL cases
    are escalated.
    """

    severity = severity.upper()

    return severity in (

        "HIGH",

        "CRITICAL"

    )
# ==================================
# BUILD EMAIL
# ==================================

def build_email(

    case,

    rca

):

    """
    Builds subject and body
    for the escalation email.
    """

    subject = (

        f"[{rca['severity']}] "

        f"InsightDesk Escalation "

        f"| Case ID : {case['case_id']}"

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

{case["department"]}


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

{chr(10).join(
f"- {item}"
for item in rca["source_evidence"]
)}


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

    body,

    sender,

    password

):

    """
    Sends escalation emails
    using SMTP.
    """

    try:

        message = MIMEMultipart()

        message["From"] = sender

        message["To"] = receiver

        message["Subject"] = subject


        message.attach(

            MIMEText(

                body,

                "plain"

            )

        )


        server = smtplib.SMTP(

            "smtp.gmail.com",

            587

        )

        server.starttls()


        server.login(

            sender,

            password

        )


        server.sendmail(

            sender,

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
# ESCALATE CASE
# ==================================

def escalate_case(

    case_id,

    sender,

    password

):

    """
    Main email escalation function.
    """

    # ------------------------
    # Fetch Case
    # ------------------------

    cases = get_cases()


    case = next(

        (

            item

            for item in cases

            if item["case_id"] == case_id

        ),

        None

    )


    if not case:

        print(

            "Case not found."

        )

        return False


    # ------------------------
    # Fetch RCA
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
    # Severity Check
    # ------------------------

    if not should_escalate(

        rca["severity"]

    ):

        print(

            "Severity below escalation threshold."

        )

        return False


    # ------------------------
    # Get Department Email
    # ------------------------

    receiver = get_department_email(

        case["department"]

    )


    # ------------------------
    # Build Email
    # ------------------------

    subject, body = build_email(

        case,

        rca

    )


    # ------------------------
    # Send Email
    # ------------------------

    return send_email(

        receiver,

        subject,

        body,

        sender,

        password

    )