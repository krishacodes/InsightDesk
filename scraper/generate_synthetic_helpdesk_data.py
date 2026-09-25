"""
Synthetic helpdesk-software complaint generator (v2 - scaled).

Produces complaints across the 5 existing department categories
(Engineering, Authentication, Payments, Customer Success, Platform)
with NO real product/company names, a single default user_id
(per the "dedup is purely semantic" decision), and multiple
phrasing variants per underlying issue so duplicate-detection
calibration has known ground truth to work against.

v2 adds:
  - more issues per department
  - more hand-written phrasing variants per issue
  - an opener/closer wrapping pass to multiply volume without
    producing literally-identical rows (each "repeat" gets a
    different natural framing, simulating different people/times
    reporting the same underlying issue)

Outputs two files:
  1. synthetic_complaints.csv   -> matches the real ingestion schema
  2. synthetic_ground_truth.csv -> complaint_ref -> issue_id mapping,
                                    for building the calibration set
                                    later. NOT part of the DB schema,
                                    kept separate.
"""

import csv
import random
from datetime import datetime, timedelta

random.seed(42)

DEFAULT_USER_ID = "pre_auth_default"

# ---------------------------------------------------------------
# Issue bank
# ---------------------------------------------------------------

ISSUES = {

    "Authentication": [
        ("login_otp_not_received", [
            "I never received the OTP when trying to log in.",
            "The verification code never arrived on my phone, so I can't sign in.",
            "OTP delivery is broken, I've waited 20 minutes and nothing came through.",
            "Two-factor login is stuck because the code never shows up.",
            "Signing in fails every time because the SMS code doesn't arrive.",
            "No OTP text message has come through after multiple retries.",
        ]),
        ("password_reset_broken", [
            "The password reset link in the email doesn't work, it just shows an error page.",
            "I clicked forgot password and the reset link is broken.",
            "Password reset email arrives but the link leads to a blank screen.",
            "Can't reset my password, the link expires instantly.",
            "Reset password flow throws a 404 as soon as I click the emailed link.",
        ]),
        ("session_logs_out_randomly", [
            "I keep getting logged out randomly in the middle of work.",
            "My session expires every few minutes even though I'm active.",
            "The app signs me out on its own, forcing me to log back in constantly.",
            "Random logouts are happening throughout the day, very disruptive.",
            "Session keeps dying even while I'm actively typing.",
        ]),
        ("sso_login_fails", [
            "Single sign-on through our company account fails with an authentication error.",
            "SSO login just spins and never completes.",
            "Trying to log in via our identity provider throws a token error.",
            "SAML login redirects back with an invalid token message.",
        ]),
        ("account_locked_incorrectly", [
            "My account got locked after one failed login attempt, which seems wrong.",
            "I was locked out of my account even though I entered the correct password.",
            "Account lockout triggered incorrectly and support hasn't unlocked it yet.",
            "Got flagged as suspicious and locked out for no real reason.",
        ]),
        ("mfa_setup_fails", [
            "Setting up two-factor authentication fails at the QR code step.",
            "The authenticator app won't pair no matter how many times I scan the code.",
            "MFA enrollment keeps timing out before I can finish setup.",
        ]),
        ("cant_change_email", [
            "I can't update the email address linked to my account.",
            "Changing my login email fails with a generic error every time.",
        ]),
    ],

    "Payments": [
        ("double_charged", [
            "I was charged twice for the same subscription this month.",
            "My card shows two separate charges for one invoice.",
            "Billing deducted the amount twice in the same billing cycle.",
            "There's a duplicate charge on my statement for this plan.",
            "Got billed two times for a single month of service.",
        ]),
        ("refund_not_processed", [
            "I cancelled weeks ago and still haven't received my refund.",
            "The refund was approved but the money never came back to my account.",
            "Refund status shows completed but I don't see the amount in my bank.",
            "Still waiting on a refund that was promised over two weeks ago.",
            "Refund request was accepted but nothing has actually been credited.",
        ]),
        ("invoice_missing_details", [
            "The invoice doesn't show a proper tax breakdown for accounting purposes.",
            "Generated invoices are missing the company billing address.",
            "Invoice PDF is incomplete, some line items are blank.",
            "Tax ID isn't showing up anywhere on the downloaded invoice.",
        ]),
        ("subscription_upgrade_failed", [
            "Upgrading my plan keeps failing at the payment step.",
            "I tried to move to a higher tier and the transaction errors out.",
            "Plan upgrade charged my card but didn't actually upgrade the account.",
            "Upgrade button just spins forever and never completes.",
        ]),
        ("wallet_balance_incorrect", [
            "My account wallet balance is showing an incorrect amount.",
            "The credit balance doesn't match what I actually topped up.",
            "Wallet shows less credit than what was added last week.",
        ]),
        ("card_declined_incorrectly", [
            "My card keeps getting declined even though there are sufficient funds.",
            "Payment fails at checkout despite the card working everywhere else.",
        ]),
        ("cannot_cancel_subscription", [
            "There's no way to actually cancel my subscription from the settings page.",
            "The cancel button on the billing page does nothing when clicked.",
        ]),
    ],

    "Engineering": [
        ("app_crashes_on_open", [
            "The application crashes immediately every time I open it.",
            "App closes itself right after launch, can't get past the loading screen.",
            "It crashes on startup on my device, happens every single time.",
            "The moment I open the app it force-closes.",
            "App refuses to open, just crashes back to the home screen.",
        ]),
        ("dashboard_extremely_slow", [
            "The dashboard takes over a minute to load basic data.",
            "Everything is painfully slow, loading a single page takes forever.",
            "Performance has gotten really bad, pages hang for a long time before loading.",
            "Dashboard is lagging badly, barely usable during peak hours.",
            "Loading times have gotten unbearable over the last few weeks.",
        ]),
        ("sync_not_updating", [
            "Changes I make aren't syncing across devices.",
            "Data updated on desktop doesn't show up on mobile until much later.",
            "Sync between the web app and mobile app is broken.",
            "Records updated by a teammate don't reflect on my screen without a manual refresh.",
            "Cross-device sync just isn't happening anymore.",
        ]),
        ("update_broke_feature", [
            "The latest update broke a feature that was working fine before.",
            "After updating the app, a core feature stopped working entirely.",
            "This version introduced a bug that wasn't there before the update.",
            "Something in the newest release broke a workflow I rely on daily.",
        ]),
        ("connection_drops_frequently", [
            "The connection keeps dropping while I'm using the app.",
            "I keep getting disconnected and have to reload the page constantly.",
            "Network connection drops randomly during active use.",
        ]),
        ("search_returns_nothing", [
            "Search returns no results even for terms I know exist in my tickets.",
            "The search bar just comes back empty no matter what I type.",
        ]),
        ("attachments_fail_to_upload", [
            "File attachments fail to upload no matter the file size.",
            "Uploading a screenshot to a ticket just hangs and never finishes.",
        ]),
    ],

    "Customer Success": [
        ("no_response_from_support", [
            "I submitted a support ticket three days ago and haven't heard back.",
            "No one has responded to my request for help yet.",
            "Support hasn't replied to my message even after multiple follow-ups.",
            "I'm still waiting on any response from the support team.",
            "Radio silence from support since I opened this ticket.",
        ]),
        ("agent_unhelpful", [
            "The support agent I spoke to didn't actually solve my problem.",
            "Chat support just gave generic answers that didn't address my issue.",
            "The agent closed my ticket without actually fixing anything.",
            "Got a copy-pasted response that had nothing to do with my actual question.",
        ]),
        ("ticket_reopened_repeatedly", [
            "My ticket keeps getting closed before the issue is actually resolved.",
            "Support marks the ticket as resolved but the problem is still there.",
            "I have to keep reopening the same ticket because it's never really fixed.",
        ]),
        ("live_chat_unavailable", [
            "Live chat support shows as available but no one ever responds.",
            "The chat widget just sits there with no agent picking it up.",
        ]),
        ("escalation_ignored", [
            "I asked to escalate this issue and nothing has happened since.",
            "Requested a manager review three times with no follow-up at all.",
        ]),
    ],

    "Platform": [
        ("export_fails", [
            "Exporting reports to CSV fails every time I try.",
            "The export button doesn't do anything, no file gets downloaded.",
            "Trying to export data throws an error and nothing downloads.",
            "CSV export has been broken for me the past few days.",
            "Export just spins indefinitely and never produces a file.",
        ]),
        ("api_integration_broken", [
            "Our API integration stopped working after the last platform update.",
            "API calls are returning errors that weren't happening before.",
            "The webhook integration isn't firing events anymore.",
            "API responses started returning malformed data out of nowhere.",
        ]),
        ("permissions_not_saving", [
            "Permission changes I make for team members don't save.",
            "I set a teammate's access level but it reverts back on its own.",
            "Role permissions keep resetting to default after I configure them.",
        ]),
        ("notifications_not_sent", [
            "I'm not getting notified when a new ticket is assigned to me.",
            "Notification alerts for updates have completely stopped coming through.",
            "No alerts are sent when a ticket status changes, I have to check manually.",
            "Push notifications just stopped working a week ago.",
        ]),
        ("analytics_data_wrong", [
            "The analytics dashboard is showing numbers that don't match reality.",
            "Report totals in the dashboard don't add up correctly.",
            "Analytics figures seem to be miscalculated compared to raw data.",
        ]),
        ("cant_bulk_edit", [
            "There's no way to bulk update multiple tickets at once, only one at a time.",
            "Bulk actions on the tickets list just don't apply to anything selected.",
        ]),
    ],
}

# ---------------------------------------------------------------
# Wrapping phrases: used to multiply volume without producing
# literally identical rows. Each repeat of a variant gets a
# random (possibly empty) opener and/or closer.
# ---------------------------------------------------------------

OPENERS = [
    "", "", "",  # empty = no opener, keep some rows plain
    "Update: ",
    "Still happening - ",
    "Reporting this again: ",
    "Following up on this: ",
    "This has come up again. ",
    "Just ran into this once more: ",
]

CLOSERS = [
    "", "", "",
    " This is affecting my daily work.",
    " Please look into this soon.",
    " Happening consistently, not a one-off.",
    " Can someone confirm this is being looked at?",
]


def wrap_variant(base_text):
    opener = random.choice(OPENERS)
    closer = random.choice(CLOSERS)
    text = opener + base_text + closer
    return text.strip()


# ---------------------------------------------------------------
# Timestamp generation
# ---------------------------------------------------------------

START_DATE = datetime(2026, 3, 1)
END_DATE = datetime(2026, 5, 30)
SPIKE_ISSUE = "sync_not_updating"
SPIKE_WINDOW_START = datetime(2026, 4, 20)
SPIKE_WINDOW_END = datetime(2026, 4, 24)


def random_baseline_date():
    delta_days = (END_DATE - START_DATE).days
    return START_DATE + timedelta(
        days=random.randint(0, delta_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def random_spike_date():
    delta = (SPIKE_WINDOW_END - SPIKE_WINDOW_START).total_seconds()
    return SPIKE_WINDOW_START + timedelta(seconds=random.randint(0, int(delta)))


# ---------------------------------------------------------------
# Generate
# ---------------------------------------------------------------

rows = []
ground_truth = []
complaint_ref = 1

# Repeats per issue: how many times each variant gets reused
# (with wrapping) to build up realistic volume per issue.
NORMAL_REPEATS = 4
SPIKE_REPEATS = 8

for department, issues in ISSUES.items():
    for issue_id, variants in issues:
        n_repeats = SPIKE_REPEATS if issue_id == SPIKE_ISSUE else NORMAL_REPEATS

        for _ in range(n_repeats):
            for base_variant in variants:
                text = wrap_variant(base_variant)

                if issue_id == SPIKE_ISSUE:
                    ts = random_spike_date()
                else:
                    ts = random_baseline_date()

                rows.append({
                    "complaint_text": text,
                    "rating": random.choice([1, 1, 2, 2, 3]),
                    "date": ts.strftime("%Y-%m-%d"),
                    "source": "synthetic",
                    "product": "Generic Helpdesk Software",
                    "company_size": "",
                    "user_id": DEFAULT_USER_ID,
                    "is_synthetic": True,
                })
                ground_truth.append({
                    "complaint_ref": complaint_ref,
                    "department": department,
                    "issue_id": issue_id,
                    "complaint_text": text,
                })
                complaint_ref += 1

combined = list(zip(rows, ground_truth))
random.shuffle(combined)
rows, ground_truth = zip(*combined)

with open("data/synthetic_complaints.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "complaint_text", "rating", "date", "source", "product",
        "company_size", "user_id", "is_synthetic"
    ])
    writer.writeheader()
    writer.writerows(rows)

with open("data/synthetic_ground_truth.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "complaint_ref", "department", "issue_id", "complaint_text"
    ])
    writer.writeheader()
    writer.writerows(ground_truth)

n_issues = sum(len(v) for v in ISSUES.values())
print(f"Generated {len(rows)} synthetic complaints across {n_issues} distinct issues.")
print(f"Departments: {list(ISSUES.keys())}")
print(f"Spike issue '{SPIKE_ISSUE}' concentrated in {SPIKE_WINDOW_START.date()} - {SPIKE_WINDOW_END.date()}")