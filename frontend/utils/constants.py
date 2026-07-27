"""Design tokens and demo data for InsightDesk dashboard."""

NAVY = "#0B1F3A"
NAVY_DEEP = "#081729"
NAVY_MID = "#13315C"
ACCENT = "#3B82F6"
ACCENT_LIGHT = "#7FB3E8"
WHITE = "#FFFFFF"
OFFWHITE = "#F0F4F8"
SIDEBAR_BG = "#E8EEF5"
BORDER = "#E2E8F0"
TEXT_MUTED = "#5B6B82"
TEXT_DARK = "#1A2332"
RED = "#DC2626"
RED_SOFT = "#FEE2E2"
RED_TEXT = "#B42318"
GREEN = "#067647"
GREEN_SOFT = "#E7F6EC"
ORANGE = "#B54708"
ORANGE_SOFT = "#FEF3E2"

MONITOR_PAGES = ["Overview", "Clusters", "Sentiment", "Spikes"]
AI_PAGES = ["Diagnostics", "Chatbot", "Admin"]
ALL_PAGES = MONITOR_PAGES + AI_PAGES

# Demo metrics (replace with API data later)
DEMO_METRICS = {
    "complaints": 1284,
    "complaints_delta": "+47 today",
    "cases": 9,
    "cases_spiking": "2 spiking",
    "anger_score": 0.71,
    "anger_model": "RoBERTa ↑",
    "topics": 6,
    "topics_model": "BERTopic",
    "spikes": 3,
}

DEMO_CLUSTERS = [
    {"rank": 1, "name": "App crashes", "count": 312, "status": "spike"},
    {"rank": 2, "name": "SLA breach", "count": 247, "status": "stable"},
    {"rank": 3, "name": "Login fail", "count": 218, "status": "spike"},
    {"rank": 4, "name": "Ticket routing", "count": 167, "status": "stable"},
]

DEMO_COMPLAINT_CHART = {
    "Day": ["M", "T", "W", "T", "F", "S", "S"],
    "Complaints": [320, 410, 550, 601, 715, 520, 910],
}

DEMO_POST_MORTEM = {
    "tag": "Android 14 · critical",
    "root_cause": (
        "App v3.2.1 WebView incompatibility. Crashes at list init on Android 14."
    ),
    "action": "Rollback v3.2.1 → v3.1.9 immediately.",
}

DEMO_BENCHMARKS = {
    "model": "RoBERTa",
    "macro_f1": 0.83,
    "latency": "310ms",
    "throughput": "3.2/sec",
    "z_score": "2.8 ↑",
}

DEMO_SPIKE_ALERT = {
    "title": "Spike active",
    "headline": "App crashes +68%",
    "meta": "312 users · 6 hrs",
}
