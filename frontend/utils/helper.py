"""HTML rendering helpers."""

from utils.constants import DEMO_METRICS


def badge(text: str, kind: str = "info") -> str:
    return f'<span class="id-badge id-badge-{kind}">{text}</span>'


def format_number(value: int) -> str:
    return f"{value:,}"


def render_topbar() -> str:
    m = DEMO_METRICS
    return f"""
    <div class="id-topbar">
        <div class="id-topbar-brand">
            <strong>InsightDesk</strong>
            <span>Vision Helpdesk · complaint intelligence</span>
        </div>
        <div class="id-topbar-stats">
            <div class="id-topbar-stat"><b>{format_number(m['complaints'])}</b> complaints</div>
            <div class="id-topbar-stat"><b>{m['cases']}</b> cases</div>
            <div class="id-topbar-stat"><b>{m['anger_score']}</b> anger</div>
            <div class="id-topbar-stat danger"><b>{m['spikes']}</b> spikes</div>
            <div class="id-topbar-stat"><b>{m['topics']}</b> topics</div>
        </div>
        <div class="id-topbar-pills">
            <span class="id-pill id-pill-live">Live</span>
            <span class="id-pill id-pill-spike">{m['spikes']} spikes</span>
        </div>
    </div>
    """
