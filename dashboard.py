import asyncio
import random
import time
from datetime import datetime

import streamlit as st
from temporalio.client import Client
from temporalio.common import Priority
from temporalio.envconfig import ClientConfig

from activities.handle_chat_turn import ChatTurnInput
from workflows.chat_turn_workflow import ChatTurnWorkflow

TASK_QUEUE = "priority-fairness-task-queue"

_loop = asyncio.new_event_loop()


def run_async(coro):
    return _loop.run_until_complete(coro)


# --- Holocene design tokens (dark mode) ---
# Palette
SLATE_300 = "#92A4C3"
SLATE_700 = "#465A78"
SLATE_800 = "#273860"
SLATE_900 = "#1E293B"
SLATE_950 = "#0F172A"
SPACE_BLACK = "#141414"
OFF_WHITE = "#F8FAFC"
RED_400 = "#FF6637"
YELLOW_400 = "#FEC321"
BLUE_500 = "#3B82F6"
GREEN_600 = "#00C05F"
INDIGO_500 = "#6173F3"
PURPLE_500 = "#8B5CF6"

# Semantic tokens
TEXT_PRIMARY = OFF_WHITE
TEXT_SECONDARY = SLATE_300
TEXT_SUBTLE = SLATE_700
SURFACE_BG = SPACE_BLACK
SURFACE_PRIMARY = "#000000"
SURFACE_SECONDARY = SLATE_950
BORDER_SECONDARY = SLATE_700
BORDER_SUBTLE = SLATE_800
BRAND = INDIGO_500
RADIUS = "4px"
FONT_SANS = "'Inter', sans-serif"
FONT_MONO = "'Noto Sans Mono', monospace"

# --- Feature colors mapped to Holocene palette ---
# Consumer tiers: Pro ($200/mo), Plus ($20/mo), Free
TIER_LABELS = {1: "Pro $200/mo", 3: "Plus $20/mo", 5: "Free"}
TIER_CODES = {1: "P1", 3: "P2", 5: "P3"}
TIER_COLORS = {1: RED_400, 3: YELLOW_400, 5: BLUE_500}

# Enterprise customers (within the Pro tier)
CUSTOMER_COLORS = {
    "bigcorp": RED_400,
    "midco": YELLOW_400,
    "startup": BLUE_500,
}
CUSTOMER_LABELS = {
    "bigcorp": "BigCorp",
    "midco": "MidCo",
    "startup": "Startup",
}
CUSTOMER_CODES = {
    "bigcorp": "B",
    "midco": "M",
    "startup": "S",
}
CUSTOMER_WEIGHTS = {
    "bigcorp": 10.0,
    "midco": 3.0,
    "startup": 1.0,
}


# --- Streamlit theme override ---
def apply_theme():
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Noto+Sans+Mono:wght@400;600&display=swap');

        .stApp, .stMarkdown, .stButton button,
        [data-testid="stMetricLabel"], [data-testid="stHeader"],
        section[data-testid="stSidebar"], h1, h2, h3, h4, p, label {{
            font-family: {FONT_SANS} !important;
        }}
        [data-testid="stMetricValue"], code, pre {{
            font-family: {FONT_MONO} !important;
        }}
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def get_client():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    return run_async(Client.connect(**config))


# --- Workflow management ---

def _next_seq():
    seq = st.session_state.get("wf_seq", 0)
    st.session_state["wf_seq"] = seq + 1
    return seq


def _start_one(client, tier, customer, use_priority=False, use_fairness=False, use_weights=False):
    batch_id = st.session_state["batch_id"]
    seq = _next_seq()
    turn_id = f"turn-{batch_id}-{seq:03d}"
    wf_id = f"chat-{turn_id}"

    start_kwargs = dict(id=wf_id, task_queue=TASK_QUEUE)
    if use_priority or use_fairness:
        pri_kwargs = {}
        if use_priority:
            pri_kwargs["priority_key"] = tier
        if use_fairness:
            pri_kwargs["fairness_key"] = customer
            if use_weights:
                pri_kwargs["fairness_weight"] = CUSTOMER_WEIGHTS.get(customer, 1.0)
        start_kwargs["priority"] = Priority(**pri_kwargs)

    run_async(
        client.start_workflow(
            ChatTurnWorkflow.run,
            ChatTurnInput(turn_id, customer, tier),
            **start_kwargs,
        )
    )
    return {
        "id": wf_id,
        "turn_id": turn_id,
        "tier": tier,
        "customer": customer,
        "submitted_at": datetime.now(),
    }


def _reset_batch():
    st.session_state["batch_id"] = int(time.time())
    st.session_state["wf_seq"] = 0
    st.session_state["workflows"] = []
    st.session_state["completed_order"] = []
    st.session_state["seen_completed"] = set()


def start_priority_batch(client, count, use_priority):
    _reset_batch()
    st.session_state["tab"] = "priority"
    workflows = []
    for i in range(count):
        tier = random.choice([1, 3, 5])
        workflows.append(
            _start_one(client, tier, f"customer-{i}", use_priority=use_priority)
        )
    st.session_state["workflows"] = workflows


def start_fairness_batch(client, total_count, use_fairness, use_weights=False):
    _reset_batch()
    st.session_state["tab"] = "fairness"
    big = max(1, int(total_count * 0.8))
    mid = max(1, int(total_count * 0.15))
    small = max(1, total_count - big - mid)
    customers = (
        ["bigcorp"] * big + ["midco"] * mid + ["startup"] * small
    )
    random.shuffle(customers)
    workflows = [
        _start_one(client, 1, customer, use_fairness=use_fairness, use_weights=use_weights)
        for customer in customers
    ]
    st.session_state["workflows"] = workflows


def add_priority_workflows(client, count, tier, use_priority):
    existing = st.session_state.get("workflows", [])
    for _ in range(count):
        existing.append(
            _start_one(client, tier, "customer-x", use_priority=use_priority)
        )
    st.session_state["workflows"] = existing


def add_fairness_workflows(client, count, customer, use_fairness, use_weights=False):
    existing = st.session_state.get("workflows", [])
    for _ in range(count):
        existing.append(
            _start_one(client, 1, customer, use_fairness=use_fairness, use_weights=use_weights)
        )
    st.session_state["workflows"] = existing


# --- Polling ---

def poll_statuses(client, workflows):
    async def _poll_all():
        async def _poll_one(wf):
            try:
                desc = await client.get_workflow_handle(wf["id"]).describe()
                status_name = desc.status.name if desc.status else "Unknown"
                if "COMPLETED" in status_name:
                    status = "Completed"
                else:
                    pending = desc.raw_description.pending_activities
                    if pending and pending[0].state == 2:  # STARTED
                        status = "Running"
                    else:
                        # SCHEDULED, no pending yet (workflow task), or other
                        status = "Queued"
            except Exception:
                status = "Queued"
            return {**wf, "status": status}

        return await asyncio.gather(*[_poll_one(wf) for wf in workflows])

    return list(run_async(_poll_all()))


# --- Rendering ---

def render_priority_chiclet(wf):
    tier = wf.get("tier", 3)
    color = TIER_COLORS.get(tier, TEXT_SUBTLE)
    label = TIER_CODES.get(tier, "?")
    return (
        f'<div title="{wf["turn_id"]} {TIER_LABELS.get(tier, "?")}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:{RADIUS}; "
        f"display:inline-flex; align-items:center; justify-content:center; "
        f"margin:2px; font-size:10px; font-weight:600; color:{SURFACE_PRIMARY}; "
        f'font-family:{FONT_MONO};">'
        f"{label}"
        f"</div>"
    )


def render_fairness_chiclet(wf):
    customer = wf.get("customer", "unknown")
    color = CUSTOMER_COLORS.get(customer, TEXT_SUBTLE)
    label = CUSTOMER_CODES.get(customer, "?")
    return (
        f'<div title="{wf["turn_id"]} {CUSTOMER_LABELS.get(customer, customer)}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:{RADIUS}; "
        f"display:inline-flex; align-items:center; justify-content:center; "
        f"margin:2px; font-size:10px; font-weight:600; color:{SURFACE_PRIMARY}; "
        f'font-family:{FONT_MONO};">'
        f"{label}"
        f"</div>"
    )


def render_swimlane(label, workflows, render_fn, empty_msg=""):
    count = len(workflows)
    chiclets = "".join(render_fn(wf) for wf in workflows)
    return (
        f'<div style="margin-bottom:16px;">'
        f'<div style="font-size:12px; font-weight:600; color:{TEXT_SECONDARY}; '
        f'margin-bottom:4px; font-family:{FONT_SANS}; text-transform:uppercase; letter-spacing:0.05em;">'
        f'{label} <span style="font-weight:400; color:{TEXT_SUBTLE};">({count})</span></div>'
        f'<div style="background:{SURFACE_SECONDARY}; border:1px solid {BORDER_SUBTLE}; border-radius:{RADIUS}; '
        f'min-height:36px; padding:4px; display:flex; flex-wrap:wrap; align-items:center;">'
        f'{chiclets if chiclets else f"<span style=&quot;color:{TEXT_SUBTLE}; font-size:12px; padding:4px;&quot;>{empty_msg}</span>"}'
        f"</div></div>"
    )


def render_all_swimlanes(workflows, sort_fn, render_fn):
    seen = st.session_state.get("seen_completed", set())
    completed_order = st.session_state.get("completed_order", [])
    for wf in workflows:
        if wf["status"] == "Completed" and wf["id"] not in seen:
            seen.add(wf["id"])
            completed_order.append(wf)
    st.session_state["seen_completed"] = seen
    st.session_state["completed_order"] = completed_order

    queued = sorted(
        [wf for wf in workflows if wf["status"] == "Queued"],
        key=sort_fn,
    )
    running = [wf for wf in workflows if wf["status"] == "Running"]
    completed = completed_order

    total = len(workflows)
    n_queued = len(queued)
    n_running = len(running)
    n_completed = len(completed)

    cols = st.columns(4)
    cols[0].metric("Total", total)
    cols[1].metric("Queued", n_queued)
    cols[2].metric("Running", n_running)
    cols[3].metric("Completed", n_completed)

    html = render_swimlane("QUEUED", queued, render_fn)
    html += render_swimlane("RUNNING", running, render_fn)
    html += render_swimlane("COMPLETED", completed, render_fn)
    st.markdown(html, unsafe_allow_html=True)

    return total, n_completed


# --- Main ---

def main():
    st.set_page_config(page_title="AI Assistant: Priority & Fairness", layout="wide")
    apply_theme()

    client = get_client()

    def on_tab_change():
        st.session_state.pop("workflows", None)
        st.session_state.pop("completed_order", None)
        st.session_state.pop("seen_completed", None)

    tab_priority, tab_fairness = st.tabs(
        ["Priority", "Fairness"],
        key="active_tab",
        on_change=on_tab_change,
    )

    active_tab = st.session_state.get("active_tab", "Priority")

    with tab_priority:
        if tab_priority.open:
            with st.sidebar:
                st.header("Priority Controls")

                if st.button("Start Batch", type="primary", key="pri_start"):
                    st.session_state["_pri_start"] = True

                count = st.slider("Chat turns", 10, 45, 30, key="pri_count")
                use_priority = st.toggle("Enable priority", value=False, key="pri_toggle")

                if st.session_state.pop("_pri_start", False):
                    with st.spinner("Starting workflows..."):
                        start_priority_batch(client, count, use_priority)

                st.divider()
                st.markdown("**Add to batch**")
                add_count = st.slider("Count to add", 1, 20, 5, key="pri_add_count")

                def _ensure_pri_batch():
                    if "workflows" not in st.session_state or st.session_state.get("tab") != "priority":
                        _reset_batch()
                        st.session_state["tab"] = "priority"

                for tier in [1, 3, 5]:
                    color = TIER_COLORS[tier]
                    code = TIER_CODES[tier]
                    tier_label = TIER_LABELS[tier]
                    dot_col, btn_col = st.columns([0.18, 0.82])
                    dot_col.markdown(
                        f'<div style="width:28px; height:28px; background:{color}; '
                        f'border-radius:{RADIUS}; margin-top:4px; '
                        f'display:flex; align-items:center; justify-content:center; '
                        f'font-size:11px; font-weight:600; color:{SURFACE_PRIMARY}; '
                        f'font-family:{FONT_MONO};">{code}</div>',
                        unsafe_allow_html=True,
                    )
                    if btn_col.button(f"+ {code} ({tier_label})", key=f"pri_add_{tier}"):
                        _ensure_pri_batch()
                        add_priority_workflows(client, add_count, tier, use_priority)

            title = "Priority: Pro > Plus > Free" if use_priority else "FIFO: Pro = Plus = Free"
            st.markdown(f"## {title}")

            workflows = st.session_state.get("workflows", [])
            if workflows:
                workflows = poll_statuses(client, workflows)
                st.session_state["workflows"] = workflows

                if use_priority:
                    sort_fn = lambda wf: (wf["tier"], wf["submitted_at"])
                else:
                    sort_fn = lambda wf: wf["submitted_at"]

                render_all_swimlanes(workflows, sort_fn, render_priority_chiclet)
            else:
                render_all_swimlanes([], lambda wf: wf["submitted_at"], render_priority_chiclet)

    with tab_fairness:
        if tab_fairness.open:
            with st.sidebar:
                st.header("Fairness Controls")

                if st.button("Start Batch", type="primary", key="fair_start"):
                    st.session_state["_fair_start"] = True

                fair_count = st.slider("Chat turns", 10, 45, 30, key="fair_count")
                use_fairness = st.toggle("Enable fairness", value=False, key="fair_toggle")
                use_weights = st.toggle(
                    "Use weights",
                    value=False,
                    key="fair_weights_toggle",
                    disabled=not use_fairness,
                )

                if st.session_state.pop("_fair_start", False):
                    with st.spinner("Starting workflows..."):
                        start_fairness_batch(client, fair_count, use_fairness, use_weights)

                st.divider()
                st.markdown("**Add to batch**")
                add_count_f = st.slider("Count to add", 1, 20, 5, key="fair_add_count")

                def _ensure_fair_batch():
                    if "workflows" not in st.session_state or st.session_state.get("tab") != "fairness":
                        _reset_batch()
                        st.session_state["tab"] = "fairness"

                for customer in ["bigcorp", "midco", "startup"]:
                    color = CUSTOMER_COLORS[customer]
                    code = CUSTOMER_CODES[customer]
                    cust_label = CUSTOMER_LABELS[customer]
                    dot_col, btn_col = st.columns([0.18, 0.82])
                    dot_col.markdown(
                        f'<div style="width:28px; height:28px; background:{color}; '
                        f'border-radius:{RADIUS}; margin-top:4px; '
                        f'display:flex; align-items:center; justify-content:center; '
                        f'font-size:11px; font-weight:600; color:{SURFACE_PRIMARY}; '
                        f'font-family:{FONT_MONO};">{code}</div>',
                        unsafe_allow_html=True,
                    )
                    if btn_col.button(f"+ {code} ({cust_label})", key=f"fair_add_{customer}"):
                        _ensure_fair_batch()
                        add_fairness_workflows(client, add_count_f, customer, use_fairness, use_weights)

            if use_fairness and use_weights:
                title = "Weighted Fairness: BigCorp Gets More, Startup Still Served"
            elif use_fairness:
                title = "Fairness: MidCo/Startup Protected"
            else:
                title = "FIFO: BigCorp Dominates"
            st.markdown(f"## {title}")

            workflows = st.session_state.get("workflows", [])
            if workflows:
                workflows = poll_statuses(client, workflows)
                st.session_state["workflows"] = workflows
                sort_fn = lambda wf: wf["submitted_at"]
                render_all_swimlanes(workflows, sort_fn, render_fairness_chiclet)
            else:
                render_all_swimlanes([], lambda wf: wf["submitted_at"], render_fairness_chiclet)

    # Auto-refresh only when there are active workflows
    workflows = st.session_state.get("workflows", [])
    if workflows:
        n_completed = len(st.session_state.get("completed_order", []))
        if n_completed < len(workflows):
            time.sleep(0.1)
            st.rerun()


if __name__ == "__main__":
    main()
