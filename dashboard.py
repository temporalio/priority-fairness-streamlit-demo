import asyncio
import random
import time
from datetime import datetime

import streamlit as st
from temporalio.client import Client
from temporalio.common import Priority
from temporalio.envconfig import ClientConfig

from workflows.order_workflow import OrderWorkflow, ProcessOrderInput

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
PRIORITY_LABELS = {1: "High", 3: "Medium", 5: "Low"}
PRIORITY_COLORS = {1: RED_400, 3: YELLOW_400, 5: BLUE_500}

TENANT_COLORS = {
    "tenant-big": RED_400,
    "tenant-mid": YELLOW_400,
    "tenant-small": BLUE_500,
}
TENANT_COUNTS = {"tenant-big": 30, "tenant-mid": 10, "tenant-small": 5}


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


def _start_one(client, priority, tenant, use_priority=False, use_fairness=False):
    batch_id = st.session_state["batch_id"]
    seq = _next_seq()
    order_id = f"ORD-{batch_id}-{seq:03d}"
    wf_id = f"order-{order_id}"

    start_kwargs = dict(id=wf_id, task_queue=TASK_QUEUE)
    if use_priority or use_fairness:
        pri_kwargs = {}
        if use_priority:
            pri_kwargs["priority_key"] = priority
        if use_fairness:
            pri_kwargs["fairness_key"] = tenant
        start_kwargs["priority"] = Priority(**pri_kwargs)

    run_async(
        client.start_workflow(
            OrderWorkflow.run,
            ProcessOrderInput(order_id, tenant, priority),
            **start_kwargs,
        )
    )
    return {
        "id": wf_id,
        "order_id": order_id,
        "priority": priority,
        "tenant": tenant,
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
        priority = random.choice([1, 3, 5])
        workflows.append(
            _start_one(client, priority, f"tenant-{i}", use_priority=use_priority)
        )
    st.session_state["workflows"] = workflows


def start_fairness_batch(client, total_count, use_fairness):
    _reset_batch()
    st.session_state["tab"] = "fairness"
    big = max(1, int(total_count * 0.6))
    mid = max(1, int(total_count * 0.25))
    small = max(1, total_count - big - mid)
    workflows = []
    for _ in range(big):
        workflows.append(_start_one(client, 3, "tenant-big", use_fairness=use_fairness))
    for _ in range(mid):
        workflows.append(_start_one(client, 3, "tenant-mid", use_fairness=use_fairness))
    for _ in range(small):
        workflows.append(_start_one(client, 3, "tenant-small", use_fairness=use_fairness))
    st.session_state["workflows"] = workflows


def add_priority_workflows(client, count, priority, use_priority):
    existing = st.session_state.get("workflows", [])
    for _ in range(count):
        existing.append(
            _start_one(client, priority, "tenant-x", use_priority=use_priority)
        )
    st.session_state["workflows"] = existing


def add_fairness_workflows(client, count, tenant, use_fairness):
    existing = st.session_state.get("workflows", [])
    for _ in range(count):
        existing.append(
            _start_one(client, 3, tenant, use_fairness=use_fairness)
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
    pri = wf.get("priority", 3)
    color = PRIORITY_COLORS.get(pri, TEXT_SUBTLE)
    return (
        f'<div title="{wf["order_id"]} P{pri}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:{RADIUS}; "
        f"display:inline-flex; align-items:center; justify-content:center; "
        f"margin:2px; font-size:10px; font-weight:600; color:{SURFACE_PRIMARY}; "
        f'font-family:{FONT_MONO};">'
        f"{pri}"
        f"</div>"
    )


def render_fairness_chiclet(wf):
    tenant = wf.get("tenant", "unknown")
    color = TENANT_COLORS.get(tenant, TEXT_SUBTLE)
    label = tenant.split("-")[-1][0].upper() if "-" in tenant else "?"
    return (
        f'<div title="{wf["order_id"]} {tenant}" style="'
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
    st.set_page_config(page_title="Task Queue Priority & Fairness", layout="wide")
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

                count = st.slider("Workflows", 10, 50, 30, key="pri_count")
                use_priority = st.toggle("Enable priority", value=True, key="pri_toggle")

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

                for pri, label in [(1, "P1 High"), (3, "P3 Medium"), (5, "P5 Low")]:
                    color = PRIORITY_COLORS[pri]
                    dot_col, btn_col = st.columns([0.12, 0.88])
                    dot_col.markdown(
                        f'<div style="width:14px; height:14px; background:{color}; '
                        f'border-radius:{RADIUS}; margin-top:10px;"></div>',
                        unsafe_allow_html=True,
                    )
                    if btn_col.button(f"+ {label}", key=f"pri_add_{pri}"):
                        _ensure_pri_batch()
                        add_priority_workflows(client, add_count, pri, use_priority)

            title = "Priority" if use_priority else "FIFO (no priority)"
            st.markdown(f"## {title}")

            workflows = st.session_state.get("workflows", [])
            if workflows:
                workflows = poll_statuses(client, workflows)
                st.session_state["workflows"] = workflows

                if use_priority:
                    sort_fn = lambda wf: (wf["priority"], wf["submitted_at"])
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

                fair_count = st.slider("Workflows", 10, 50, 30, key="fair_count")
                use_fairness = st.toggle("Enable fairness", value=True, key="fair_toggle")

                if st.session_state.pop("_fair_start", False):
                    with st.spinner("Starting workflows..."):
                        start_fairness_batch(client, fair_count, use_fairness)

                st.divider()
                st.markdown("**Add to batch**")
                add_count_f = st.slider("Count to add", 1, 20, 5, key="fair_add_count")

                def _ensure_fair_batch():
                    if "workflows" not in st.session_state or st.session_state.get("tab") != "fairness":
                        _reset_batch()
                        st.session_state["tab"] = "fairness"

                for tenant, label in [("tenant-big", "Big"), ("tenant-mid", "Mid"), ("tenant-small", "Small")]:
                    color = TENANT_COLORS[tenant]
                    dot_col, btn_col = st.columns([0.12, 0.88])
                    dot_col.markdown(
                        f'<div style="width:14px; height:14px; background:{color}; '
                        f'border-radius:{RADIUS}; margin-top:10px;"></div>',
                        unsafe_allow_html=True,
                    )
                    if btn_col.button(f"+ {label}", key=f"fair_add_{tenant}"):
                        _ensure_fair_batch()
                        add_fairness_workflows(client, add_count_f, tenant, use_fairness)

            title = "Fairness" if use_fairness else "No Fairness (noisy neighbor)"
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
