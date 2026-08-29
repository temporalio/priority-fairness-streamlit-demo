import asyncio
import html
import random
import time
from datetime import UTC, datetime

import streamlit as st
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import ListTaskQueuePartitionsRequest
from temporalio.common import Priority

from activities.handle_chat_turn import ChatTurnInput
from activities.long_running_task import LongRunningTaskInput
from demo_config import (
    CONCURRENCY_FAIRNESS_TASK_QUEUE,
    CONCURRENCY_TASK_QUEUE,
    CONCURRENCY_WORKER_IDENTITIES,
    CONCURRENCY_WORKER_SLOTS,
    connect_local_client,
    get_activity_task_queue_config,
    update_activity_task_queue_controls,
)
from workflows.chat_turn_workflow import ChatTurnWorkflow
from workflows.concurrency_workflow import ConcurrencyDemoWorkflow

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
    return run_async(connect_local_client(identity="priority-fairness-dashboard"))


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

    start_kwargs = {"id": wf_id, "task_queue": TASK_QUEUE}
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
        "submitted_at": datetime.now(UTC),
    }


def _start_concurrency_one(
    client,
    task_queue,
    customer,
    duration_seconds,
    use_fairness,
):
    batch_id = st.session_state["batch_id"]
    seq = _next_seq()
    task_id = f"task-{batch_id}-{seq:03d}"
    wf_id = f"concurrency-{task_id}"

    start_kwargs = {"id": wf_id, "task_queue": task_queue}
    if use_fairness:
        start_kwargs["priority"] = Priority(fairness_key=customer)

    run_async(
        client.start_workflow(
            ConcurrencyDemoWorkflow.run,
            LongRunningTaskInput(task_id, customer, duration_seconds),
            **start_kwargs,
        )
    )
    return {
        "id": wf_id,
        "turn_id": task_id,
        "tier": 1,
        "customer": customer,
        "submitted_at": datetime.now(UTC),
        "duration_seconds": duration_seconds,
        "worker_identity": "",
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


def start_concurrency_batch(client, total_count, duration_seconds):
    _reset_batch()
    st.session_state["tab"] = "concurrency"
    st.session_state["workflows"] = [
        _start_concurrency_one(
            client,
            CONCURRENCY_TASK_QUEUE,
            "workload",
            duration_seconds,
            False,
        )
        for _ in range(total_count)
    ]


def start_concurrency_fairness_batch(
    client,
    total_count,
    duration_seconds,
    use_fairness,
):
    _reset_batch()
    st.session_state["tab"] = "concurrency_fairness"
    big = max(1, int(total_count * 0.6))
    mid = max(1, int(total_count * 0.25))
    small = max(1, total_count - big - mid)
    customers = ["bigcorp"] * big + ["midco"] * mid + ["startup"] * small
    random.shuffle(customers)
    st.session_state["workflows"] = [
        _start_concurrency_one(
            client,
            CONCURRENCY_FAIRNESS_TASK_QUEUE,
            customer,
            duration_seconds,
            use_fairness,
        )
        for customer in customers
    ]


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
            worker_identity = wf.get("worker_identity", "")
            try:
                desc = await client.get_workflow_handle(wf["id"]).describe()
                status_name = desc.status.name if desc.status else "Unknown"
                if "COMPLETED" in status_name:
                    status = "Completed"
                else:
                    pending = desc.raw_description.pending_activities
                    if pending and pending[0].state == 2:  # STARTED
                        status = "Running"
                        worker_identity = (
                            pending[0].last_worker_identity or "assigning worker"
                        )
                    else:
                        # SCHEDULED, no pending yet (workflow task), or other
                        status = "Queued"
            except Exception:  # noqa: BLE001 - transient describe failures stay queued
                status = "Queued"
            return {
                **wf,
                "status": status,
                "worker_identity": worker_identity,
            }

        return await asyncio.gather(*[_poll_one(wf) for wf in workflows])

    return list(run_async(_poll_all()))


def get_activity_partition_count(client, task_queue):
    async def _get_partition_count():
        response = await client.workflow_service.list_task_queue_partitions(
            ListTaskQueuePartitionsRequest(
                namespace=client.namespace,
                task_queue=TaskQueue(name=task_queue),
            )
        )
        return len(response.activity_task_queue_partitions)

    try:
        return run_async(_get_partition_count())
    except Exception:  # noqa: BLE001 - keep the visualization usable while reconnecting
        return "—"


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


def render_concurrency_chiclet(wf):
    task_code = html.escape(wf["turn_id"].rsplit("-", 1)[-1])
    worker_identity = html.escape(wf.get("worker_identity") or "not assigned")
    title = html.escape(
        f'{wf["turn_id"]} · {wf.get("duration_seconds", 0):g}s · '
        f"{worker_identity}"
    )
    return (
        f'<div title="{title}" style="'
        f"width:28px; min-width:28px; height:28px; background:{BRAND}; border-radius:{RADIUS}; "
        f"display:inline-flex; align-items:center; justify-content:center; "
        f"margin:2px; padding:0; font-size:9px; font-weight:600; "
        f'color:{SURFACE_PRIMARY}; font-family:{FONT_MONO};">'
        f"{task_code}"
        f"</div>"
    )


def render_concurrency_fairness_chiclet(wf):
    customer = wf.get("customer", "unknown")
    color = CUSTOMER_COLORS.get(customer, TEXT_SUBTLE)
    customer_code = CUSTOMER_CODES.get(customer, "?")
    task_code = html.escape(wf["turn_id"].rsplit("-", 1)[-1])
    worker_identity = html.escape(wf.get("worker_identity") or "not assigned")
    title = html.escape(
        f'{wf["turn_id"]} · {CUSTOMER_LABELS.get(customer, customer)} · '
        f'{wf.get("duration_seconds", 0):g}s · {worker_identity}'
    )
    return (
        f'<div title="{title}" style="'
        f"width:38px; min-width:38px; height:28px; background:{color}; border-radius:{RADIUS}; "
        f"display:inline-flex; align-items:center; justify-content:center; gap:2px; "
        f"margin:2px; padding:0; font-size:9px; font-weight:600; "
        f'color:{SURFACE_PRIMARY}; font-family:{FONT_MONO};">'
        f"{customer_code}<span style=\"opacity:0.72\">{task_code}</span>"
        f"</div>"
    )


def render_swimlane(
    label,
    workflows,
    render_fn,
    empty_msg="",
    *,
    fixed_height=False,
):
    count = len(workflows)
    chiclets = "".join(render_fn(wf) for wf in workflows)
    lane_layout = (
        "height:40px; box-sizing:border-box; display:flex; flex-wrap:nowrap; "
        "align-items:center; overflow-x:auto; overflow-y:hidden;"
        if fixed_height
        else "min-height:36px; display:flex; flex-wrap:wrap; align-items:center;"
    )
    return (
        f'<div style="margin-bottom:16px;">'
        f'<div style="font-size:12px; font-weight:600; color:{TEXT_SECONDARY}; '
        f'margin-bottom:4px; font-family:{FONT_SANS}; text-transform:uppercase; letter-spacing:0.05em;">'
        f'{label} <span style="font-weight:400; color:{TEXT_SUBTLE};">({count})</span></div>'
        f'<div style="background:{SURFACE_SECONDARY}; border:1px solid {BORDER_SUBTLE}; border-radius:{RADIUS}; '
        f'padding:4px; {lane_layout}">'
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


def render_concurrency_swimlanes(workflows, render_fn):
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
        key=lambda wf: wf["submitted_at"],
    )
    running = [wf for wf in workflows if wf["status"] == "Running"]
    observed_workers = {
        wf.get("worker_identity") or "assigning worker" for wf in running
    }
    workers = list(CONCURRENCY_WORKER_IDENTITIES)
    workers.extend(sorted(observed_workers - set(workers)))

    cols = st.columns(4)
    cols[0].metric("Total", len(workflows))
    cols[1].metric("Queued", len(queued))
    cols[2].metric("Running", len(running))
    cols[3].metric("Completed", len(completed_order))

    lane_html = render_swimlane(
        "QUEUED",
        queued,
        render_fn,
        "No tasks waiting",
        fixed_height=True,
    )
    for worker_identity in workers:
        worker_tasks = [
            wf
            for wf in running
            if (wf.get("worker_identity") or "assigning worker")
            == worker_identity
        ]
        safe_worker_identity = html.escape(worker_identity)
        lane_html += render_swimlane(
            f"RUNNING · {safe_worker_identity} · {CONCURRENCY_WORKER_SLOTS} slots",
            worker_tasks,
            render_fn,
            "Idle",
            fixed_height=True,
        )
    lane_html += render_swimlane(
        "COMPLETED",
        completed_order,
        render_fn,
        "No tasks completed",
        fixed_height=True,
    )
    st.markdown(lane_html, unsafe_allow_html=True)

    return len(workflows), len(completed_order)


def render_task_queue_controls(key_prefix, header, task_queue):
    st.header(header)
    st.markdown("**Write configuration**")

    enable_concurrency = st.toggle(
        "Enable concurrency limit",
        value=False,
        key=f"{key_prefix}_toggle",
    )
    concurrency_limit = st.slider(
        "Maximum running activities",
        1,
        20,
        4,
        key=f"{key_prefix}_limit",
        disabled=not enable_concurrency,
    )
    queue_rps = st.slider(
        "Task queue RPS",
        0.5,
        10.0,
        3.0,
        0.5,
        format="%.2f",
        key=f"{key_prefix}_rps",
        help="The queue rate limiter can allow an initial burst before settling at this sustained rate.",
    )

    if st.button(
        "Apply controls",
        type="primary",
        key=f"{key_prefix}_apply",
    ):
        with st.spinner("Applying server controls..."):
            try:
                response = update_activity_task_queue_controls(
                    task_queue=task_queue,
                    requests_per_second=queue_rps,
                    concurrency_limit=(
                        concurrency_limit if enable_concurrency else None
                    ),
                )
                st.session_state[f"applied_{key_prefix}_controls"] = {
                    "enabled": enable_concurrency,
                    "limit": concurrency_limit if enable_concurrency else None,
                    "rps": queue_rps,
                    "response": response,
                }
                time.sleep(2.0)
            except RuntimeError as exc:
                st.error(str(exc))

    try:
        observed = get_activity_task_queue_config(task_queue)
        config = observed.get("config", {})
        observed_concurrency = config.get("queueConcurrencyLimit", {}).get(
            "concurrencyLimit"
        )
        observed_rps = config.get("queueRateLimit", {}).get("rateLimit")
        observed_state = {
            "enabled": observed_concurrency is not None,
            "limit": (
                observed_concurrency.get("concurrentTasks")
                if observed_concurrency
                else None
            ),
            "rps": (
                observed_rps.get("requestsPerSecond") if observed_rps else None
            ),
        }
        st.session_state[f"observed_{key_prefix}_controls"] = observed_state
    except RuntimeError:
        st.session_state.pop(f"observed_{key_prefix}_controls", None)


def render_concurrency_content(
    client,
    render_fn,
    page_title,
    cell_key=None,
    *,
    key_prefix,
    task_queue,
    fairness_enabled=None,
):
    observed = st.session_state.get(f"observed_{key_prefix}_controls")
    configured = observed or st.session_state.get(f"applied_{key_prefix}_controls")
    partition_count = get_activity_partition_count(client, task_queue)
    st.markdown(f"## {page_title}")
    if cell_key:
        st.markdown(cell_key)

    if configured:
        limit_text = (
            str(configured["limit"]) if configured["enabled"] else "off"
        )
        rps_text = (
            f'{float(configured["rps"]):.2f}'
            if configured["rps"] is not None
            else "off"
        )
        fairness_text = (
            f" · Fairness {'on' if fairness_enabled else 'off'}"
            if fairness_enabled is not None
            else ""
        )
        st.caption(
            f"Observed on `{task_queue}`: "
            f"task queue RPS {rps_text} · concurrency {limit_text}"
            f"{fairness_text} · partitions {partition_count}"
        )

    workflows = st.session_state.get("workflows", [])
    if workflows:
        workflows = poll_statuses(client, workflows)
        st.session_state["workflows"] = workflows
    render_concurrency_swimlanes(workflows, render_fn)


# --- Main ---

def main():
    st.set_page_config(
        page_title="AI Assistant: Priority, Fairness & Concurrency",
        layout="wide",
    )
    apply_theme()

    client = get_client()

    def on_tab_change():
        st.session_state.pop("workflows", None)
        st.session_state.pop("completed_order", None)
        st.session_state.pop("seen_completed", None)

    (
        tab_priority,
        tab_fairness,
        tab_concurrency,
        tab_concurrency_fairness,
    ) = st.tabs(
        ["Priority", "Fairness", "Concurrency", "Concurrency + Fairness"],
        key="active_tab",
        on_change=on_tab_change,
    )

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
                use_fairness = st.toggle("Enable Fairness", value=False, key="fair_toggle")
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

    with tab_concurrency:
        if tab_concurrency.open:
            with st.sidebar:
                render_task_queue_controls(
                    "concurrency",
                    "Concurrency Controls",
                    CONCURRENCY_TASK_QUEUE,
                )
                st.divider()
                st.markdown("**Workload**")
                concurrency_count = st.slider(
                    "Activities",
                    10,
                    60,
                    30,
                    key="concurrency_count",
                )
                duration_seconds = st.slider(
                    "Duration (seconds)",
                    2,
                    20,
                    8,
                    key="concurrency_duration",
                )
                if st.button(
                    "Start Batch",
                    type="primary",
                    key="concurrency_start",
                ):
                    with st.spinner("Starting long-running activities..."):
                        start_concurrency_batch(
                            client,
                            concurrency_count,
                            duration_seconds,
                        )

            render_concurrency_content(
                client,
                render_concurrency_chiclet,
                "Task Queue Concurrency",
                key_prefix="concurrency",
                task_queue=CONCURRENCY_TASK_QUEUE,
            )

    with tab_concurrency_fairness:
        if tab_concurrency_fairness.open:
            with st.sidebar:
                render_task_queue_controls(
                    "concurrency_fairness",
                    "Concurrency + Fairness Controls",
                    CONCURRENCY_FAIRNESS_TASK_QUEUE,
                )
                st.divider()
                st.markdown("**Multi-tenant workload**")
                concurrency_fairness_count = st.slider(
                    "Activities",
                    10,
                    60,
                    30,
                    key="concurrency_fairness_count",
                )
                concurrency_fairness_duration = st.slider(
                    "Duration (seconds)",
                    2,
                    20,
                    8,
                    key="concurrency_fairness_duration",
                )
                concurrency_fairness_enabled = st.toggle(
                    "Enable Fairness",
                    value=True,
                    key="concurrency_fairness_enabled",
                )
                if st.button(
                    "Start Batch",
                    type="primary",
                    key="concurrency_fairness_start",
                ):
                    with st.spinner("Starting multi-tenant activities..."):
                        start_concurrency_fairness_batch(
                            client,
                            concurrency_fairness_count,
                            concurrency_fairness_duration,
                            concurrency_fairness_enabled,
                        )

            render_concurrency_content(
                client,
                render_concurrency_fairness_chiclet,
                "Task Queue Concurrency + Fairness",
                key_prefix="concurrency_fairness",
                task_queue=CONCURRENCY_FAIRNESS_TASK_QUEUE,
                fairness_enabled=concurrency_fairness_enabled,
            )

    # Auto-refresh only when there are active workflows
    workflows = st.session_state.get("workflows", [])
    if workflows:
        n_completed = len(st.session_state.get("completed_order", []))
        if n_completed < len(workflows):
            time.sleep(1.0)
            st.rerun()


if __name__ == "__main__":
    main()
