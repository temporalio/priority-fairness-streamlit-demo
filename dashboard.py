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


# --- Config ---
PRIORITY_LABELS = {1: "High", 3: "Medium", 5: "Low"}
PRIORITY_COLORS = {1: "#ef4444", 3: "#f59e0b", 5: "#3b82f6"}

TENANT_COLORS = {
    "tenant-big": "#ef4444",
    "tenant-mid": "#f59e0b",
    "tenant-small": "#3b82f6",
}
TENANT_COUNTS = {"tenant-big": 30, "tenant-mid": 10, "tenant-small": 5}


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
    # Split roughly 60/25/15 to create an asymmetric noisy-neighbor distribution
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
    updated = []
    for wf in workflows:
        try:
            desc = run_async(client.get_workflow_handle(wf["id"]).describe())
            status_name = desc.status.name if desc.status else "Unknown"
            if "COMPLETED" in status_name:
                status = "Completed"
            else:
                pending = desc.raw_description.pending_activities
                if pending and pending[0].state == 1:
                    status = "Queued"
                elif pending and pending[0].state == 2:
                    status = "Running"
                else:
                    status = "Running"
        except Exception:
            status = "Queued"
        updated.append({**wf, "status": status})
    return updated


# --- Rendering ---

def render_priority_chiclet(wf):
    pri = wf.get("priority", 3)
    color = PRIORITY_COLORS.get(pri, "#888")
    return (
        f'<div title="{wf["order_id"]} P{pri}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:4px; "
        f'display:inline-flex; align-items:center; justify-content:center; '
        f'margin:2px; font-size:10px; font-weight:bold; color:#fff; '
        f'font-family:monospace;">'
        f"{pri}"
        f"</div>"
    )


def render_fairness_chiclet(wf):
    tenant = wf.get("tenant", "unknown")
    color = TENANT_COLORS.get(tenant, "#888")
    label = tenant.split("-")[-1][0].upper() if "-" in tenant else "?"
    return (
        f'<div title="{wf["order_id"]} {tenant}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:4px; "
        f'display:inline-flex; align-items:center; justify-content:center; '
        f'margin:2px; font-size:10px; font-weight:bold; color:#fff; '
        f'font-family:monospace;">'
        f"{label}"
        f"</div>"
    )


def render_swimlane(label, workflows, render_fn, empty_msg=""):
    count = len(workflows)
    chiclets = "".join(render_fn(wf) for wf in workflows)
    return (
        f'<div style="margin-bottom:16px;">'
        f'<div style="font-size:13px; font-weight:bold; color:#888; '
        f'margin-bottom:4px; font-family:sans-serif;">'
        f'{label} <span style="font-weight:normal; color:#555;">({count})</span></div>'
        f'<div style="background:#111; border:1px solid #333; border-radius:6px; '
        f'min-height:36px; padding:4px; display:flex; flex-wrap:wrap; align-items:center;">'
        f'{chiclets if chiclets else f"<span style=&quot;color:#555; font-size:12px; padding:4px;&quot;>{empty_msg}</span>"}'
        f"</div></div>"
    )


def render_legend(items):
    html = ""
    for color, label in items:
        html += (
            f'<div style="display:inline-flex; align-items:center; margin-right:16px;">'
            f'<div style="width:16px; height:16px; background:{color}; border-radius:3px; margin-right:4px;"></div>'
            f'<span style="font-size:13px;">{label}</span></div>'
        )
    return html


def render_all_swimlanes(workflows, sort_fn, render_fn):
    # Track completion order
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

    client = get_client()

    # --- Tabs with change detection ---
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
            # --- Priority sidebar ---
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
                add_cols = st.columns(3)
                has_batch = "workflows" in st.session_state
                if add_cols[0].button("+ High", disabled=not has_batch, key="pri_add_high"):
                    add_priority_workflows(client, add_count, 1, use_priority)
                if add_cols[1].button("+ Med", disabled=not has_batch, key="pri_add_med"):
                    add_priority_workflows(client, add_count, 3, use_priority)
                if add_cols[2].button("+ Low", disabled=not has_batch, key="pri_add_low"):
                    add_priority_workflows(client, add_count, 5, use_priority)

                st.divider()
                st.markdown("**Legend**")
                legend = render_legend([
                    (PRIORITY_COLORS[1], "P1 High"),
                    (PRIORITY_COLORS[3], "P3 Medium"),
                    (PRIORITY_COLORS[5], "P5 Low"),
                ])
                st.markdown(legend, unsafe_allow_html=True)

            # --- Priority content ---
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
            # --- Fairness sidebar ---
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
                add_cols_f = st.columns(3)
                has_batch_f = "workflows" in st.session_state
                if add_cols_f[0].button("+ Big", disabled=not has_batch_f, key="fair_add_big"):
                    add_fairness_workflows(client, add_count_f, "tenant-big", use_fairness)
                if add_cols_f[1].button("+ Mid", disabled=not has_batch_f, key="fair_add_mid"):
                    add_fairness_workflows(client, add_count_f, "tenant-mid", use_fairness)
                if add_cols_f[2].button("+ Small", disabled=not has_batch_f, key="fair_add_small"):
                    add_fairness_workflows(client, add_count_f, "tenant-small", use_fairness)

                st.divider()
                st.markdown("**Legend**")
                legend_f = render_legend([
                    (TENANT_COLORS["tenant-big"], "Big (30)"),
                    (TENANT_COLORS["tenant-mid"], "Mid (10)"),
                    (TENANT_COLORS["tenant-small"], "Small (5)"),
                ])
                st.markdown(legend_f, unsafe_allow_html=True)

            # --- Fairness content ---
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

    # Always auto-refresh
    time.sleep(0.1)
    st.rerun()


if __name__ == "__main__":
    main()
