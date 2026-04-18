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


PRIORITY_LABELS = {1: "High", 3: "Medium", 5: "Low"}
PRIORITY_COLORS = {1: "#ef4444", 3: "#f59e0b", 5: "#3b82f6"}


@st.cache_resource
def get_client():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    return run_async(Client.connect(**config))


def _next_batch_id():
    if "batch_id" not in st.session_state:
        st.session_state["batch_id"] = int(time.time())
    return st.session_state["batch_id"]


def _next_seq():
    seq = st.session_state.get("wf_seq", 0)
    st.session_state["wf_seq"] = seq + 1
    return seq


def start_workflow(client, priority, mode):
    batch_id = _next_batch_id()
    seq = _next_seq()
    order_id = f"ORD-{batch_id}-{seq:03d}"
    wf_id = f"order-{order_id}"

    start_kwargs = dict(id=wf_id, task_queue=TASK_QUEUE)
    if mode != "FIFO":
        start_kwargs["priority"] = Priority(priority_key=priority)

    run_async(
        client.start_workflow(
            OrderWorkflow.run,
            ProcessOrderInput(order_id, f"tenant-{seq}", priority),
            **start_kwargs,
        )
    )
    return {
        "id": wf_id,
        "order_id": order_id,
        "priority": priority,
        "submitted_at": datetime.now(),
    }


def start_batch(client, count, mode):
    st.session_state["batch_id"] = int(time.time())
    st.session_state["wf_seq"] = 0
    workflows = []
    for _ in range(count):
        priority = random.choice([1, 3, 5])
        workflows.append(start_workflow(client, priority, mode))
    return workflows


def add_workflows(client, count, priority, mode):
    existing = st.session_state.get("workflows", [])
    for _ in range(count):
        existing.append(start_workflow(client, priority, mode))
    st.session_state["workflows"] = existing


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
                if pending and pending[0].state == 1:  # SCHEDULED
                    status = "Queued"
                elif pending and pending[0].state == 2:  # STARTED
                    status = "Running"
                else:
                    status = "Running"
        except Exception:
            status = "Queued"

        updated.append({**wf, "status": status})
    return updated


def render_chiclet(wf):
    pri = wf["priority"]
    color = PRIORITY_COLORS[pri]
    return (
        f'<div title="{wf["order_id"]} P{pri}" style="'
        f"width:28px; height:28px; background:{color}; border-radius:4px; "
        f'display:inline-flex; align-items:center; justify-content:center; '
        f'margin:2px; font-size:10px; font-weight:bold; color:#fff; '
        f'font-family:monospace;">'
        f"{pri}"
        f"</div>"
    )


def render_swimlane(label, workflows, empty_msg=""):
    count = len(workflows)
    chiclets = "".join(render_chiclet(wf) for wf in workflows)
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


def main():
    st.set_page_config(page_title="Priority Queue Demo", layout="wide")

    client = get_client()

    # Sidebar
    with st.sidebar:
        st.header("Controls")
        if st.button("Start Batch", type="primary"):
            st.session_state["_start_pending"] = True

        count = st.slider("Workflows", 10, 50, 30)
        mode = st.radio(
            "Queue mode",
            ["FIFO", "Priority", "Fairness", "Fairness + Weights"],
            index=1,
        )
        st.session_state["mode"] = mode

        if st.session_state.pop("_start_pending", False):
            with st.spinner("Starting workflows..."):
                workflows = start_batch(client, count, mode)
            st.session_state["workflows"] = workflows
            st.session_state["completed_order"] = []
            st.session_state["seen_completed"] = set()
            st.session_state["auto_refresh"] = True

        st.divider()
        st.markdown("**Add to batch**")
        add_count = st.slider("Count to add", 1, 20, 5, key="add_count")
        add_cols = st.columns(3)
        if add_cols[0].button("+ High", disabled="workflows" not in st.session_state):
            add_workflows(client, add_count, 1, mode)
        if add_cols[1].button("+ Med", disabled="workflows" not in st.session_state):
            add_workflows(client, add_count, 3, mode)
        if add_cols[2].button("+ Low", disabled="workflows" not in st.session_state):
            add_workflows(client, add_count, 5, mode)

        st.divider()
        auto_refresh = st.checkbox(
            "Auto-refresh", value=st.session_state.get("auto_refresh", False)
        )
        st.session_state["auto_refresh"] = auto_refresh
        if st.button("Refresh Now"):
            pass

        st.divider()
        st.markdown("**Legend**")
        legend = ""
        for pri in [1, 3, 5]:
            color = PRIORITY_COLORS[pri]
            label = PRIORITY_LABELS[pri]
            legend += (
                f'<div style="display:inline-flex; align-items:center; margin-right:16px;">'
                f'<div style="width:16px; height:16px; background:{color}; border-radius:3px; margin-right:4px;"></div>'
                f'<span style="font-size:13px;">P{pri} {label}</span></div>'
            )
        st.markdown(legend, unsafe_allow_html=True)

    # Title
    mode = st.session_state.get("mode", "Priority")
    st.markdown(f"## Task Queue Demo: {mode}")

    if "workflows" not in st.session_state:
        st.info("Click **Start Batch** to begin.")
        return

    # Poll
    workflows = poll_statuses(client, st.session_state["workflows"])
    st.session_state["workflows"] = workflows

    # Track completion order
    seen = st.session_state.get("seen_completed", set())
    completed_order = st.session_state.get("completed_order", [])
    for wf in workflows:
        if wf["status"] == "Completed" and wf["id"] not in seen:
            seen.add(wf["id"])
            completed_order.append(wf)
    st.session_state["seen_completed"] = seen
    st.session_state["completed_order"] = completed_order

    # Split
    queued = [wf for wf in workflows if wf["status"] == "Queued"]
    running = [wf for wf in workflows if wf["status"] == "Running"]
    completed = completed_order

    # Sort queued: highest priority (lowest number) on the right
    # So sort descending by priority (5, 3, 1) — low priority left, high priority right
    if mode == "FIFO":
        queued = sorted(queued, key=lambda wf: wf["submitted_at"])
    else:
        queued = sorted(queued, key=lambda wf: (wf["priority"], wf["submitted_at"]))

    total = len(workflows)
    n_queued = len(queued)
    n_running = len(running)
    n_completed = len(completed)

    # Stats
    cols = st.columns(4)
    cols[0].metric("Total", total)
    cols[1].metric("Queued", n_queued)
    cols[2].metric("Running", n_running)
    cols[3].metric("Completed", n_completed)

    # Arrow label
    arrow = (
        '<div style="text-align:left; font-size:11px; color:#555; '
        'margin-bottom:2px; font-family:sans-serif;">'
        "&#9664; next to execute</div>"
    )

    # Swimlanes
    st.markdown(arrow, unsafe_allow_html=True)
    html = render_swimlane("QUEUED", queued, "Empty")
    html += render_swimlane("RUNNING", running, "Idle")
    html += render_swimlane("COMPLETED", completed, "None yet")
    st.markdown(html, unsafe_allow_html=True)

    # Auto-refresh
    if st.session_state.get("auto_refresh") and n_completed < total:
        time.sleep(0.1)
        st.rerun()
    elif n_completed == total and st.session_state.get("auto_refresh"):
        st.session_state["auto_refresh"] = False


if __name__ == "__main__":
    main()
