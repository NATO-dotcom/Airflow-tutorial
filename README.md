# 🌬️ Apache Airflow 3 — Fundamentals for Data Engineering

A hands-on tutorial repository that walks through the core concepts of **Apache Airflow 3** for data engineering — from writing your very first DAG all the way to cross-DAG orchestration and asset-driven pipelines. Every concept is backed by a working DAG you can run locally with Docker Compose.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Tutorial Roadmap](#-tutorial-roadmap)
  - [1 — Your First DAG](#1--your-first-dag)
  - [2 — DAG Versioning](#2--dag-versioning)
  - [3 — Operators (Python & Bash)](#3--operators-python--bash)
  - [4 — XComs: Automatic (TaskFlow)](#4--xcoms-automatic-taskflow)
  - [5 — XComs: Manual (kwargs)](#5--xcoms-manual-kwargs)
  - [6 — Parallel Tasks](#6--parallel-tasks)
  - [7 — Conditional Branching](#7--conditional-branching)
  - [8 — Scheduling: Presets](#8--scheduling-presets)
  - [9 — Scheduling: Cron Expressions](#9--scheduling-cron-expressions)
  - [10 — Scheduling: Timedelta / Duration](#10--scheduling-timedelta--duration)
  - [11 — Incremental Loading (Data Intervals)](#11--incremental-loading-data-intervals)
  - [12 — Scheduling: Special / Event Dates](#12--scheduling-special--event-dates)
  - [13 & 14 — Assets (Data-Aware Scheduling)](#13--14--assets-data-aware-scheduling)
  - [15 — Cross-DAG Orchestration](#15--cross-dag-orchestration)
- [Key Airflow Concepts at a Glance](#-key-airflow-concepts-at-a-glance)
- [Common CLI Commands](#-common-cli-commands)
- [Resources](#-resources)
- [Author](#-author)
- [License](#-license)

---

## 🔭 Overview

This repository is a progressive, example-driven guide to Apache Airflow 3. Each DAG file in the `dags/` directory is numbered and focuses on a single Airflow concept, making it easy to learn one idea at a time, run it, inspect the results in the Airflow UI, and then move on.

**What you'll learn:**

| # | Concept | DAG File |
|---|---------|----------|
| 1 | Creating & running your first DAG | `1_first_dag.py` |
| 2 | DAG versioning | `2_dag_versioning.py` |
| 3 | Operators — Python & Bash | `3_operators.py` |
| 4 | XComs — automatic (TaskFlow API) | `4_XCOMS_auto.py` |
| 5 | XComs — manual push/pull with kwargs | `5_XCOMS_kwargs.py` |
| 6 | Parallel / fan-out fan-in tasks | `6_parallel_tasks.py` |
| 7 | Conditional branching | `7_branches.py` |
| 8 | Schedule presets (`@daily`, etc.) | `8_schedule_presets.py` |
| 9 | Cron-based scheduling | `9_schedule_cron.py` |
| 10 | Timedelta / duration scheduling | `10_schedule_delta.py` |
| 11 | Incremental loading with data intervals | `11_incremental_load.py` |
| 12 | Event / special-date scheduling | `12_special_dates.py` |
| 13 | Assets — producing data-aware assets | `assets_13.py` |
| 14 | Assets — dependent / consumer assets | `14_assets_dependent.py` |
| 15 | Cross-DAG orchestration (`TriggerDagRunOperator`) | `dag_orchestrate_*.py` |

---

## 🛠️ Tech Stack

| Component | Version / Details |
|-----------|-------------------|
| **Apache Airflow** | 3.3.2 |
| **Python** | 3.14 |
| **Executor** | CeleryExecutor |
| **Message Broker** | Redis 7.2 |
| **Metadata DB** | PostgreSQL 16 |
| **Containerisation** | Docker Compose |
| **Package Manager** | uv |

---

## 📁 Project Structure

```
Airflow-tutorial/
├── dags/                          # 🎯 All tutorial DAG files live here
│   ├── 1_first_dag.py             # Your first DAG
│   ├── 2_dag_versioning.py        # DAG versioning
│   ├── 3_operators.py             # Python & Bash operators
│   ├── 4_XCOMS_auto.py            # XComs with TaskFlow API
│   ├── 5_XCOMS_kwargs.py          # XComs with manual push/pull
│   ├── 6_parallel_tasks.py        # Fan-out / fan-in parallelism
│   ├── 7_branches.py              # Conditional branching
│   ├── 8_schedule_presets.py       # Schedule presets (@daily)
│   ├── 9_schedule_cron.py         # Cron-based scheduling
│   ├── 10_schedule_delta.py       # Duration/delta scheduling
│   ├── 11_incremental_load.py     # Incremental loading
│   ├── 12_special_dates.py        # Event-driven timetables
│   ├── assets_13.py               # Asset producer
│   ├── 14_assets_dependent.py     # Asset consumer
│   ├── dag_orchestrate_1.py       # Child DAG 1 (orchestration)
│   ├── dag_orchestrate_2.py       # Child DAG 2 (orchestration)
│   └── dag_orchestrate_parent.py  # Parent orchestrator DAG
├── config/
│   └── airflow.cfg                # Airflow configuration file
├── logs/                          # Task execution logs (auto-generated)
├── plugins/                       # Custom Airflow plugins (empty)
├── src/airflow_tutorial/          # Python package scaffold
│   └── __init__.py
├── .env                           # Environment variables (AIRFLOW_UID)
├── .gitignore
├── .python-version                # Python 3.14
├── docker-compose.yaml            # Full Airflow cluster definition
├── pyproject.toml                 # Project metadata & dependencies
└── uv.lock                        # Lockfile for reproducible builds
```

---

## 🚀 Getting Started

### Prerequisites

- **Docker** & **Docker Compose** installed ([Install Docker](https://docs.docker.com/get-docker/))
- At least **4 GB RAM** and **2 CPUs** allocated to Docker
- (Optional) **uv** package manager for local Python development

### 1. Clone the Repository

```bash
git clone https://github.com/NATO-dotcom/Airflow-tutorial.git
cd Airflow-tutorial
```

### 2. Set the Airflow UID

On Linux, ensure file permissions map correctly:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 3. Start the Airflow Cluster

```bash
docker compose up airflow-init   # Run DB migrations & create admin user
docker compose up -d             # Start all services in the background
```

This spins up the full stack:

| Service | Port | Purpose |
|---------|------|---------|
| **API Server (Web UI)** | `localhost:8080` | DAG management & monitoring |
| **Scheduler** | — | Parses DAGs & schedules task runs |
| **DAG Processor** | — | Processes DAG files |
| **Worker** (Celery) | — | Executes tasks |
| **Triggerer** | — | Handles deferrable operators |
| **PostgreSQL** | `localhost:5440` | Metadata database |
| **Redis** | `6379` (internal) | Celery message broker |

### 4. Access the Web UI

Open **http://localhost:8080** and log in with:

| Field | Value |
|-------|-------|
| Username | `airflow` |
| Password | `airflow` |

### 5. Shut Down

```bash
docker compose down           # Stop containers
docker compose down -v        # Stop containers AND remove database volumes
```

---

## 📚 Tutorial Roadmap

### 1 — Your First DAG

> **File:** `dags/1_first_dag.py`

Introduces the fundamental building blocks of Airflow:

- **`@dag` decorator** — defines a DAG using the TaskFlow API
- **`@task.python` decorator** — wraps a Python function as an Airflow task
- **Task dependencies** — the `>>` (bitshift) operator creates sequential execution order
- **DAG instantiation** — calling the decorated function registers the DAG with Airflow

```python
first_task() >> second_task() >> third_task()
```

```
first_task  →  second_task  →  third_task
```

---

### 2 — DAG Versioning

> **File:** `dags/2_dag_versioning.py`

Demonstrates how to evolve a DAG over time by adding new tasks to an existing pipeline without breaking prior runs. This DAG adds a `version_task` to the chain from DAG #1.

**Key takeaway:** Airflow tracks DAG versions automatically — modifying a DAG creates a new version visible in the Airflow UI's version history.

---

### 3 — Operators (Python & Bash)

> **File:** `dags/3_operators.py`

Compares two styles of defining tasks:

| Style | Example | When to Use |
|-------|---------|-------------|
| **Modern TaskFlow** | `@task.bash` decorator | New projects, cleaner syntax |
| **Classic Operator** | `BashOperator(...)` instantiation | Legacy DAGs, complex operator configs |

```python
# Modern — @task.bash
@task.bash
def bash_task_modern():
    return "echo https://airflow.apache.org/"

# Classic — BashOperator
bash_task_oldschool = BashOperator(
    task_id="bash_task_oldschool",
    bash_command="echo https://airflow.apache.org/",
)
```

---

### 4 — XComs: Automatic (TaskFlow)

> **File:** `dags/4_XCOMS_auto.py`

Shows **automatic XCom passing** via the TaskFlow API. When a task returns a value, Airflow automatically pushes it as an XCom. Downstream tasks receive it simply by accepting the upstream task's output as a function argument.

```python
first = first_task()          # Returns {"data": [1,2,3,4,5]}
second = second_task(first)   # Receives first's return automatically
third = third_task(second)    # Receives second's return automatically
```

**Why this matters:** This is the idiomatic ETL pattern — **Extract → Transform → Load** — with data flowing seamlessly between tasks.

---

### 5 — XComs: Manual (kwargs)

> **File:** `dags/5_XCOMS_kwargs.py`

Demonstrates **manual XCom push/pull** using the `ti` (TaskInstance) object from `**kwargs`:

```python
# Pushing
ti = kwargs['ti']
ti.xcom_push(key='return_result', value=fetched_data)

# Pulling
fetched_data = ti.xcom_pull(task_ids='first_task', key='return_result')
```

**When to use manual XComs:**
- You need custom keys (not just `return_value`)
- You need to pull from a task that isn't a direct upstream
- You're working with classic operators that don't support TaskFlow auto-passing

---

### 6 — Parallel Tasks

> **File:** `dags/6_parallel_tasks.py`

Implements a **fan-out / fan-in** pattern where one extract task feeds three parallel transform tasks, all converging into a single load task:

```
                    ┌─── transform_api ───┐
extract_task ──────►├─── transform_db  ───├──────► load_task
                    └─── transform_s3  ───┘
```

```python
extract >> [transform_api, transform_db, transform_s3] >> load
```

**Key concept:** Wrapping tasks in a list `[...]` tells Airflow to run them **in parallel**. The downstream task waits for all parallel tasks to complete before executing.

---

### 7 — Conditional Branching

> **File:** `dags/7_branches.py`

Adds a **`@task.branch`** decorator to choose which downstream task to execute at runtime based on data:

```python
@task.branch
def decider_task(**kwargs):
    weekend_flag = ti.xcom_pull(task_ids='extract_task')['weekend_flag']
    if weekend_flag == "true":
        return 'no_load_task'    # Skip loading on weekends
    else:
        return 'load_task'       # Proceed with loading
```

```
                                            ┌─── load_task
[transforms] ──► decider_task ──────────────┤
                                            └─── no_load_task
```

**Key concept:** The branch task returns the **`task_id`** of the next task to execute. All other branches are **skipped**.

---

### 8 — Scheduling: Presets

> **File:** `dags/8_schedule_presets.py`

Introduces Airflow's built-in schedule presets:

| Preset | Equivalent Cron | Description |
|--------|-----------------|-------------|
| `@once` | — | Run exactly once |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Every day at midnight |
| `@weekly` | `0 0 * * 0` | Every Sunday at midnight |
| `@monthly` | `0 0 1 * *` | First day of each month |
| `@yearly` | `0 0 1 1 *` | January 1st each year |

Also demonstrates:
- **`start_date`** — when the scheduler begins looking for runs (uses `pendulum.datetime` with timezone)
- **`catchup=True`** — backfills all missed runs from `start_date` to now
- **`is_paused_upon_creation=False`** — starts the DAG immediately without manual activation

---

### 9 — Scheduling: Cron Expressions

> **File:** `dags/9_schedule_cron.py`

Uses `CronTriggerTimetable` for fine-grained cron control:

```python
from airflow.timetables.trigger import CronTriggerTimetable

schedule = CronTriggerTimetable("0 16 * * MON-FRI", timezone="America/Halifax")
```

This runs the DAG at **4:00 PM every weekday**. Also demonstrates:
- **`end_date`** — stops scheduling after a specific date
- **Timezone-aware scheduling** — ensures runs happen at the correct local time

---

### 10 — Scheduling: Timedelta / Duration

> **File:** `dags/10_schedule_delta.py`

Uses `DeltaTriggerTimetable` for interval-based scheduling (every N hours/days):

```python
from airflow.timetables.trigger import DeltaTriggerTimetable
from pendulum import duration

schedule = DeltaTriggerTimetable(duration(days=3))
```

**When to use:** When you need a DAG to run at a fixed interval (e.g., every 3 days, every 6 hours) rather than at a specific clock time.

---

### 11 — Incremental Loading (Data Intervals)

> **File:** `dags/11_incremental_load.py`

Demonstrates the critical data engineering concept of **incremental loading** using Airflow's `data_interval_start` and `data_interval_end`:

```python
@task.python
def incremental_data_fetch(**kwargs):
    start = kwargs['data_interval_start']
    end   = kwargs['data_interval_end']
    print(f"Fetching data from {start} to {end}")
```

Uses `CronDataIntervalTimetable` (from `airflow.timetables.interval`) which provides **data intervals** — unlike `CronTriggerTimetable` which only fires triggers. This enables idempotent, partition-aware ETL jobs.

**Also demonstrates Jinja templating in Bash tasks:**

```python
@task.bash
def incremental_data_process():
    return "echo 'Processing data from {{ data_interval_start }} to {{ data_interval_end }}'"
```

---

### 12 — Scheduling: Special / Event Dates

> **File:** `dags/12_special_dates.py`

Uses `EventsTimetable` to schedule DAG runs on specific, irregular dates:

```python
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(event_dates=[
    datetime(2026, 10, 1),
    datetime(2026, 11, 15),
    datetime(2026, 11, 26),
    datetime(2026, 11, 30),
])
```

**Use cases:** End-of-quarter reports, holiday schedules, audit dates, or any irregular business calendar.

---

### 13 & 14 — Assets (Data-Aware Scheduling)

> **Files:** `dags/assets_13.py` & `dags/14_assets_dependent.py`

Introduces **Airflow Assets** — a data-aware scheduling mechanism where DAGs are triggered by data availability rather than time:

**Producer (`assets_13.py`):**

```python
@asset(schedule="@daily", uri="/opt/airflow/logs/data/data_extract.txt", name="fetch_data")
def fetch_data(self):
    with open(self.uri, 'w') as f:
        f.write("Data fetched successfully")
```

**Consumer (`14_assets_dependent.py`):**

```python
@asset(schedule=fetch_data, uri="/opt/airflow/logs/data/data_processed.txt", name="process_data")
def process_data(self):
    with open(self.uri, 'w') as f:
        f.write("Data processed successfully")
```

**How it works:**
1. `fetch_data` runs daily and produces data at its URI
2. `process_data` is scheduled by `fetch_data` — it only runs **after** the producer completes
3. This creates a **data-driven dependency** chain

---

### 15 — Cross-DAG Orchestration

> **Files:** `dags/dag_orchestrate_parent.py`, `dags/dag_orchestrate_1.py`, `dags/dag_orchestrate_2.py`

Demonstrates triggering child DAGs from a parent DAG using `TriggerDagRunOperator`:

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_first_dag = TriggerDagRunOperator(
    task_id="trigger_first_orchestrator_dag",
    trigger_dag_id="first_orchestrator_dag",
    # wait_for_completion=True  # Optional: blocks until child completes
)
```

```
dag_orchestrate_parent
    ├──► trigger first_orchestrator_dag
    └──► trigger second_orchestrator_dag
```

> ⚠️ **Important:** Both child DAGs must be **enabled** in the Airflow UI before triggering the parent. Otherwise, they'll be queued but won't execute.

---

## 🧠 Key Airflow Concepts at a Glance

| Concept | Description |
|---------|-------------|
| **DAG** | Directed Acyclic Graph — a collection of tasks with defined dependencies and execution order |
| **Task** | A single unit of work in a DAG (Python function, Bash command, etc.) |
| **Operator** | A template/class for creating tasks (BashOperator, PythonOperator, etc.) |
| **TaskFlow API** | Modern decorator-based approach (`@task.python`, `@task.bash`) for defining tasks |
| **XCom** | Cross-Communication — mechanism for tasks to share small amounts of data |
| **Schedule** | Defines when/how often a DAG runs (cron, preset, timedelta, event-based) |
| **Catchup** | When `True`, Airflow backfills all missed runs from `start_date` to the current date |
| **Data Interval** | The time window a DAG run covers — critical for incremental/partition-based processing |
| **Asset** | A data-aware object that triggers downstream DAGs when updated |
| **Branching** | Runtime conditional logic that chooses which downstream task(s) to execute |
| **TriggerDagRunOperator** | Operator for triggering external/child DAGs from a parent DAG |

---

## 💻 Common CLI Commands

```bash
# Start the full Airflow cluster
docker compose up -d

# View running containers
docker compose ps

# Follow logs for a specific service
docker compose logs -f airflow-scheduler

# Access the Airflow CLI inside a container
docker compose exec airflow-apiserver airflow dags list
docker compose exec airflow-apiserver airflow tasks list <dag_id>

# Trigger a DAG manually
docker compose exec airflow-apiserver airflow dags trigger <dag_id>

# Stop everything
docker compose down

# Stop and wipe all data (fresh start)
docker compose down -v
```

---

## 📖 Resources

- [Apache Airflow 3 Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [TaskFlow API Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Airflow Docker Compose Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Timetables & Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html)
- [XComs — Cross-Communication](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

---

## ✍️ Author

**NATO-dotcom** — [dnaliaka365@gmail.com](mailto:dnaliaka365@gmail.com)

---

## 📄 License

This project uses the official Apache Airflow Docker Compose configuration, which is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
