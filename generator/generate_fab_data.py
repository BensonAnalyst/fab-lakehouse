#!/usr/bin/env python3
"""
Synthetic 200mm fab data generator (standard library only).

Produces the tables an MES / SPC / equipment-automation system would hold:
  tools.csv              tool + chamber master data
  recipe_master.csv      CURRENT recipe version per step/tool (MES overwrites it on change)
  lots.csv               lot header
  lot_steps.csv          lot history: one row per lot per route step (track-in / track-out)
  spc_points.csv         SPC measurements at metrology steps, with OOC flags
  tool_state_events.csv  SEMI E10 equipment state changes

Realism built in (so the pipeline has something to find):
  * ETCH01 chamber B drifts upward over time -> CD excursions show up in SPC
  * random unscheduled-down events per tool
  * lots still in progress at the end of the window (WIP, track_out_time empty)

Optional data issues (--inject-issues), to exercise data-quality checks:
  * duplicate lot_steps rows (same lot_id + step_seq, new txn_id) = double logging
  * SPC points with empty tool_id
  * one "late commit": a row whose updated_at is BEFORE the batch window
    (simulates a transaction committed after the previous extract ran)

Usage:
  python generator/generate_fab_data.py --start 2026-11-01 --days 7 --out data/batch_01 --batch 1
  (first batch = initial load, keep --recipe-changes 0 so every recipe's starting version is captured)
  python generator/generate_fab_data.py --start 2026-11-08 --days 1 --out data/batch_02 --batch 2 \
         --recipe-changes 3 --inject-issues
"""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta

# --------------------------------------------------------------------------- master data
TOOLS = [
    # tool_id, module, chambers
    ("DIFF01", "DIFFUSION", ["A"]),
    ("LITH01", "LITHO", ["A"]),
    ("LITH02", "LITHO", ["A"]),
    ("ETCH01", "ETCH", ["A", "B", "C"]),
    ("ETCH02", "ETCH", ["A", "B", "C"]),
    ("IMPL01", "IMPLANT", ["A"]),
    ("CVD01", "CVD", ["A", "B"]),
    ("CVD02", "CVD", ["A", "B"]),
    ("CMP01", "CMP", ["A", "B"]),
    ("METR01", "METROLOGY", ["A"]),
    ("METR02", "METROLOGY", ["A"]),
]

# step_id, description, module, mean process minutes, spc parameter (None = no SPC)
ROUTE = [
    ("1000", "Gate oxidation", "DIFFUSION", 240, None),
    ("2000", "Poly litho", "LITHO", 45, None),
    ("3000", "Poly etch", "ETCH", 35, None),
    ("3100", "Poly CD measure", "METROLOGY", 15, ("POLY_CD_NM", 180.0, 3.0)),
    ("4000", "Source/drain implant", "IMPLANT", 50, None),
    ("5000", "ILD deposition", "CVD", 60, None),
    ("5100", "ILD thickness", "METROLOGY", 15, ("ILD_THK_A", 8000.0, 60.0)),
    ("6000", "ILD CMP", "CMP", 40, None),
    ("7000", "Contact litho", "LITHO", 45, None),
    ("8000", "Contact etch", "ETCH", 40, None),
    ("8100", "Contact CD measure", "METROLOGY", 15, ("CONTACT_CD_NM", 220.0, 4.0)),
]

E10_STATES = ["PRODUCTIVE", "STANDBY", "ENGINEERING", "SCHEDULED_DOWN", "UNSCHEDULED_DOWN"]
RECIPE_BASE_DATE = datetime(2026, 1, 1)  # all recipes exist since start of year

TS_FMT = "%Y-%m-%d %H:%M:%S"


def ts(d):
    return d.strftime(TS_FMT) if d else ""


def tools_for(module):
    return [t for t in TOOLS if t[1] == module]


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


# --------------------------------------------------------------------------- generators
def gen_tools():
    rows = []
    for tool_id, module, chambers in TOOLS:
        for ch in chambers:
            rows.append([tool_id, ch, module, "BAY" + module[:3], "ACTIVE", ts(RECIPE_BASE_DATE)])
    return rows


def recipe_id_for(step_id, tool_id):
    return f"R_{step_id}_{tool_id}"


def gen_recipe_master(rng, start, end, n_changes):
    """Current state only, like a real MES table. Changed recipes get a new version + updated_at."""
    rows = {}
    for step_id, _, module, _, _ in ROUTE:
        for tool_id, _, _ in tools_for(module):
            rid = recipe_id_for(step_id, tool_id)
            rows[rid] = [rid, step_id, tool_id, 1, ts(RECIPE_BASE_DATE)]
    for rid in rng.sample(sorted(rows), k=min(n_changes, len(rows))):
        change_time = start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))
        rows[rid][3] = rows[rid][3] + rng.randint(1, 2)
        rows[rid][4] = ts(change_time)
    return list(rows.values())


def gen_lots_and_steps(rng, start, days, lots_per_day, end, batch):
    lots, steps, txn = [], [], batch * 1_000_000
    for d in range(days):
        day = start + timedelta(days=d)
        for n in range(lots_per_day):
            lot_id = f"L{day:%y%m%d}{n + 1:03d}"
            lot_start = day + timedelta(minutes=rng.randint(0, 24 * 60 - 1))
            if lot_start >= end:
                continue
            priority = rng.choice(["NORMAL"] * 8 + ["HOT"] * 2)
            wafers = rng.choice([25] * 9 + [24, 23])
            t = lot_start
            lot_status, last_update = "COMPLETED", lot_start
            for seq, (step_id, _, module, proc_min, _) in enumerate(ROUTE, start=1):
                queue = rng.expovariate(1 / (30 if priority == "HOT" else 90))
                track_in = t + timedelta(minutes=queue)
                if track_in >= end:
                    lot_status = "WIP"
                    break
                tool_id, _, chambers = rng.choice(tools_for(module))
                chamber = rng.choice(chambers)
                track_out = track_in + timedelta(minutes=max(5, rng.gauss(proc_min, proc_min * 0.15)))
                txn += 1
                if track_out >= end:  # still processing at end of window
                    steps.append([txn, lot_id, seq, step_id, tool_id, chamber,
                                  recipe_id_for(step_id, tool_id), wafers, ts(track_in), "", ts(track_in)])
                    lot_status, last_update = "WIP", track_in
                    break
                steps.append([txn, lot_id, seq, step_id, tool_id, chamber,
                              recipe_id_for(step_id, tool_id), wafers, ts(track_in), ts(track_out), ts(track_out)])
                t, last_update = track_out, track_out
            lots.append([lot_id, "PROD_MCU_A", priority, wafers, lot_status, ts(lot_start), ts(last_update)])
    return lots, steps, txn


def gen_spc(rng, steps, window_start, window_days):
    params = {r[0]: r[4] for r in ROUTE if r[4]}
    rows, pid = [], 0
    by_key = {(p[1], p[2]): p for p in steps}
    for s in steps:
        txn, lot_id, seq, step_id, tool_id, chamber, _, wafers, t_in, t_out, _ = s
        if step_id not in params or not t_out:
            continue
        name, target, sigma = params[step_id]
        # the measured parameter reflects the PREVIOUS process step's tool (e.g. etch -> CD)
        prev = by_key.get((lot_id, seq - 1))
        proc_tool, proc_ch = (prev[4], prev[5]) if prev else (tool_id, chamber)
        measure_time = datetime.strptime(t_out, TS_FMT)
        drift = 0.0
        if proc_tool == "ETCH01" and proc_ch == "B":
            progress = (measure_time - window_start).total_seconds() / (window_days * 86400)
            drift = sigma * 3.5 * max(0.0, progress)  # slow upward drift
        for site in range(1, 6):  # 5-site measurement per lot
            pid += 1
            value = rng.gauss(target + drift, sigma)
            z = (value - target) / sigma
            ooc = 1 if abs(z) > 3 else 0
            rows.append([f"{lot_id}-{step_id}-{site}", lot_id, step_id, name, proc_tool, proc_ch, "METR",
                         site, round(value, 3), target, round(target + 3 * sigma, 3),
                         round(target - 3 * sigma, 3), ooc, ts(measure_time), ts(measure_time)])
    return rows


def gen_tool_states(rng, start, end, batch):
    rows, eid = [], batch * 1_000_000
    weights = [0.62, 0.18, 0.07, 0.06, 0.07]
    mean_minutes = {"PRODUCTIVE": 300, "STANDBY": 60, "ENGINEERING": 90,
                    "SCHEDULED_DOWN": 240, "UNSCHEDULED_DOWN": 150}
    for tool_id, _, _ in TOOLS:
        t, prev = start, None
        while t < end:
            state = rng.choices(E10_STATES, weights)[0]
            if state == prev:
                continue
            eid += 1
            reason = {"UNSCHEDULED_DOWN": rng.choice(["RF_FAULT", "ROBOT_ALARM", "PARTICLE_FAIL", "VACUUM_LEAK"]),
                      "SCHEDULED_DOWN": rng.choice(["PM_WEEKLY", "PM_MONTHLY", "QUAL_RUN"]),
                      "ENGINEERING": rng.choice(["RECIPE_DEV", "DOE_RUN"])}.get(state, "")
            rows.append([f"E{eid}", tool_id, state, reason, ts(t), ts(t)])
            t = t + timedelta(minutes=max(10, rng.expovariate(1 / mean_minutes[state])))
            prev = state
    return rows


def inject_issues(rng, steps, spc, start, next_txn):
    notes = []
    for s in rng.sample(steps, k=min(3, len(steps))):  # double logging
        next_txn += 1
        dup = list(s)
        dup[0] = next_txn
        steps.append(dup)
    notes.append("3 duplicate lot_steps rows (same lot_id + step_seq, new txn_id)")
    for p in rng.sample(spc, k=min(4, len(spc))):
        p[4] = ""
    notes.append("4 SPC points with empty tool_id")
    # late commit: a rework measurement on a lot from the PREVIOUS day (i.e. the previous batch),
    # finished 45 min before this window but only committed to the MES now
    prev_lot = f"L{start - timedelta(days=1):%y%m%d}001"
    t_out = start - timedelta(minutes=45)
    next_txn += 1
    late = [next_txn, prev_lot, 99, "9999", "METR01", "A", recipe_id_for("8100", "METR01"), 25,
            ts(t_out - timedelta(minutes=20)), ts(t_out), ts(t_out)]
    steps.append(late)
    notes.append(f"1 late-commit lot_steps row ({prev_lot} rework step 9999, updated_at {late[10]})")
    return notes


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True, help="window start date YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--lots-per-day", type=int, default=24)
    ap.add_argument("--batch", type=int, default=1, help="batch number, keeps ids unique across batches")
    ap.add_argument("--recipe-changes", type=int, default=0,
                    help="recipes that get a new version inside this window (keep 0 for the first batch)")
    ap.add_argument("--inject-issues", action="store_true")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    rng = random.Random(a.seed if a.seed is not None else a.batch)
    start = datetime.strptime(a.start, "%Y-%m-%d")
    end = start + timedelta(days=a.days)
    os.makedirs(a.out, exist_ok=True)

    tools = gen_tools()
    recipes = gen_recipe_master(rng, start, end, a.recipe_changes)
    lots, steps, last_txn = gen_lots_and_steps(rng, start, a.days, a.lots_per_day, end, a.batch)
    spc = gen_spc(rng, steps, start, a.days)
    states = gen_tool_states(rng, start, end, a.batch)
    notes = inject_issues(rng, steps, spc, start, last_txn) if a.inject_issues else []

    counts = {
        "tools": write_csv(os.path.join(a.out, "tools.csv"),
                           ["tool_id", "chamber_id", "module", "bay", "status", "updated_at"], tools),
        "recipe_master": write_csv(os.path.join(a.out, "recipe_master.csv"),
                                   ["recipe_id", "step_id", "tool_id", "recipe_version", "updated_at"], recipes),
        "lots": write_csv(os.path.join(a.out, "lots.csv"),
                          ["lot_id", "product_id", "priority", "wafer_qty", "lot_status", "start_time",
                           "updated_at"], lots),
        "lot_steps": write_csv(os.path.join(a.out, "lot_steps.csv"),
                               ["txn_id", "lot_id", "step_seq", "step_id", "tool_id", "chamber_id", "recipe_id",
                                "wafer_qty", "track_in_time", "track_out_time", "updated_at"], steps),
        "spc_points": write_csv(os.path.join(a.out, "spc_points.csv"),
                                ["point_id", "lot_id", "step_id", "parameter", "tool_id", "chamber_id",
                                 "measure_tool_prefix", "site", "value", "target", "ucl", "lcl", "ooc_flag",
                                 "measure_time", "updated_at"], spc),
        "tool_state_events": write_csv(os.path.join(a.out, "tool_state_events.csv"),
                                       ["event_id", "tool_id", "e10_state", "reason_code", "event_time",
                                        "updated_at"], states),
    }
    print(f"Batch {a.batch}: {a.start} + {a.days} day(s) -> {a.out}")
    for k, v in counts.items():
        print(f"  {k:<18} {v:>7,} rows")
    for n in notes:
        print(f"  [injected] {n}")


if __name__ == "__main__":
    main()
