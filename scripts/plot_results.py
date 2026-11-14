#!/usr/bin/env python3
"""Gera gráficos a partir dos CSVs de experiments/results/."""
import glob
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
except ImportError:
    print("!! Instale: pip install matplotlib pandas")
    sys.exit(1)

RESULTS = "experiments/results"
OUT_DIR = os.path.join(RESULTS, "plots")
os.makedirs(OUT_DIR, exist_ok=True)

def plot_2pc():
    files = sorted(glob.glob(os.path.join(RESULTS, "twopc_N*.csv")))
    if not files:
        print("   (sem CSVs de 2PC)"); return
    fig, ax = plt.subplots(figsize=(8, 5))
    labels, p50s, p95s, p99s = [], [], [], []
    for f in files:
        n = os.path.basename(f).split("_N")[1].replace(".csv", "")
        df = pd.read_csv(f)
        if df.empty: continue
        lats = df["latency_ms"].astype(float)
        labels.append(n)
        p50s.append(lats.quantile(0.50))
        p95s.append(lats.quantile(0.95))
        p99s.append(lats.quantile(0.99))
    x = range(len(labels))
    w = 0.25
    ax.bar([i-w for i in x], p50s, w, label="p50", color="steelblue")
    ax.bar(x,           p95s, w, label="p95", color="orange")
    ax.bar([i+w for i in x], p99s, w, label="p99", color="red")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_xlabel("N (nós)")
    ax.set_ylabel("Latência (ms)")
    ax.set_title("2PC — p50/p95/p99 vs N")
    ax.legend(); ax.grid(True, axis="y", alpha=0.3)
    out = os.path.join(OUT_DIR, "twopc_latency.png")
    fig.savefig(out, dpi=130, bbox_inches="tight"); plt.close(fig)
    print(f"   -> {out}")

def plot_bully():
    files = sorted(glob.glob(os.path.join(RESULTS, "bully_N*.csv")))
    if not files:
        print("   (sem CSVs de Bully)"); return
    fig, ax = plt.subplots(figsize=(7, 4))
    labels, means, errs = [], [], []
    for f in files:
        n = os.path.basename(f).split("_N")[1].replace(".csv", "")
        df = pd.read_csv(f)
        vals = df["converge_ms"].replace(9999, pd.NA).dropna()
        if vals.empty: continue
        labels.append(n)
        means.append(vals.mean())
        errs.append(vals.std() if len(vals) > 1 else 0)
    ax.bar(labels, means, yerr=errs, capsize=5, color="steelblue")
    ax.set_xlabel("N (nós)")
    ax.set_ylabel("Convergência (ms)")
    ax.set_title("Bully — tempo de convergência pós-kill")
    ax.grid(True, axis="y", alpha=0.3)
    out = os.path.join(OUT_DIR, "bully_converge.png")
    fig.savefig(out, dpi=130, bbox_inches="tight"); plt.close(fig)
    print(f"   -> {out}")

def plot_heartbeat():
    f = os.path.join(RESULTS, "heartbeat_jitter.csv")
    if not os.path.exists(f):
        print("   (sem CSV de heartbeat)"); return
    df = pd.read_csv(f)
    fig, ax = plt.subplots(figsize=(7, 4))
    means = df.groupby("jitter_ms")["fp_count"].mean()
    stds = df.groupby("jitter_ms")["fp_count"].std().fillna(0)
    ax.bar([str(j) for j in means.index], means.values, yerr=stds.values,
           capsize=5, color="tomato")
    ax.set_xlabel("Jitter (ms)")
    ax.set_ylabel("Falsos positivos (média)")
    ax.set_title("Heartbeat — FP vs jitter")
    ax.grid(True, axis="y", alpha=0.3)
    out = os.path.join(OUT_DIR, "heartbeat_fp.png")
    fig.savefig(out, dpi=130, bbox_inches="tight"); plt.close(fig)
    print(f"   -> {out}")

if __name__ == "__main__":
    plot_2pc()
    plot_bully()
    plot_heartbeat()
    print("\n==> Gráficos em", OUT_DIR)
