"""Compare a fresh public benchmark JSON against latest_public_baseline.json.

Prints per-workload scoreboard deltas (ApexBase vs baseline, and competitor
shift) plus the largest per-metric changes. Read-only analysis helper;
does not modify any baseline files.
"""
import json
import sys
from pathlib import Path

BASELINE = Path(__file__).resolve().parent.parent / "latest_public_baseline.json"


def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def main(new_path):
    new = load(new_path)
    base = load(BASELINE)

    print("=" * 100)
    print("Run metadata")
    print("=" * 100)
    for d, name in ((base, "baseline"), (new, "current")):
        g, c = d.get("git", {}), d.get("config", {})
        print(f"{name:9s}: commit={g.get('commit','?')[:10]} dirty={g.get('dirty')} "
              f"rows={c.get('rows')} warmup={c.get('warmup')} iters={c.get('iterations')} "
              f"vector={'skip' if c.get('skip_vector') else 'on'}")

    def metrics(d):
        m = {}
        for r in d.get("results", []):
            v = r.get("ApexBase")
            if v is not None:
                m[r["query"]] = float(v)
        vec = d.get("vector_similarity") or {}
        for sec in ("head_to_head", "batch"):
            for r in vec.get(sec, []):
                v = r.get("ApexBase")
                if v is not None:
                    m[r["query"]] = float(v)
        return m

    mb, mn = metrics(base), metrics(new)
    common = sorted(set(mb) & set(mn))
    print(f"\nMetric coverage: baseline={len(mb)} current={len(mn)} common={len(common)} "
          f"only_baseline={sorted(set(mb)-set(mn))} only_current={sorted(set(mn)-set(mb))}")

    # Per-workload scoreboard from both reports (current report carries the
    # authoritative grouped scoreboard; baseline has the same shape).
    def scoreboard(d):
        return {w["workload"]: w for w in d.get("fair_workload_scoreboard", [])}

    sb_b, sb_n = scoreboard(base), scoreboard(new)
    print("\n" + "=" * 100)
    print("Fair-workload scoreboard (totals, ms; apex_vs_best = current run's ratio)")
    print("=" * 100)
    hdr = f"{'Workload':<26} | {'ApexBase now':>14} | {'best competitor':>15} | {'ratio':>9} | W/T/L | {'ApexDelta vs base':>18}"
    print(hdr)
    print("-" * len(hdr))
    for name in sb_n:
        w = sb_n[name]
        apex_total = w["total_ms"]["ApexBase"] or 0.0
        wb = sb_b.get(name)
        base_total = wb["total_ms"]["ApexBase"] if wb else None
        delta = f"{(apex_total-base_total)/base_total*100:+.1f}%" if base_total else "n/a"
        best = min((v for k, v in w["total_ms"].items() if k != "ApexBase" and v is not None), default=None)
        best_s = f"{best:.1f}" if best is not None else "n/a"
        ratio = w.get("apex_vs_best_total", "") or "-"
        print(f"{name:<26} | {apex_total:>14.3f} | {best_s:>15} | {ratio:<9} | "
              f"{w['apex_wins']}/{w['apex_ties']}/{w['apex_slower']:<3} | {delta:>18}")

    # Per-metric deltas, sorted by relative change
    rows = []
    for q in common:
        b, n = mb[q], mn[q]
        if b <= 0:
            continue
        rel = (n - b) / b
        rows.append((rel, q, b, n))
    rows.sort(key=lambda r: r[0])

    print("\n" + "=" * 100)
    print(f"Per-metric ApexBase delta vs baseline (top 15 slower / top 15 faster)")
    print("=" * 100)
    for rel, q, b, n in rows[:15]:
        flag = "SLOWER" if rel > 0.02 else ("faster" if rel < -0.02 else "flat  ")
        print(f"{flag} {rel*100:+7.1f}%  {q[:48]:<48} {b:>12.3f} -> {n:>12.3f} ms")
    print("   ...")
    for rel, q, b, n in rows[-15:][::-1]:
        flag = "SLOWER" if rel > 0.02 else ("faster" if rel < -0.02 else "flat  ")
        print(f"{flag} {rel*100:+7.1f}%  {q[:48]:<48} {b:>12.3f} -> {n:>12.3f} ms")

    # Competitor stability (sanity: same machine, did competitors drift?)
    def comp(d, name):
        m = {}
        for r in d.get("results", []):
            v = r.get(name)
            if v is not None:
                m[r["query"]] = float(v)
        return m

    print("\n" + "=" * 100)
    print("Competitor drift check (geomean ratio current/baseline, per engine)")
    print("=" * 100)
    import math
    for eng in ("SQLite", "DuckDB"):
        cb, cn = comp(base, eng), comp(new, eng)
        common_c = set(cb) & set(cn)
        if common_c:
            g = math.exp(sum(math.log(cn[q] / cb[q]) for q in common_c if cb[q] > 0) / len(common_c))
            print(f"{eng:<8}: geomean current/baseline = {g:.3f}x over {len(common_c)} metrics")

    # Vector summary
    vb, vn = (base.get("vector_similarity") or {}).get("summary"), (new.get("vector_similarity") or {}).get("summary")
    if vb and vn:
        print(f"\nVector head-to-head: baseline wins/ties/slower={vb['wins']}/{vb['ties']}/{vb['slower']}  "
              f"current={vn['wins']}/{vn['ties']}/{vn['slower']} (of {vn['total']})")

    # Guardrail summary: metrics slower by >15% and >0.005ms
    guard = [(q, b, n) for rel, q, b, n in rows if rel > 0.15 and (n - b) > 0.005]
    print("\n" + "=" * 100)
    print(f"Guardrail check (>15% AND >0.005ms slower than baseline): {len(guard)} metrics")
    for q, b, n in guard:
        print(f"  {q}: {b:.3f} -> {n:.3f} ms")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "results/public_rerun_release.json"))
