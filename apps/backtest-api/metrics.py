from typing import List, Dict
import numpy as np

def equity_from_trades(trades: List[Dict], start_eq: float = 1.0):
    eq = start_eq
    curve = [start_eq]
    for t in trades:
        if "pnl" in t:
            eq = eq * (1.0 + float(t["pnl"]))
            curve.append(eq)
    return curve

def sharpe_approx(trades: List[Dict]):
    rets = [float(t["pnl"]) for t in trades if "pnl" in t]
    if len(rets) < 2:
        return 0.0
    return float(np.mean(rets) / (np.std(rets) + 1e-9) * (len(rets) ** 0.5))
