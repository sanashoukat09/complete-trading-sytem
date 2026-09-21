"""Descriptive outcome validation. This module does not certify a trading edge."""
import math,statistics,random
from .model import dumps

def evaluate(outcomes):
    unique={}
    for o in outcomes:
        pid=o.get('position_id')
        if not pid:raise ValueError('Outcome identity required')
        if pid in unique and dumps(unique[pid])!=dumps(o):raise ValueError('Conflicting outcome: '+pid)
        if o.get('remaining_qty')!=0 or o.get('status') not in ('STOP_HIT','TP1_HIT','TP2_HIT','TIME_EXIT','INVALIDATION_EXIT'):raise ValueError('Incomplete outcome')
        if not all(isinstance(o.get(k),(int,float)) and math.isfinite(o[k]) for k in ('net','net_r','initial_risk','closed_ms')) or o['initial_risk']<=0:raise ValueError('Invalid accounting')
        unique[pid]=o
    funded=[o for o in unique.values() if o.get('funding_complete') is True]
    values=[o['net_r'] for o in funded];days={}
    for o in funded:days.setdefault(o['closed_ms']//86400000,[]).append(o['net_r'])
    interval=None
    if len(days)>=20:
        rng=random.Random(6001);blocks=list(days.values());means=[]
        for _ in range(2000):
            sample=[x for b in rng.choices(blocks,k=len(blocks)) for x in b];means.append(statistics.mean(sample))
        means.sort();interval=[means[100],means[1899]]
    return dict(unique_positions=len(unique),funding_reconciled=len(funded),excluded_pending_funding=len(unique)-len(funded),
                total_net=sum(o['net'] for o in funded),mean_net_r=statistics.mean(values) if values else None,
                win_fraction=sum(v>0 for v in values)/len(values) if values else None,
                observed_days=len(days),exploratory_daily_block_90pct_interval=interval,
                verdict='EDGE_NOT_CERTIFIED',notes='Descriptive paper results. Daily blocks do not remove strategy-selection bias or prove future performance.')
