from dataclasses import dataclass, asdict, fields
import json, math

@dataclass(frozen=True)
class Config:
    mode: str = 'paper'
    excluded_symbols: tuple = ('BTCUSDT','ETHUSDT')
    initial_equity: float = 10000
    risk_fraction: float = 0.003
    total_risk_fraction: float = 0.012
    daily_loss_fraction: float = 0.02
    max_notional_multiple: float = 1.0
    max_positions: int = 3
    fee_fraction: float = 0.0005
    slippage_fraction: float = 0.0005
    funding_allowance_fraction: float = 0.001
    min_net_rr: float = 1.5
    max_quote_age_ms: int = 1500
    max_exchange_lag_ms: int = 3000
    max_oi_age_ms: int = 900000
    min_turnover: float = 10000000
    universe_size: int = 12
    candidate_count: int = 40
    refresh_seconds: int = 300
    compression_bars: int = 30
    baseline_bars: int = 90
    contraction_ratio: float = 0.75
    max_drift_fraction: float = 0.5
    episode_ms: int = 5400000
    attempt_ms: int = 600000
    max_hold_ms: int = 14400000
    min_response_trades: int = 5
    min_response_ms: int = 2000
    aggression_fraction: float = 0.55
    min_net_move_ticks: int = 2
    partial_fraction: float = 0.5
    max_spread_fraction: float = 0.002
    depth_participation: float = 0.1
    require_depth: bool = True
    clock_uncertainty_ms: int = 2000
    max_disk_mb: int = 10000

    def validate(self):
        if self.mode not in ('paper','shadow','collection'):
            raise ValueError('Only collection, shadow and paper modes are supported; real orders are disabled')
        for f in fields(self):
            v=getattr(self,f.name)
            if f.name=='excluded_symbols':
                if not isinstance(v,(list,tuple)) or not all(isinstance(x,str) and x.endswith('USDT') for x in v):raise ValueError('Invalid excluded_symbols')
                continue
            if f.name=='mode':continue
            if f.name=='require_depth':
                if not isinstance(v,bool):raise ValueError('require_depth must be boolean')
                continue
            if isinstance(v,(bool,str)):raise ValueError(f'{f.name} has the wrong type')
            if not isinstance(v,(int,float)) or not math.isfinite(v) or v<0 or (v==0 and f.name not in ('fee_fraction','slippage_fraction','funding_allowance_fraction')):
                raise ValueError(f'{f.name} must be positive and finite')
            if isinstance(f.default,int) and not isinstance(v,int):
                raise ValueError(f'{f.name} must be an integer')
        if not 0<self.risk_fraction<=self.total_risk_fraction<=self.daily_loss_fraction<1:
            raise ValueError('Require risk <= total risk <= daily budget < 1')
        for k in ('partial_fraction','contraction_ratio','max_drift_fraction','aggression_fraction','depth_participation'):
            if not 0<getattr(self,k)<1:raise ValueError(k+' must be a fraction in (0,1)')
        for k in ('fee_fraction','slippage_fraction','funding_allowance_fraction'):
            if not 0<=getattr(self,k)<1:raise ValueError(k+' must be in [0,1)')
        if self.compression_bars<10 or self.baseline_bars<self.compression_bars:raise ValueError('Insufficient structural history')
        if self.candidate_count<self.universe_size:raise ValueError('candidate_count must cover universe_size')
        return self

    def to_dict(self):return asdict(self)
    @classmethod
    def load(cls,path):
        d=json.load(open(path,encoding='utf-8'))
        unknown=set(d)-{f.name for f in fields(cls)}
        if unknown:raise ValueError(f'Unknown configuration fields: {sorted(unknown)}')
        return cls(**d).validate()
