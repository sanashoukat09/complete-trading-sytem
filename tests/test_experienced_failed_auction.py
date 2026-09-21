import pytest
from radar.strategy import initial, observe
from radar.model import Event
from radar.config import Config

SYM='XUSDT'; BASE=1900000000000

def state(direction=1):
    s=initial()
    s['meta']=dict(tick_size=.01,step_size=.01,min_qty=.01,max_qty=1e6,min_notional=5,status='TRADING')
    s['bars']=[dict(open_ms=BASE-60000,close_ms=BASE-1,open=105,high=110,low=100,close=105,volume=1)]
    s['episode']=dict(lower=100.,upper=110.,width=10.,created=BASE-1000,expires=BASE+600000,
                      source_start_ms=BASE-100000,source_end_ms=BASE-1,baseline_width=20.,contraction=.5,
                      id='EP',version=1,rotations=8)
    s['warm_trades']=30;s['last_trade_id']=100;s['last_trade_ms']=BASE-10
    # Seed recent prints around the boundary so the pre-attempt reference is causal.
    anchor=100.3 if direction==1 else 109.7
    s['trades']=[dict(price=anchor,qty=1,buy=True,at=BASE-100+i,id=f'seed{i}') for i in range(20)]
    return s

def trade(s,tid,ms,price,buy,cfg=None):
    e=Event('TRADE',SYM,str(tid),BASE+ms,BASE+ms,dict(trade_id=tid,price=price,quantity=10.,buyer_maker=not buy))
    return observe(s,e,cfg or Config())

def test_spring_retest_is_not_one_tick_invalidated():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    assert trade(s,101,0,99.5,False,c) is None
    assert trade(s,102,10,100.2,True,c) is None
    trade(s,103,20,100.45,True,c); trade(s,104,30,100.5,False,c)
    # A controlled revisit toward the boundary remains a live Spring hypothesis.
    trade(s,105,40,100.08,False,c)
    assert s['attempt'] is not None
    assert s['reason'] in {'WAIT_RETEST_DEFENSE','NO_EFFECTIVE_RESPONSE','RECLAIM_UNDER_PRESSURE'}

def test_true_acceptance_after_reclaim_kills_spring():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    trade(s,101,0,99.5,False,c);trade(s,102,10,100.2,True,c)
    trade(s,103,20,99.85,False,c);trade(s,104,40,99.80,False,c)
    assert s['attempt'] is None
    assert s['reason']=='OUTSIDE_ACCEPTANCE_AFTER_RECLAIM'

def test_price_led_response_can_confirm_without_55pct_favorable_flow():
    s=state(1);c=Config(min_response_trades=5,min_response_ms=1,aggression_fraction=.55)
    trade(s,101,0,99.5,False,c);trade(s,102,10,100.2,True,c)
    # Only 40% favorable quantity, but price makes a strong favorable response.
    out=None
    seq=[(100.35,False),(100.45,False),(100.55,False),(100.65,True),(100.75,True)]
    for i,(p,buy) in enumerate(seq,103):out=trade(s,i,(i-100)*10,p,buy,c)
    assert out is not None
    assert out['explanation']['route']=='DIRECT_PRICE_LED_RESPONSE'
    assert out['explanation']['aggression']==pytest.approx(.4)

def test_failed_auction_logic_is_mirrored_for_upthrust():
    c=Config(min_response_trades=5,min_response_ms=1)
    long=state(1); short=state(-1)
    lp=[(99.5,False),(100.2,True),(100.35,True),(100.4,True),(100.45,True),(100.5,True),(100.55,True)]
    sp=[(110.5,True),(109.8,False),(109.65,False),(109.6,False),(109.55,False),(109.5,False),(109.45,False)]
    lo=so=None
    for i,((a,ab),(b,bb)) in enumerate(zip(lp,sp),101):
        lo=trade(long,i,(i-100)*10,a,ab,c) or lo
        so=trade(short,i,(i-100)*10,b,bb,c) or so
    assert lo and so
    assert lo['direction']==1 and so['direction']==-1
    assert lo['explanation']['route']==so['explanation']['route']=='DIRECT_FLOW_RESPONSE'
    assert lo['explanation']['kind']=='SPRING' and so['explanation']['kind']=='UPTHRUST'

def test_retest_only_mode_does_not_chase_direct_response():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    trade(s,101,0,99.5,False,c);trade(s,102,10,100.2,True,c)
    trade(s,103,20,100.5,True,c);trade(s,104,30,100.6,True,c)
    assert s['attempt']['best_progress']>0
    s['attempt']['entry_mode']='RETEST_ONLY'
    # More favorable extension must not become a chased direct entry.
    assert trade(s,105,40,100.8,True,c) is None
    assert s['reason']=='WAIT_BETTER_ENTRY_RETEST'
    # A controlled revisit starts the distinct retest route.
    assert trade(s,106,50,100.05,False,c) is None
    assert s['attempt']['stage']=='RETEST'
    out=None
    for tid,p in [(107,100.12),(108,100.24),(109,100.40)]:
        out=trade(s,tid,50+(tid-106)*10,p,True,c) or out
    assert out is not None and out['explanation']['route']=='DEFENDED_RETEST'

def test_public_book_gap_preserves_failed_auction_attempt():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    trade(s,101,0,99.5,False,c)
    assert s['attempt'] is not None
    e=Event('GAP',SYM,'bookgap',BASE+5,BASE+5,dict(reason='STREAM_CONNECT_OR_RECONNECT',group='public'))
    observe(s,e,c)
    assert s['attempt'] is not None
    assert s['quote'] is None and s['depth'] is None
    assert s['reason']=='BOOK_GAP_WAIT_LIQUIDITY'


def test_market_trade_gap_invalidates_attempt():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    trade(s,101,0,99.5,False,c)
    assert s['attempt'] is not None
    e=Event('GAP',SYM,'tradegap',BASE+5,BASE+5,dict(reason='STREAM_CONNECT_OR_RECONNECT',group='market'))
    observe(s,e,c)
    assert s['attempt'] is None and s['warm_trades']==0
    assert s['reason']=='TRADE_GAP_WARMUP'


def test_micro_probe_is_not_promoted_to_failed_auction():
    s=state(1);c=Config(min_response_trades=2,min_response_ms=1)
    # 4% of the 10-point balance width: outside the edge, but not a genuine 5% sweep.
    assert trade(s,101,0,99.6,False,c) is None
    assert s['attempt'] is None
    assert s['reason']=='WAIT_BOUNDARY_SWEEP'
    # The normalized genuine-sweep boundary does create the attempt.
    assert trade(s,102,10,99.5,False,c) is None
    assert s['attempt'] is not None and s['attempt']['stage']=='OUTSIDE'


def test_mixed_effort_price_led_route_is_stronger_than_aligned_threshold():
    s=state(1);c=Config(min_response_trades=5,min_response_ms=1,aggression_fraction=.55)
    # Make outside participation comparable to its baseline -> mixed effort/result.
    e=Event('TRADE',SYM,'101',BASE,BASE,dict(trade_id=101,price=99.5,quantity=20.,buyer_maker=True))
    observe(s,e,c)
    trade(s,102,10,100.2,True,c)
    assert s['attempt']['quality']['route']=='MIXED_EFFORT_RESULT'
    # 40% favorable flow and ~6% of range progress is not 'strong' enough for mixed effort.
    out=None
    seq=[(100.4,False),(100.5,False),(100.6,False),(100.75,True),(100.9,True)]
    for i,(px,buy) in enumerate(seq,103): out=trade(s,i,(i-100)*10,px,buy,c) or out
    assert out is None
    # Continued price response >7.5% of the range can qualify the price-led route.
    out=trade(s,108,90,101.1,True,c)
    assert out is not None and out['explanation']['route']=='DIRECT_PRICE_LED_RESPONSE'
