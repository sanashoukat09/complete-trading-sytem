import json,sqlite3,random,pytest,math
from dataclasses import replace
from radar.config import Config
from radar.engine import Engine
from radar.model import Event,dumps
from radar.market import normalize,candle,metadata
from .fixtures import *

@pytest.fixture
def eng(tmp_path):
 e=Engine(tmp_path/'state.db');yield e;e.close()

@pytest.mark.parametrize('direction',[1,-1])
def test_raw_pipeline_generates_evidenced_intent(eng,direction):
 now,tid=setup(eng,direction)
 assert eng.state(SYM)['episode']
 ps=eng.positions();assert len(ps)==1,eng.status()['decisions']
 p=ps[0];assert p['direction']==direction and p['evidence']['evidence']
 assert p['status'] in ('PENDING','OPEN')

@pytest.mark.parametrize('direction',[1,-1])
def test_restart_and_exit_ledger(eng,direction):
 now,tid=setup(eng,direction);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+direction*eng.cfg.slippage_fraction))
 before=eng.positions()[0];assert before['status']=='OPEN'
 e2=Engine(eng.path,eng.cfg);assert e2.positions()==eng.positions()
 quote(e2,now+1000,112 if direction==1 else 98)
 assert not e2.positions()
 p=e2.positions(False)[0];assert p['remaining']==0 and p['status']=='CLOSED'
 rows=[json.loads(x[0]) for x in e2.db.execute('SELECT payload FROM executions')]
 assert len(rows)>=2
 expected=sum(direction*(x['price']-p['entry_price'])*x['qty'] for x in rows if x['reason']!='ENTRY')-sum(x['fee'] for x in rows)
 assert p['net']==pytest.approx(expected);e2.close()

def test_duplicate_event_exactly_once(eng):
 e=Event('TIMER',SYM,'a',BASE,BASE,{})
 eng.ingest(e);eng.ingest(e);assert eng.status()['processed_events']==1
 with pytest.raises(ValueError):eng.ingest(replace(e,data={'changed':True}))

def test_crash_before_commit_recovers_without_double_effect(eng):
 e=Event('META',SYM,'a',BASE,BASE,metadata(raw_meta()))
 def fail(*a):raise OSError('disk simulated')
 eng.fault=fail
 with pytest.raises(OSError):eng.ingest(e)
 assert eng.state(SYM)['meta'] is None and eng.status()['pending_events']==1
 eng.fault=None;eng.recover();assert eng.state(SYM)['meta'];assert eng.status()['pending_events']==0

def test_failure_blocks_later_sequence(eng):
 eng.fault=lambda *a: (_ for _ in ()).throw(OSError('failure'))
 for i in (1,2):
  with pytest.raises(OSError):eng.ingest(Event('TIMER',SYM,str(i),BASE+i,BASE+i,{}))
 assert eng.status()['processed_events']==0 and eng.status()['pending_events']==2
 eng.fault=None;eng.recover();assert eng.status()['processed_events']==2

def test_closed_attempt_never_reenters_after_restart(eng):
 now,tid=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 quote(eng,now+1000,112)
 e2=Engine(eng.path,eng.cfg)
 for i in range(20):
  t=now+2000+i*10
  e2.ingest(normalize(dict(e='aggTrade',s=SYM,E=t,T=t,a=tid+i,p='100.6',q='10',m=False),t))
 assert len(e2.positions(False))==1;e2.close()

@pytest.mark.parametrize('bid,ask,at,received',[(100,99,BASE,BASE),(float('inf'),101,BASE,BASE),(99,100,0,0),(99,100,BASE-10000,BASE),(99,100,BASE+10000,BASE+10000)])
def test_quote_invalid(eng,bid,ask,at,received):
 assert not eng.quote_valid({'quote':dict(bid=bid,ask=ask,at=at,received=received)},BASE)

def test_no_oi_no_entry(eng):
 now,tid=setup(eng)
 # A separate fixture has no injected eligibility: replay all events except OI.
 events=[Event(**json.loads(r[0])) for r in eng.db.execute("SELECT payload FROM events ORDER BY seq")]
 e2=Engine(eng.path+'.withoutoi',eng.cfg)
 for e in events:
  if e.kind!='OI':e2.ingest(e)
 assert not e2.positions();e2.close()

def test_shadow_never_executes(tmp_path):
 e=Engine(tmp_path/'s.db',Config(mode='shadow'));setup(e)
 assert not e.positions() and any(d['action']=='SIGNAL' for d in e.status()['decisions']);e.close()

def test_collection_never_signals(tmp_path):
 e=Engine(tmp_path/'c.db',Config(mode='collection'));setup(e)
 assert not e.positions() and not any(d['action']=='SIGNAL' for d in e.status()['decisions']);e.close()

def test_replay_identical_decisions_and_state(eng):
 setup(eng);e2=Engine(eng.path+'.replay',eng.cfg)
 for r in eng.db.execute('SELECT payload FROM events ORDER BY seq'):e2.ingest(Event(**json.loads(r[0])))
 assert e2.status()==eng.status();assert e2.state(SYM)==eng.state(SYM);e2.close()

def test_forming_bar_excluded(eng):
 bootstrap(eng);before=eng.state(SYM)['bars']
 eng.ingest(candle(SYM,[BASE,105,110,100,108,10,BASE+59999],BASE+1000))
 assert eng.state(SYM)['bars']==before

def test_tick_gap_revokes_attempt(eng):
 now,tid=setup(eng)
 eng.ingest(normalize(dict(e='aggTrade',s=SYM,E=now+1,T=now+1,a=tid+100,p='100.5',q='1',m=False),now+1))
 assert eng.state(SYM)['attempt'] is None and eng.state(SYM)['warm_trades']==1

def test_freeze_config(tmp_path):
 e=Engine(tmp_path/'s.db');e.close()
 with pytest.raises(ValueError):Engine(tmp_path/'s.db',Config(risk_fraction=.002))

@pytest.mark.parametrize('mode',['live','unknown'])
def test_live_disabled(mode):
 with pytest.raises(ValueError):Config(mode=mode).validate()

@pytest.mark.parametrize('seed',range(10))
def test_seeded_restart_duplicate_interleavings(tmp_path,seed):
 rng=random.Random(seed);e=Engine(tmp_path/f'{seed}.db');bootstrap(e)
 for i in range(100):
  t=BASE+10000+i;event=Event('TIMER',SYM,str(i),t,t,{})
  e.ingest(event)
  if rng.random()<.3:e.ingest(event)
  if rng.random()<.1:e.close();e=Engine(tmp_path/f'{seed}.db')
 assert e.status()['pending_events']==0
 assert e.db.execute('SELECT COUNT(*) FROM events').fetchone()[0]==e.status()['processed_events'];e.close()

@pytest.mark.parametrize('direction',[1,-1])
def test_partial_exit_restart_finishes_remaining(eng,direction):
 now,_=setup(eng,direction);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+direction*eng.cfg.slippage_fraction))
 p=eng.positions()[0];assert p['tp2'] is not None
 quote(eng,now+1000,p['tp1']+direction*.05)
 p=eng.positions()[0];assert p['partial_done'] and 0<p['remaining']<p['original_qty']
 e2=Engine(eng.path,eng.cfg);assert e2.positions()==eng.positions()
 quote(e2,now+2000,p['tp2']+direction*.1)
 assert not e2.positions();assert e2.positions(False)[0]['status']=='CLOSED';e2.close()

@pytest.mark.parametrize('direction',[1,-1])
def test_stop_gap_can_exceed_initial_risk(eng,direction):
 now,_=setup(eng,direction);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+direction*eng.cfg.slippage_fraction))
 quote(eng,now+1000,90 if direction==1 else 120)
 outcome=eng.status()['outcomes'][0]
 assert outcome['status']=='STOP_HIT' and outcome['net_r'] < -1

def test_timer_exit_waits_for_quote(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 t=now+eng.cfg.max_hold_ms+1000
 eng.ingest(Event('TIMER',SYM,'late',t,t,{}));assert eng.positions()[0]['pending_reason']=='TIME_EXIT'
 quote(eng,t+10,101);assert not eng.positions()

def test_closed_funding_settlement_uses_historical_quantity(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 p=eng.positions()[0];at=p['entry_ms']+1;qty=p['remaining']
 quote(eng,now+2000,112);before=eng.status()['outcomes'][0]['net']
 event=Event('FUNDING',SYM,'rate',at,now+3000,dict(mark=101.,rate=.001));eng.ingest(event);eng.ingest(event)
 assert eng.status()['outcomes'][0]['net']==pytest.approx(before-qty*101*.001)
 eng.ingest(Event('FUNDING_SYNC',SYM,'sync',now+4000,now+4000,dict(start=BASE-86400000,end=now+4000)))
 assert eng.status()['outcomes'][0]['funding_complete']

def test_atomic_position_and_fill_rollback(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 before=eng.positions();count=eng.db.execute('SELECT COUNT(*) FROM executions').fetchone()[0]
 def fail(*args):raise OSError('transaction fault')
 eng.fault=fail
 with pytest.raises(OSError):quote(eng,now+2000,112)
 assert eng.positions()==before and eng.db.execute('SELECT COUNT(*) FROM executions').fetchone()[0]==count
 eng.fault=None;eng.recover();quote(eng,now+2001,112);assert not eng.positions()

def test_account_cap_across_symbols(eng):
 for sym in ['AAAUSDT','BBBUSDT','CCCUSDT','DDDUSDT']:
  setup(eng,symbol=sym)
 assert len(eng.positions())<=eng.cfg.max_positions
 assert sum(x['risk_remaining'] for x in eng.positions())<=eng.cfg.initial_equity*eng.cfg.total_risk_fraction
 assert any(x['reason']=='ACCOUNT_RISK_BUDGET' for x in eng.status()['decisions'])

def test_two_connections_same_event(tmp_path):
 from concurrent.futures import ThreadPoolExecutor
 path=tmp_path/'same.db';a=Engine(path);b=Engine(path)
 e=Event('TIMER',SYM,'same',BASE,BASE,{})
 with ThreadPoolExecutor(2) as pool:list(pool.map(lambda engine:engine.ingest(e),[a,b]))
 assert a.status()['processed_events']==1;a.close();b.close()

@pytest.mark.parametrize('scale',[.0001,1.,10000.])
def test_oi_growth_unit_invariant(eng,scale):
 from radar.strategy import features
 for i in range(20):
  t=BASE+i*300000;eng.ingest(Event('OI',SYM,str(i),t,t,dict(value=scale*10000*1.001**i)))
 f=features(eng.state(SYM));assert f['oi_growth']==pytest.approx(math.log(1.001))
 assert f['oi_growth_z']==pytest.approx(0.,abs=1e-4)

def test_research_requires_identity_and_flat():
 from radar.research import evaluate
 with pytest.raises(ValueError):evaluate([dict(status='TP2_HIT',net=1,net_r=1)])
 a=dict(position_id='A',status='TP2_HIT',remaining_qty=0,net=1.,net_r=1.,initial_risk=1.,closed_ms=BASE,funding_complete=True)
 assert evaluate([a,a])['unique_positions']==1
 with pytest.raises(ValueError):evaluate([a,dict(a,net=-1.)])
 assert evaluate([dict(a,funding_complete=False)])['funding_reconciled']==0

@pytest.mark.parametrize('seed',range(10))
def test_unseen_price_paths_preserve_accounting(tmp_path,seed):
 rng=random.Random(seed+6100);e=Engine(tmp_path/f'path{seed}.db');now,tid=setup(e);p=e.positions()[0]
 if p['status']=='PENDING':quote(e,now+100,p['entry_reference']/(1+e.cfg.slippage_fraction))
 for i in range(100):
  now+=1000;price=100+sum(rng.uniform(-.4,.5) for _ in range(i%20+1))
  quote(e,now,price)
  if rng.random()<.15:e.close();e=Engine(tmp_path/f'path{seed}.db')
  for p in e.positions(False):
   assert p['remaining']>=0 and p['risk_remaining']>=0 and p['fees']>=0
   assert p['net']==pytest.approx(p['gross']-p['fees']+p['funding'])
  if not e.positions():break
 e.close()

def test_no_compression_no_setup(eng):
 bootstrap(eng)
 # Fresh independent sequence with non-contracting directional bars, no fabricated setup.
 e2=Engine(eng.path+'.trend')
 events=[Event(**json.loads(r[0])) for r in eng.db.execute('SELECT payload FROM events ORDER BY seq')]
 for ev in events:
  if ev.kind=='BAR':
   i=(ev.data['open_ms']-(BASE-120*60000))//60000
   data=dict(ev.data,open=100+i,close=100+i,low=99+i,high=101+i)
   ev=replace(ev,data=data)
  e2.ingest(ev)
 assert e2.state(SYM)['episode'] is None;e2.close()

def test_future_quote_and_trade_rejected(eng):
 with pytest.raises(ValueError):eng.ingest(Event('QUOTE',SYM,'future',BASE+100000,BASE,dict(bid=99,ask=100)))
 assert eng.status()['processed_events']==0

def test_late_trade_does_not_reset_newer_state(eng):
 bootstrap(eng)
 for i in (10,11):eng.ingest(Event('TRADE',SYM,str(i),BASE+10000,BASE+10000,dict(trade_id=i,price=105.,quantity=1.,buyer_maker=False)))
 eng.ingest(Event('TRADE',SYM,'old',BASE+10000,BASE+10001,dict(trade_id=9,price=104.,quantity=1.,buyer_maker=False)))
 assert eng.state(SYM)['last_trade_id']==11 and eng.state(SYM)['warm_trades']==2

def test_no_liquidity_does_not_fill(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 now+=1000
 eng.ingest(Event('DEPTH',SYM,'thin',now,now,dict(bids=[[89.99,.0001]],asks=[[90.01,.0001]])))
 eng.ingest(Event('QUOTE',SYM,'thin',now,now,dict(bid=89.99,ask=90.01)))
 assert eng.positions()[0]['pending_reason']=='STOP_HIT'
 assert eng.positions()[0]['remaining']>0

@pytest.mark.parametrize('scale',[.001,1000.])
def test_price_quantity_scaling_preserves_direction(tmp_path,scale):
 original=Engine(tmp_path/'original.db');setup(original)
 transformed=Engine(tmp_path/'scaled.db')
 for row in original.db.execute('SELECT payload FROM events ORDER BY seq'):
  e=Event(**json.loads(row[0]));d=dict(e.data)
  if e.kind=='META':
   d['tick_size']*=scale
   for k in ('step_size','min_qty','max_qty'):d[k]/=scale
  if e.kind=='BAR':
   for k in ('open','high','low','close'):d[k]*=scale
   d['volume']/=scale
  if e.kind=='TRADE':d['price']*=scale;d['quantity']/=scale
  if e.kind=='QUOTE':
   d['bid']*=scale;d['ask']*=scale
   for k in ('bid_qty','ask_qty'):d[k]/=scale
  if e.kind=='DEPTH':
   for k in ('bids','asks'):d[k]=[[p*scale,q/scale] for p,q in d[k]]
  transformed.ingest(replace(e,data=d,raw=None))
 assert len(transformed.positions())==len(original.positions())==1
 assert transformed.positions()[0]['direction']==original.positions()[0]['direction']
 original.close();transformed.close()

def test_realized_daily_loss_blocks_next_coin(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 quote(eng,now+1000,80)
 assert -eng.status()['outcomes'][0]['net']>eng.cfg.initial_equity*eng.cfg.daily_loss_fraction
 setup(eng,symbol='NEXTUSDT')
 assert not eng.positions()
 assert any(x['reason']=='ACCOUNT_RISK_BUDGET' for x in eng.status()['decisions'])

def test_crossed_quote_does_not_invent_stop_trigger(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 t=now+1000
 eng.ingest(Event('QUOTE',SYM,'cross',t,t,dict(bid=80.,ask=70.)))
 assert eng.positions()[0]['pending_reason'] is None

def test_trade_before_entry_does_not_trigger_exit(eng):
 now,_=setup(eng);p=eng.positions()[0]
 if p['status']=='PENDING':quote(eng,now+100,p['entry_reference']/(1+eng.cfg.slippage_fraction))
 p=eng.positions()[0];now+=1000
 eng.ingest(Event('TRADE',SYM,'oldprice',p['entry_ms']-100,now,dict(trade_id=0,price=80.,quantity=1.,buyer_maker=True)))
 assert eng.positions()[0]['pending_reason'] is None

def test_unclean_process_exit_replays_pending_event(tmp_path):
 import subprocess,sys
 path=tmp_path/'crash.db'
 script="""
import os,sys
from radar.engine import Engine
from radar.model import Event
e=Engine(sys.argv[1]);e.fault=lambda *args:os._exit(23)
e.ingest(Event('TIMER','TESTUSDT','crash',1800000000000,1800000000000,{}))
"""
 run=subprocess.run([sys.executable,'-c',script,str(path)])
 assert run.returncode==23
 e=Engine(path);assert e.status()['pending_events']==1
 e.recover();assert e.status()['pending_events']==0 and e.status()['processed_events']==1;e.close()
