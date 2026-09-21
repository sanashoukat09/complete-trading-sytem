import asyncio,json,sqlite3
from dataclasses import replace
import pytest
from radar.engine import Engine
from radar.config import Config
from radar.model import Event,dumps,event_from_payload,unpack_event_payload
from radar.strategy import observe,compression,initial
from radar.market import Feed
from radar.__main__ import enrich_status
from .fixtures import *
from .test_feed import FakeClient

@pytest.fixture
def eng(tmp_path):
 e=Engine(tmp_path/'r.db');yield e;e.close()

def test_pending_lookup_uses_index(eng):
 plan=str(eng.db.execute('EXPLAIN QUERY PLAN SELECT seq,payload FROM events WHERE applied=0 ORDER BY seq LIMIT 32').fetchall()[0][3])
 assert 'pending_events' in plan and 'SEARCH' in plan

def test_batch_fault_rolls_back_state_and_all_offsets(eng):
 events=[Event('META',s,s,BASE,BASE,metadata(raw_meta())) for s in ['AUSDT','BUSDT']]
 def fault(_,seq):
  if seq==2:raise OSError('power loss')
 eng.fault=fault
 with pytest.raises(OSError):eng.ingest_many(events)
 assert not eng.state('AUSDT')['meta'] and eng.status()['pending_events']==2
 eng.fault=None;eng.recover()
 assert eng.state('AUSDT')['meta'] and eng.state('BUSDT')['meta']
 eng.ingest_many(events);assert eng.status()['processed_events']==2

def test_queue_delayed_quote_cannot_fill_pending(eng,tmp_path):
 source=Engine(tmp_path/'source.db');setup(source)
 for row in source.db.execute('select payload from events order by seq'):
  eng.ingest(event_from_payload(row[0]))
  if eng.positions():break
 source.close();p=eng.positions()[0];now=p['created_ms'];assert p['status']=='PENDING'
 ev=normalize(dict(e='bookTicker',s=SYM,E=now+100,u=now+100,b='100.45',a='100.47',B='100',A='100'),now+100)
 eng.ingest(replace(ev,processed_ms=now+10000))
 assert eng.positions(False)[0]['status']=='CANCELED'
 assert eng.db.execute('select count(*) from executions').fetchone()[0]==0

def test_processed_clock_is_persisted_and_quote_age_not_rewritten(eng):
 ev=Event('QUOTE',SYM,'q',BASE,BASE,dict(bid=100,ask=101),processed_ms=BASE+5000)
 eng.ingest(ev);s=eng.state(SYM)
 assert s['quote']['received']==BASE and not eng.quote_valid(s,BASE+5000)
 assert json.loads(unpack_event_payload(eng.db.execute('select payload from events').fetchone()[0]))['processed_ms']==BASE+5000

def test_timer_preserves_flow_blocker(eng):
 bootstrap(eng);eng.ingest(Event('TRADE',SYM,'delayed',BASE,BASE+10000,dict(trade_id=1,price=100.,quantity=1.,buyer_maker=False)))
 eng.ingest(Event('TIMER',SYM,'timer',BASE+10001,BASE+10001,{}))
 assert eng.state(SYM)['reason']=='DELAYED_TRADE'
 assert eng.db.execute("select count(*) from decisions where reason='DELAYED_TRADE'").fetchone()[0]>=1

def test_episode_expires_on_quote_not_only_trade(eng):
 bootstrap(eng);ep=eng.state(SYM)['episode'];quote(eng,ep['expires']+1,105)
 assert eng.state(SYM)['episode'] is None

def test_dashboard_stale_price_never_becomes_bar_close(eng):
 now=bootstrap(eng);quote(eng,now,100.1)
 client=FakeClient();client.now=lambda:now+10000;feed=Feed(eng,client);feed.selected=[SYM]
 status=enrich_status(eng,feed);m=status['market_states'][SYM]
 assert m['price'] is None and m['last_closed_bar_price'] is not None
 assert not status['approaching'];assert status['compressions'][0]['pos_pct'] is None
 assert status['operational']=='WARMING_OR_DEGRADED'

def test_dashboard_excludes_expired_unstreamed_ranges(eng):
 bootstrap(eng);client=FakeClient();client.now=lambda:BASE+10000000
 assert not enrich_status(eng,Feed(eng,client))['compressions']

def test_dashboard_fresh_boundary_zero_is_real_position(eng):
 now=bootstrap(eng);quote(eng,now,100)
 client=FakeClient();client.now=lambda:now;feed=Feed(eng,client);feed.selected=[SYM]
 status=enrich_status(eng,feed)
 assert status['compressions'][0]['pos_pct']==0
 assert status['approaching'][0]['stage']=='NEAR_SUPPORT'

def test_pin_retains_range_outside_rank_cap(tmp_path):
 async def run():
  e=Engine(tmp_path/'pin.db',replace(Config(),universe_size=1,candidate_count=1));bootstrap(e)
  class Client(FakeClient):
   async def get(self,path,**kw):
    if path.endswith('exchangeInfo'):return {'symbols':[raw_meta(),dict(raw_meta(),symbol='HOTUSDT')]}
    if path.endswith('24hr'):return [dict(symbol=SYM,quoteVolume='20000000',priceChangePercent='1'),dict(symbol='HOTUSDT',quoteVolume='100000000',priceChangePercent='50')]
    return await super().get(path,**kw)
  f=Feed(e,Client())
  async def parked(*args):await asyncio.Event().wait()
  f.stream=parked;worker=asyncio.create_task(f.worker())
  try:
   await f.refresh();assert SYM in f.selected and not e.positions()
   assert SYM in json.loads(e.db.execute('select payload from universe').fetchone()[0])['pinned']
  finally:
   worker.cancel()
   for t in f.stream_tasks:t.cancel()
   await asyncio.gather(worker,*f.stream_tasks,return_exceptions=True);e.close()
 asyncio.run(run())

def test_universe_change_preserves_existing_socket_tasks(eng):
 async def run():
  f=Feed(eng,FakeClient())
  async def parked(*a):await asyncio.Event().wait()
  f.stream=parked
  try:
   await f.sync_streams(['AUSDT']);market=f._subscriptions['market'];public=f._subscriptions['public']
   await f.sync_streams(['AUSDT','BUSDT'])
   assert f._subscriptions['market'] is market and f._subscriptions['public'] is public
   assert not market.cancelled() and f._desired['market']=={'AUSDT','BUSDT'}
   await f.sync_streams(['AUSDT'])
   assert f._subscriptions['market'] is market and f._desired['market']=={'AUSDT'}
  finally:
   for t in f.stream_tasks:t.cancel()
   await asyncio.gather(*f.stream_tasks,return_exceptions=True)
 asyncio.run(run())

def test_expired_pin_released_but_timer_can_clean_it(eng):
 bootstrap(eng);end=eng.state(SYM)['episode']['expires']+1
 assert SYM not in eng.monitoring_pins(end)
 assert SYM in eng.monitoring_pins(end,include_expired=True)

def test_historical_backfill_cannot_create_current_episode(eng):
 bootstrap(eng);s=eng.state(SYM);s['episode']=None;s['bars']=s['bars'][:-1]
 d=dict(eng.state(SYM)['bars'][-1]);observe(s,Event('BAR',SYM,'old',d['close_ms'],BASE+3600000,d),eng.cfg)
 assert s['episode'] is None

def test_compression_uses_comparable_duration_windows():
 s=initial()
 for i in range(120):
  if i<90:lo=100+i;hi=lo+1;cl=lo+.5;op=cl
  else:lo=120;hi=160;cl=122 if (i//3)%2==0 else 158;op=140
  s['bars'].append(dict(open_ms=i*60000,close_ms=i*60000+59999,open=op,high=hi,low=lo,close=cl,volume=100))
 assert compression(s,120*60000+3000,Config()) is None

def test_range_provenance_and_equal_window_metric(eng):
 bootstrap(eng);ep=eng.state(SYM)['episode']
 assert ep['source_end_ms']-ep['source_start_ms']==30*60000-1
 assert ep['baseline_width']==50 and ep['contraction']==.2

def test_health_persisted_not_in_symbol_states(eng):
 eng.ingest(Event('HEALTH','*','h',BASE,BASE,dict(queue_delay_ms=123)))
 assert eng.status()['runtime_health']['queue_delay_ms']==123
 assert not eng.db.execute("select 1 from states where symbol='*'").fetchone()

@pytest.mark.parametrize('field,value',[('max_positions',True),('max_quote_age_ms','1500'),('require_depth','false')])
def test_config_rejects_wrong_types(field,value):
 with pytest.raises(ValueError):replace(Config(),**{field:value}).validate()

def test_dashboard_mark_is_executable_side_and_not_entry(eng):
 now,_=setup(eng);quote(eng,now+1000,103)
 client=FakeClient();client.now=lambda:now+1000;f=Feed(eng,client);f.selected=[SYM]
 p=enrich_status(eng,f)['positions'][0]
 assert p['current_price']==102.99 and p['current_price']!=p['entry_price']
 assert p['unrealized_mark_pnl']==pytest.approx((102.99-p['entry_price'])*p['remaining'])

def test_full_feed_lifecycle_with_clock_and_independent_pollers(tmp_path):
 async def run():
  class Client(FakeClient):
   offset=0
   closed=False
   async def open(self):return self
   async def close(self):self.closed=True
   async def sync_clock(self):return dict(offset_ms=0,uncertainty_ms=1,synced_ms=self.now())
  e=Engine(tmp_path/'lifecycle.db');client=Client();f=Feed(e,client)
  async def parked(*args):await asyncio.Event().wait()
  f.stream=parked;task=asyncio.create_task(f.run())
  try:
   async def ready():
    while not f.selected:
     if task.done():await task
     await asyncio.sleep(.01)
   await asyncio.wait_for(ready(),5)
   f.stop.set();await asyncio.wait_for(task,5)
   assert client.closed and not e.status()['pending_events']
   assert 'clock' in f.health and all(t.done() for t in f.stream_tasks)
  finally:
   if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
   e.close()
 asyncio.run(run())

def test_batch_and_single_reducers_match(tmp_path):
 source=Engine(tmp_path/'single.db');setup(source)
 batch=Engine(tmp_path/'batch.db');events=[event_from_payload(r[0]) for r in source.db.execute('select payload from events order by seq')]
 for i in range(0,len(events),32):batch.ingest_many(events[i:i+32])
 assert batch.positions(False)==source.positions(False)
 assert batch.state(SYM)==source.state(SYM)
 assert batch.status()['processed_events']==source.status()['processed_events']
 batch.close();source.close()
