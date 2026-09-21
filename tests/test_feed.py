import asyncio,pytest,json
from radar.market import Feed
from radar.engine import Engine
from radar.config import Config
from .fixtures import *

class FakeClient:
 def now(self):return BASE+3000
 async def get(self,path,**kw):
  if path.endswith('exchangeInfo'):return {'symbols':[raw_meta()]}
  if path.endswith('24hr'):return [dict(symbol=SYM,quoteVolume='100000000',priceChangePercent='8')]
  if path.endswith('klines'):
   result=[]
   for i in range(120):
    at=BASE-120*60000+i*60000;lo,hi,cl=(80,130,105) if i<90 else (100,110,102 if (i//3)%2==0 else 108)
    result.append([at,105,hi,lo,cl,100+i%7,at+59999])
   return result
  if path.endswith('openInterestHist'):return [dict(timestamp=BASE-19*300000+i*300000,sumOpenInterest=10000*1.001**i) for i in range(20)]
  raise AssertionError(path)

def test_feed_composition_from_raw_rest(tmp_path):
 async def run():
  e=Engine(tmp_path/'feed.db');f=Feed(e,FakeClient())
  async def parked(*args):await asyncio.Event().wait()
  f.stream=parked # Network sockets only; real REST normalization, queue, reducer and selection.
  worker=asyncio.create_task(f.worker())
  try:
   await f.refresh();await f.queue.join()
   assert f.selected==[SYM];assert e.state(SYM)['episode']
   now=BASE+4000
   prices=[100.3]*22+[99.5,100.2]+[100.35+i*.06 for i in range(6)]
   for i,p in enumerate(prices):
    now+=600
    for raw in [dict(e='depthUpdate',s=SYM,E=now,u=now,b=[[str(p-.01),'10000']],a=[[str(p+.01),'10000']]),dict(e='bookTicker',s=SYM,E=now,u=now,b=str(p-.01),a=str(p+.01),B='10000',A='10000'),dict(e='aggTrade',s=SYM,E=now,T=now,a=i+1,p=str(p),q='10',m=False)]:
     await f.emit(normalize(raw,now))
   await f.queue.join();assert e.positions()
  finally:
   worker.cancel()
   for t in f.stream_tasks:t.cancel()
   await asyncio.gather(worker,*f.stream_tasks,return_exceptions=True);e.close()
 asyncio.run(run())

def test_readonly_dashboard_serves_status(tmp_path):
 import urllib.request
 from radar.__main__ import dashboard
 e=Engine(tmp_path/'http.db');f=Feed(e,FakeClient());server=dashboard(e,f,0)
 try:
  base='http://127.0.0.1:'+str(server.server_port)
  assert 'No real orders' in urllib.request.urlopen(base).read().decode()
  status=json.loads(urllib.request.urlopen(base+'/status.json').read())
  assert status['mode']=='paper' and status['operational']=='WARMING_OR_DEGRADED'
 finally:server.shutdown();server.server_close();e.close()
