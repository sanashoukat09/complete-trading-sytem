"""Synthetic demonstration only; not historical or profitability evidence."""
from radar.model import Event
from radar.market import candle,normalize,metadata
BASE=1800000000000
SYM='TESTUSDT'
def raw_meta():
 return dict(symbol=SYM,status='TRADING',contractType='PERPETUAL',quoteAsset='USDT',filters=[
  dict(filterType='PRICE_FILTER',tickSize='.01'),dict(filterType='LOT_SIZE',stepSize='.01',minQty='.01',maxQty='10000'),dict(filterType='MIN_NOTIONAL',notional='5')])
def bootstrap(engine,symbol=SYM):
 now=BASE
 engine.ingest(Event('META',symbol,'meta',now,now,metadata(raw_meta())))
 engine.ingest(Event('UNIVERSE','*','u:'+symbol,now,now,dict(selected=[symbol],ranking=[])))
 for i in range(120):
  at=BASE-120*60000+i*60000
  if i<90:lo,hi,cl=80,130,105
  else:lo,hi,cl=100,110,102 if (i//3)%2==0 else 108
  row=[at,105,hi,lo,cl,100+i%7,at+59999]
  engine.ingest(candle(symbol,row,BASE+3000))
 for i in range(20):
  at=BASE-19*300000+i*300000
  engine.ingest(Event('OI',symbol,'5m:'+str(at),at,BASE+3000,dict(value=10000*(1.001**i))))
 return BASE+4000

def quote(engine,now,price,symbol=SYM,idx=None):
 idx=idx or str(now)
 engine.ingest(normalize(dict(e='depthUpdate',s=symbol,E=now,u=int(now),b=[[str(price-.01),'10000']],a=[[str(price+.01),'10000']]),now))
 engine.ingest(normalize(dict(e='bookTicker',s=symbol,E=now,u=int(now),b=str(price-.01),a=str(price+.01),B='10000',A='10000'),now))

def setup(engine,direction=1,symbol=SYM):
 now=bootstrap(engine,symbol);tid=1
 # Prior trade reference near lower edge for spring / upper edge for upthrust.
 anchor=100.3 if direction==1 else 109.7
 prices=[anchor]*22+([99.5,100.2] if direction==1 else [110.5,109.8])+([100.35+i*.025 for i in range(6)] if direction==1 else [109.65-i*.025 for i in range(6)])
 for p in prices:
  now+=600;quote(engine,now,p,symbol)
  engine.ingest(normalize(dict(e='aggTrade',s=symbol,E=now,T=now,a=tid,p=str(p),q='10',m=direction==-1),now));tid+=1
 return now,tid
