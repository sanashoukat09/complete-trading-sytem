import hashlib,json,math
from dataclasses import dataclass,asdict
from decimal import Decimal,ROUND_FLOOR,ROUND_CEILING

def dumps(x):return json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(x):return hashlib.sha256(dumps(x).encode()).hexdigest()
def positive(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) and x>0

def round_step(x,step,up=False):
    if not positive(step) or not positive(x):raise ValueError('Invalid price/quantity step')
    return float((Decimal(str(x))/Decimal(str(step))).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR)*Decimal(str(step)))

@dataclass(frozen=True)
class Event:
    kind:str
    symbol:str
    key:str
    at:int
    received:int
    data:dict
    version:int=1
    raw:object=None
    processed_ms:int|None=None
    @property
    def decision_ms(self):return max(self.received,self.processed_ms or self.received)
    def identity(self):return f'binance:USDT-PERP:{self.symbol}:{self.kind}:{self.key}'
    def semantic_hash(self):return digest({'kind':self.kind,'symbol':self.symbol,'key':self.key,'at':self.at,'data':self.data,'version':self.version})
    def validate(self):
        if self.kind not in {'HEALTH','META','UNIVERSE','BAR','OI','QUOTE','DEPTH','TRADE','GAP','TIMER','FUNDING','FUNDING_SYNC'}:raise ValueError('Unknown event kind')
        if not self.key or not self.symbol or self.at<0 or self.received<0:raise ValueError('Invalid event identity/time')
        if self.at>self.received+5000:raise ValueError('Exchange event is in the future')
        dumps(asdict(self))
        return self
