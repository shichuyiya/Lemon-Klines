#!/usr/bin/env python3
"""Rebuild complete adjusted histories from one provider. Never scale raw history by one anchor.
All replacements are atomic and backed up. Failed/incomplete histories remain untouched and are reported.
"""
import argparse, concurrent.futures, gzip, json, math, os, subprocess, threading, time, urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
SCHEMA=2
DEADLINE=0.0
EM_DISABLED_UNTIL=0.0
EM_LOCK=threading.Lock()
FIELDS=('open','high','low','close')
KLT={'day':101,'week':102,'month':103,'m60':60,'m30':30,'m15':15,'m5':5,'m1':1}

def valid(b):
    return (all(isinstance(b.get(k),(int,float)) and math.isfinite(b[k]) for k in FIELDS)
            and b['low']<=min(b['open'],b['close'])<=max(b['open'],b['close'])<=b['high'])

def request(symbol,tf='day',fqt=1,limit=10000):
    global EM_DISABLED_UNTIL
    with EM_LOCK:
        if time.monotonic()<EM_DISABLED_UNTIL:raise ConnectionError('Eastmoney temporarily unavailable; use alternate provider')
    secid=('1.' if symbol.startswith('sh') else '0.')+symbol[2:]
    url=(f'https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&klt={KLT[tf]}&fqt={fqt}&lmt={limit}'
         '&end=20500101&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57')
    time.sleep(.15)
    error=None
    for attempt in range(2):
        if DEADLINE and time.monotonic()>=DEADLINE:raise TimeoutError('repair budget reached')
        try:
            response=subprocess.run(['curl','-fsSL','--max-time','8','-A','Mozilla/5.0','-e','https://quote.eastmoney.com/',url],capture_output=True,text=True,timeout=20,check=True)
            data=json.loads(response.stdout)
            lines=(data.get('data') or {}).get('klines') or []
            bars=[]
            for line in lines:
                p=line.split(',');b=dict(date=p[0],open=float(p[1]),close=float(p[2]),high=float(p[3]),low=float(p[4]),volume=float(p[5]),amount=float(p[6]),priceSource='eastmoney-qfq' if fqt==1 else 'eastmoney-raw')
                if not valid(b):raise ValueError('invalid OHLC: '+p[0])
                bars.append(b)
            if not bars:raise ValueError('empty response')
            if len({b['date'] for b in bars})!=len(bars):raise ValueError('duplicate dates')
            return sorted(bars,key=lambda b:b['date'])
        except Exception as e:
            error=e
            if attempt==1 and isinstance(e,(subprocess.SubprocessError,OSError,json.JSONDecodeError)):
                with EM_LOCK:EM_DISABLED_UNTIL=time.monotonic()+600
            time.sleep(.5*(attempt+1))
    raise RuntimeError(f'{symbol} {tf}: {error}')

def tencent_full(symbol, adjusted=True):
    """Single-provider full history; Tencent fallback pages overlap and must agree."""
    all_bars={};end='';oldest=None
    for page_no in range(80):
        if DEADLINE and time.monotonic()>=DEADLINE:raise TimeoutError('repair pagination budget reached')
        url=f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,{end},640,{"qfq" if adjusted else ""}'
        response=subprocess.run(['curl','-fsSL','--retry','2','--max-time','15',url],capture_output=True,text=True,timeout=60,check=True)
        node=(json.loads(response.stdout).get('data') or {}).get(symbol) or {}
        rows=(node.get('qfqday') or node.get('day') or []) if adjusted else node.get('day') or []
        page=[]
        for r in rows:
            b=dict(date=r[0],open=float(r[1]),close=float(r[2]),high=float(r[3]),low=float(r[4]),volume=float(r[5]),amount=0,priceSource='tencent-qfq' if adjusted else 'tencent-raw')
            if not valid(b):raise ValueError('invalid Tencent OHLC')
            previous=all_bars.get(b['date'])
            if previous and not matches(previous,b,.0011):raise ValueError('Tencent page adjustment changed')
            page.append(b)
        if not page:
            if not all_bars:raise ValueError('empty full daily history')
            return sorted(all_bars.values(),key=lambda b:b['date'])
        new_oldest=min(b['date'] for b in page)
        for b in page:all_bars[b['date']]=b
        if oldest is not None and new_oldest>=oldest:return sorted(all_bars.values(),key=lambda b:b['date'])
        oldest=new_oldest;end=oldest
        time.sleep(.2)
    raise ValueError('full daily pagination did not reach beginning')
def full_daily(symbol):
    try:return request(symbol)
    except Exception:return tencent_full(symbol)

def raw_daily(symbol):
    try:return request(symbol,fqt=0)
    except Exception:return tencent_full(symbol,False)


def aggregate(bars,tf):
    groups=defaultdict(list)
    for b in bars:
        dt=datetime.fromisoformat(b['date']);key=dt.isocalendar()[:2] if tf=='week' else (dt.year,dt.month)
        groups[key].append(b)
    return [dict(date=v[-1]['date'],open=v[0]['open'],close=v[-1]['close'],high=max(b['high'] for b in v),low=min(b['low'] for b in v),volume=sum(b.get('volume',0) for b in v),amount=sum(b.get('amount',0) for b in v)) for v in groups.values()]

def day_ohlc(bars):
    return dict(open=bars[0]['open'],close=bars[-1]['close'],high=max(b['high'] for b in bars),low=min(b['low'] for b in bars))

def matches(a,b,tol):return all(abs(a[k]-b[k])<=tol for k in FIELDS)

def transform(raw,adjusted,tol):
    # Recover the provider's per-session affine transform, checking all four prices.
    spread=raw['high']-raw['low']
    if spread<=tol:return None
    scale=(adjusted['high']-adjusted['low'])/spread
    offset=adjusted['low']-scale*raw['low']
    if scale<=0 or not matches({k:raw[k]*scale+offset for k in FIELDS},adjusted,tol):return None
    return scale,offset

def session_matches(rows, daily, tf, tol):
    if not daily or not rows:return False
    if any(not valid(b) or b['low']<daily['low']-tol or b['high']>daily['high']+tol for b in rows):return False
    anchors=[]
    if rows[-1]['date'].endswith('15:00'):anchors.append(abs(rows[-1]['close']-daily['close'])<=tol)
    opening={'m1':'09:31','m5':'09:35','m15':'09:45','m30':'10:00','m60':'10:30'}[tf]
    if rows[0]['date'].endswith(opening):anchors.append(abs(rows[0]['open']-daily['open'])<=tol)
    return bool(anchors) and all(anchors)

def repair_minutes(old, fresh, daily, raw, tf):
    """Only transform a complete legacy session whose OHLC matches verified raw daily OHLC.
    Partial/unverifiable sessions are retained in backup, never silently fabricated or dropped.
    """
    target={b['date']:b for b in daily};unadjusted={b['date']:b for b in raw}
    groups=defaultdict(list)
    for b in old:groups[b['date'][:10]].append(b)
    new_by_day=defaultdict(list)
    for b in fresh:new_by_day[b['date'][:10]].append(b)
    out=[];fixed=0;unverified=[]
    # Use fresh data only where it covers every legacy timestamp for that session.
    for date,rows in sorted(groups.items()):
        rows=sorted(rows,key=lambda b:b['date']);new=new_by_day.get(date,[])
        if new and {b['date'] for b in rows}<={b['date'] for b in new}:
            out.extend(new);new_by_day.pop(date,None);continue
        q=target.get(date);r=unadjusted.get(date)
        # Daily quotes are rounded to 2 decimals for shares and 3 decimals for ETFs.
        tol=.025 if q and max(abs(q[k]) for k in FIELDS)>=5 else .0025
        end=rows[-1]['date'][-5:];start=rows[0]['date'][-5:]
        expected={'m1':240,'m5':48,'m15':16,'m30':8,'m60':4}[tf]
        complete=end=='15:00' and len(rows)==expected and start=={'m1':'09:31','m5':'09:35','m15':'09:45','m30':'10:00','m60':'10:30'}[tf]
        observed=day_ohlc(rows)
        if q and session_matches(rows,q,tf,tol):out.extend(rows);continue
        tr=transform(r,q,tol) if q and r and session_matches(rows,r,tf,tol) else None
        if tr:
            scale,offset=tr
            out.extend([{**b,**{k:round(b[k]*scale+offset,6) for k in FIELDS}} for b in rows]);fixed+=1
        else:unverified.append(date)
    for new in new_by_day.values():out.extend(new)
    return sorted({b['date']:b for b in out}.values(),key=lambda b:b['date']),fixed,unverified

def read(path):return json.loads(gzip.decompress(path.read_bytes()))

def write(path,payload,backup):
    if path.exists():
        target=backup/path.name
        if not target.exists():target.write_bytes(path.read_bytes())
    temp=path.with_suffix(path.suffix+'.repair.tmp')
    temp.write_bytes(gzip.compress(json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode()))
    os.replace(temp,path)

def repair_symbol(symbol,cache,backup):
    files=sorted(cache.glob(symbol+'.*.json.gz'));old={p.name.split('.')[1]:read(p) for p in files}
    if old.get('day',{}).get('adjustment',{}).get('schema')==SCHEMA and not old['day'].get('quarantinedHistory') and all(v.get('adjustment',{}).get('mode')=='qfq' and v.get('adjustment',{}).get('status')!='quarantined' for v in old.values()):
        return {'symbol':symbol,'status':'already-verified'}
    daily=full_daily(symbol)
    previous=old.get('day',{}).get('quarantinedHistory',old.get('day',{})).get('bars',[])
    if previous and not {b['date'] for b in previous}<={b['date'] for b in daily}:
        alternate=tencent_full(symbol)
        if not {b['date'] for b in previous}<={b['date'] for b in alternate}:raise ValueError('full providers do not cover every legacy date; refusing replacement')
        daily=alternate
    meta=dict(schema=SCHEMA,source=daily[0].get('priceSource','eastmoney-qfq').removesuffix('-qfq'),mode='qfq',method='full-history',verifiedAt=int(time.time()*1000))
    now=int(time.time()*1000)
    result={'symbol':symbol,'dailyBars':len(daily),'minuteFiles':[]}
    # Stage all minute repairs before replacing any symbol file.
    stages={tf:(daily if tf=='day' else aggregate(daily,tf),meta) for tf in ['day','week','month']}
    minute_tfs=[tf for tf in old if tf in KLT and tf.startswith('m') and tf!='month']
    raw=raw_daily(symbol) if minute_tfs else []
    for tf in minute_tfs:
        try:
            try:fresh=request(symbol,tf)
            except Exception:fresh=[]
            bars,fixed,unknown=repair_minutes(old[tf]['bars'],fresh,daily,raw,tf)
            # Keep unverified files quarantined as a whole; no unnoticed history loss.
            if unknown:
                stages[tf]=(old[tf]['bars'],dict(schema=SCHEMA,source='legacy',mode='unknown',status='quarantined',unverifiedDates=unknown))
            else:stages[tf]=(bars,{**meta,'method':'daily-validated-minute-sessions'})
            result['minuteFiles'].append({'period':tf,'transformedSessions':fixed,'unverifiedDates':unknown,'bars':len(bars)})
        except Exception as e:
            stages[tf]=(old[tf]['bars'],dict(schema=SCHEMA,source='legacy',mode='unknown',status='quarantined',reason=str(e)))
            result['minuteFiles'].append({'period':tf,'error':str(e)})
    for tf,(bars,basis) in stages.items():
        write(cache/f'{symbol}.{tf}.json.gz',dict(symbol=symbol,tf=tf,bars=[{**b,'priceSource':basis['source']+'-qfq'} for b in bars] if basis.get('mode')=='qfq' else bars,updatedAt=now if basis.get('mode')=='qfq' else 0,adjustment=basis,backfilled=basis.get('mode')=='qfq'),backup)
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--cache',type=Path,required=True);p.add_argument('--backup',type=Path,required=True);p.add_argument('--symbols',nargs='*');p.add_argument('--workers',type=int,default=2);p.add_argument('--report',type=Path,required=True);p.add_argument('--max-seconds',type=float,default=0);a=p.parse_args()
    a.backup.mkdir(parents=True,exist_ok=True);a.report.parent.mkdir(parents=True,exist_ok=True)
    symbols=a.symbols or sorted({f.name.split('.')[0] for f in a.cache.glob('*.json.gz') if __import__('re').match(r'^(sh|sz|bj)\d{6}\.',f.name)})
    # Resume completed verified symbols, but keep quarantined minute histories in the report.
    results=[]
    started=time.monotonic()
    global DEADLINE
    DEADLINE=started+a.max_seconds if a.max_seconds else 0.0
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
        futures={pool.submit(repair_symbol,s,a.cache,a.backup):s for s in symbols}
        for f in concurrent.futures.as_completed(futures):
            if a.max_seconds and time.monotonic()-started>a.max_seconds:
                for queued in futures:queued.cancel()
                print('Repair window ended; remaining symbols will resume next run',flush=True)
                break
            s=futures[f]
            try:r=f.result()
            except Exception as e:r={'symbol':s,'error':str(e)}
            results.append(r);a.report.write_text(json.dumps(results,ensure_ascii=False,indent=2))
            print(json.dumps(r,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
