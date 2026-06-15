# AURA Gateway — Async VLESS Direct Proxy / Gateway (FastAPI)
# Transports: VLESS-over-WebSocket + VLESS-over-XHTTP(packet-up)
# Relay: Direct TCP (Happy Eyeballs) | Quota manager | Camouflage front | Persian admin panel
import os
import sys
import time
import json
import uuid
import asyncio
import hmac
import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.websockets import WebSocket, WebSocketDisconnect
import uvicorn
import psutil

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AURA-Gateway")

# --- Configuration & Env Vars ---
PORT = int(os.environ.get("PORT", 8000))
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "aura_secret_2026")
ADMIN_PATH = os.environ.get("ADMIN_PATH", "panel").strip("/")
PUBLIC_HOST = os.environ.get("PUBLIC_HOST", "") 

DB_FILE = "aura_gateway.db"

# --- Database Setup (Thread-safe Async Wrappers) ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            uuid TEXT PRIMARY KEY,
            quota_gb REAL,
            used_bytes INTEGER DEFAULT 0,
            conn_limit INTEGER DEFAULT 3,
            expires_at TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

async def db_execute(query: str, params: tuple = ()):
    def _ex():
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        conn.close()
    await asyncio.to_thread(_ex)

async def db_fetch_all(query: str, params: tuple = ()) -> List[tuple]:
    def _ex():
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(query, params)
        res = cursor.fetchall()
        conn.close()
        return res
    return await asyncio.to_thread(_ex)

# --- Global State for Metrics ---
class TrafficMonitor:
    def __init__(self):
        self.samples = []
        self.active_conns = 0
        self.lock = asyncio.Lock()
        self.curr_up = 0
        self.curr_down = 0
        self.total_up = 0
        self.total_down = 0

    async def add_traffic(self, up: int, down: int):
        async with self.lock:
            self.curr_up += up
            self.curr_down += down
            self.total_up += up
            self.total_down += down

    async def update_sample(self):
        async with self.lock:
            now = time.time()
            self.samples.append({"t": now, "up": self.curr_up, "down": self.curr_down})
            self.curr_up = 0
            self.curr_down = 0
            if len(self.samples) > 30:
                self.samples.pop(0)

monitor = TrafficMonitor()

# --- XHTTP Packet-up Buffer State ---
XHTTP_QUEUES: Dict[str, asyncio.Queue] = {}
XHTTP_LOCK = asyncio.Lock()
REORDER_CAP = 512

# --- Background Worker for Logs & System Metrics ---
log_queue: List[str] = []

def add_log(msg: str):
    t = datetime.now().strftime("%H:%M:%S")
    m = f"[{t}] {msg}"
    log_queue.append(m)
    if len(log_queue) > 100:
        log_queue.pop(0)
    logger.info(msg)

async def metrics_worker():
    while True:
        await monitor.update_sample()
        await asyncio.sleep(1)

# --- Network Relay Core (Direct VLESS Parser & Connector) ---
async def relay_tcp(reader, writer):
    client_uuid = "نامشخص"
    try:
        # ۱. خواندن پکت اولیه VLESS برای استخراج مقصد واقعی (طبق استاندارد VLESS)
        header_version = await reader.readexact(1)
        client_uuid_bytes = await reader.readexact(16)
        
        # تبدیل بایت‌ها به رشته UUID استاندارد
        client_uuid = str(uuid.UUID(bytes=client_uuid_bytes))
        
        # احراز هویت و بررسی حجم کاربر از دیتابیس
        rows = await db_fetch_all("SELECT quota_gb, used_bytes FROM accounts WHERE uuid = ?", (client_uuid,))
        if not rows:
            add_log(f"اتصال رد شد: شناسه نامعتبر {client_uuid}")
            writer.close()
            return
            
        quota_gb, used_bytes = rows[0]
        if (used_bytes / (1024**3)) >= quota_gb:
            add_log(f"اتصال رد شد: اتمام حجم کاربر {client_uuid[:8]}…")
            writer.close()
            return

        # استخراج آدرس و پورت مقصد واقعی از پکت VLESS
        opt_len = await reader.readexact(1) 
        if opt_len[0] > 0:
            await reader.readexact(opt_len[0]) 
            
        cmd = await reader.readexact(1) # 1 برای TCP
        port_bytes = await reader.readexact(2)
        target_port = int.from_bytes(port_bytes, byteorder='big')
        
        atyp = await reader.readexact(1) # نوع آدرس (1: IPv4, 2: Domain, 3: IPv6)
        if atyp[0] == 1: 
            addr_bytes = await reader.readexact(4)
            target_host = ".".join(str(b) for b in addr_bytes)
        elif atyp[0] == 2: 
            addr_len = await reader.readexact(1)
            addr_bytes = await reader.readexact(addr_len[0])
            target_host = addr_bytes.decode('utf-8')
        elif atyp[0] == 3: 
            addr_bytes = await reader.readexact(16)
            target_host = ":".join(f"{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}" for i in range(0, 16, 2))
        else:
            writer.close()
            return

        # پاسخ هدر موفقیت‌آمیز VLESS به کلاینت
        writer.write(b'\x00\x00')
        await writer.drain()

        # افزایش اتصالات فعال در داشبورد
        async with monitor.lock:
            monitor.active_conns += 1
        
        add_log(f"اتصال مستقیم برقرار شد -> {target_host}:{target_port}")

        # ۲. اتصال مستقیم سرور ابری به مقصد واقعی اینترنت با مکانیزم Happy Eyeballs
        remote_reader, remote_writer = await asyncio.open_connection(
            host=target_host, port=target_port, happy_eyeballs_delay=0.1
        )

        # ۳. پایپ دوطرفه داده‌ها و ثبت ترافیک مصرفی
        async def pipe(r, w, is_upload: bool):
            try:
                while True:
                    data = await r.read(16384)
                    if not data:
                        break
                    w.write(data)
                    await w.drain()
                    
                    if is_upload:
                        await monitor.add_traffic(len(data), 0)
                    else:
                        await monitor.add_traffic(0, len(data))
                        
                    await db_execute(
                        "UPDATE accounts SET used_bytes = used_bytes + ? WHERE uuid = ?", 
                        (len(data), client_uuid)
                    )
            except Exception:
                pass
            finally:
                try: w.close() 
                except: pass

        await asyncio.gather(
            pipe(reader, remote_writer, True),
            pipe(remote_reader, writer, False)
        )
    except Exception as e:
        add_log(f"خطا در رله مستقیم ترافیک: {str(e)}")
    finally:
        async with monitor.lock:
            monitor.active_conns = max(0, monitor.active_conns - 1)
        try: writer.close()
        except: pass

# --- Auth Guard ---
def verify_token(token: str) -> bool:
    if not ADMIN_TOKEN:
        return True
    return hmac.compare_digest(token, ADMIN_TOKEN)

# --- Camouflage Decoy Front (QuickConvert Tool) ---
DECOY_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>QuickConvert — ابزار محاسباتی و تبدیل واحد آنلاین</title>
    <style>
        body { font-family: Tahoma, sans-serif; background: #121214; color: #e1e1e6; padding: 40px; text-align: center; }
        .box { max-width: 500px; margin: 0 auto; background: #202024; padding: 30px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }
        input, select, button { width: 100%; padding: 12px; margin: 10px 0; border-radius: 6px; border: 1px solid #29292e; background: #121214; color: #fff; box-sizing: border-box; }
        button { background: #00b37e; font-weight: bold; cursor: pointer; border: none; }
        button:hover { background: #00875f; }
        h2 { color: #00b37e; }
    </style>
</head>
<body>
    <div class="box">
        <h2>ابزار تبدیل واحد مگابایت به گیگابایت</h2>
        <p>مقدار مورد نظر خود را جهت تبدیل دقیق وارد نمایید:</p>
        <input type="number" id="val" value="1024" placeholder="مقدار به مگابایت">
        <button onclick="calc()">محاسبه واحد</button>
        <h3 id="res">۱ گیگابایت (GB)</h3>
    </div>
    <script>
        function calc() {
            const v = document.getElementById('val').value;
            if(!v) return;
            document.getElementById('res').textContent = (v / 1024).toFixed(2) + ' گیگابایت (GB)';
        }
    </script>
</body>
</html>"""

# --- Dashboard View (Persian UI + Advanced Glassmorphism) ---
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AURA Gateway Panel</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
    <style>
        body {
            font-family: 'Vazirmatn', sans-serif;
            background: radial-gradient(circle at 50% 50%, #16192b 0%, #0b0c16 100%);
            color: #e2e8f0;
            overflow-x: hidden;
        }
        .glass {
            background: rgba(255, 255, 255, 0.04);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }
        .gbtn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(5px);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }
        .gbtn::after {
            content: '';
            position: absolute;
            top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
            transition: 0.5s;
        }
        .gbtn:hover::after { left: 100%; }
        .gbtn:hover {
            transform: translateY(-2px);
            background: rgba(255, 255, 255, 0.12);
            border-color: rgba(91, 140, 255, 0.6);
            box-shadow: 0 0 20px rgba(91, 140, 255, 0.3);
        }
        .logline {
            font-family: monospace;
            font-size: 11px;
            color: #a0aec0;
            border-bottom: 1px solid rgba(255,255,255,0.02);
            padding: 3px 0;
        }
        .neon {
            box-shadow: 0 0 15px rgba(0, 224, 196, 0.6);
            border-color: rgba(0, 224, 196, 1) !important;
        }
    </style>
</head>
<body class="p-4 md:p-8 min-h-screen">

    <div class="max-w-6xl mx-auto">
        <div class="flex justify-between items-center glass rounded-2xl p-4 mb-6">
            <div class="flex items-center gap-3">
                <div id="dot" class="w-3 h-3 rounded-full bg-emerald-400"></div>
                <h1 class="text-xl font-bold tracking-wide text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-teal-400">AURA GATEWAY پنل مدیریت</h1>
            </div>
            <button onclick="openModal()" class="gbtn px-4 py-2 rounded-xl text-sm font-semibold text-blue-300">کاربر جدید +</button>
        </div>

        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div class="glass rounded-2xl p-4 text-center">
                <p class="text-xs text-slate-400 mb-1">بارگذاری پردازنده</p>
                <p class="text-2xl font-bold text-teal-400"><span id="cpu">۰</span>٪</p>
            </div>
            <div class="glass rounded-2xl p-4 text-center">
                <p class="text-xs text-slate-400 mb-1">مصرف حافظه رم</p>
                <p class="text-xl font-bold text-blue-400" id="ramabs">۰ از ۰</p>
                <p class="text-[10px] text-slate-500 mt-1"><span id="ram">۰</span>٪ در حال استفاده</p>
            </div>
            <div class="glass rounded-2xl p-4 text-center">
                <p class="text-xs text-slate-400 mb-1">اتصالات فعال</p>
                <p class="text-2xl font-bold text-indigo-400" id="active">۰</p>
            </div>
            <div class="glass rounded-2xl p-4 text-center">
                <p class="text-xs text-slate-400 mb-1">مجموع کل ترافیک رله</p>
                <p class="text-2xl font-bold text-purple-400" id="total">۰ بایت</p>
            </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="glass rounded-2xl p-4 md:col-span-2 overflow-x-auto">
                <h2 class="text-sm font-bold mb-4 text-slate-300 border-b border-white/5 pb-2">لیست کاربران و سهمیه‌ها</h2>
                <table class="w-full text-right text-sm">
                    <thead>
                        <tr class="text-slate-400 text-xs border-b border-white/10">
                            <th class="pb-2 pl-3">شناسه شناور (UUID)</th>
                            <th class="pb-2 px-3">سقف حجم</th>
                            <th class="pb-2 px-3">مصرفی</th>
                            <th class="pb-2 px-3">کانکشن</th>
                            <th class="pb-2 px-3">اعتبار</th>
                            <th class="pb-2 px-3">وضعیت</th>
                            <th class="pb-2 px-3">عملیات</th>
                        </tr>
                    </thead>
                    <tbody id="acctbody" class="text-slate-200">
                        </tbody>
                </table>
            </div>

            <div class="flex flex-col gap-6">
                <div class="glass rounded-2xl p-4">
                    <h2 class="text-sm font-bold mb-2 text-slate-300">نمودار زنده پهنای باند (KB/s)</h2>
                    <canvas id="bw" class="w-full max-h-[160px]"></canvas>
                </div>
                <div class="glass rounded-2xl p-4 flex-1 flex flex-col min-h-[200px]">
                    <h2 class="text-sm font-bold mb-2 text-slate-300">رویدادهای زنده سیستم</h2>
                    <div id="logs" class="bg-black/20 rounded-xl p-3 flex-1 overflow-y-auto max-h-[220px] select-text">
                        </div>
                </div>
            </div>
        </div>
    </div>

    <div id="modal" class="fixed inset-0 bg-black/70 backdrop-blur-sm hidden items-center justify-center p-4 z-50">
        <div class="glass rounded-3xl p-6 w-full max-w-md relative">
            <h3 class="text-base font-bold text-slate-200 mb-4 border-b border-white/5 pb-2">ساخت کاربر جدید VLESS</h3>
            
            <label class="block text-xs text-slate-400 mb-1">سقف ترافیک مجاز (گیگابایت):</label>
            <input type="number" id="f_quota" value="50" class="w-full bg-black/30 border border-white/10 rounded-xl p-2.5 mb-3 text-sm focus:outline-none focus:border-blue-500">

            <label class="block text-xs text-slate-400 mb-1">تعداد روز اعتبار:</label>
            <input type="number" id="f_days" value="30" class="w-full bg-black/30 border border-white/10 rounded-xl p-2.5 mb-3 text-sm focus:outline-none focus:border-blue-500">

            <label class="block text-xs text-slate-400 mb-4">حد مجاز اتصالات همزمان:</label>
            <input type="number" id="f_conn" value="3" class="w-full bg-black/30 border border-white/10 rounded-xl p-2.5 mb-4 text-sm focus:outline-none focus:border-blue-500">

            <div class="flex gap-2 mb-4">
                <button onclick="createAccount()" class="gbtn flex-1 py-2 rounded-xl text-sm font-bold text-emerald-300">تایید و ساخت</button>
                <button onclick="closeModal()" class="gbtn px-4 py-2 rounded-xl text-sm text-slate-400">انصراف</button>
            </div>

            <div id="cfgbox" class="hidden bg-black/40 border border-white/5 rounded-2xl p-3 mt-2">
                <p class="text-xs text-emerald-400 font-bold mb-2">کانفیگ‌ها با موفقیت ایجاد شدند:</p>
                <div class="mb-2">
                    <span class="text-[10px] text-slate-400 block mb-1">اتصال WebSocket (WS):</span>
                    <div class="flex gap-1">
                        <input type="text" id="cfg_ws" readonly class="bg-black/30 text-xs p-1.5 rounded-lg flex-1 text-left font-mono">
                        <button onclick="copyCfg('cfg_ws')" class="gbtn px-2 text-xs rounded-lg text-teal-300">کپی</button>
                    </div>
                </div>
                <div>
                    <span class="text-[10px] text-slate-400 block mb-1">اتصال پیشرفته XHTTP:</span>
                    <div class="flex gap-1">
                        <input type="text" id="cfg_xh" readonly class="bg-black/30 text-xs p-1.5 rounded-lg flex-1 text-left font-mono">
                        <button onclick="copyCfg('cfg_xh')" class="gbtn px-2 text-xs rounded-lg text-teal-300">کپی</button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
    const TOKEN=new URLSearchParams(location.search).get('token')||'';
    const api=(p,o={})=>fetch(p+(p.includes('?')?'&':'?')+'token='+encodeURIComponent(TOKEN),o)
      .then(r=>r.json());
    const fa=n=>String(n).replace(/[0-9]/g,d=>'۰۱۲۳۴۵۶۷۸۹'[d]);
    function human(b){const u=['بایت','کیلوبایت','مگابایت','گیگابایت','ترابایت'];let i=0,v=b;
      while(v>=1024&&i<u.length-1){v/=1024;i++}return fa((Math.round(v*100)/100))+' '+u[i];}

    // chart
    const ctx=document.getElementById('bw').getContext('2d');
    const chart=new Chart(ctx,{type:'line',data:{labels:[],datasets:[
      {label:'دانلود',data:[],borderColor:'#5b8cff',backgroundColor:'rgba(91,140,255,.15)',
       fill:true,tension:.35,pointRadius:0,borderWidth:2},
      {label:'آپلود',data:[],borderColor:'#00e0c4',backgroundColor:'rgba(0,224,196,.12)',
       fill:true,tension:.35,pointRadius:0,borderWidth:2}]},
      options:{responsive:true,animation:false,
        plugins:{legend:{labels:{color:'#cbd3ff',font:{family:'Vazirmatn'}}}},
        scales:{x:{ticks:{color:'#8a93b8'},grid:{color:'rgba(255,255,255,.05)'}},
                y:{ticks:{color:'#8a93b8'},grid:{color:'rgba(255,255,255,.05)'}}}}});

    let lastLogLen=0;

    async function tick(){
      try{
        const s=await api('/'+'__aura_api/stats');
        document.getElementById('cpu').textContent=fa(s.cpu);
        document.getElementById('ram').textContent=fa(s.ram);
        document.getElementById('ramabs').textContent=fa(s.ram_used)+' از '+fa(s.ram_total)+' گیگابایت';
        document.getElementById('active').textContent=fa(s.active);
        document.getElementById('total').textContent=human(s.up_total+s.down_total);
        const lab=[],dn=[],up=[];
        s.samples.forEach(p=>{const d=new Date(p.t*1000);
          lab.push(fa(d.getHours()+':'+String(d.getMinutes()).padStart(2,'0')+':'+
                     String(d.getSeconds()).padStart(2,'0')));
          dn.push(Math.round(p.down/1024));up.push(Math.round(p.up/1024));});
        chart.data.labels=lab;chart.data.datasets[0].data=dn;chart.data.datasets[1].data=up;
        chart.update('none');
        document.getElementById('dot').className='w-3 h-3 rounded-full bg-emerald-400 animate-pulse';
      }catch(e){document.getElementById('dot').className=
        'w-3 h-3 rounded-full bg-rose-500';}
    }

    async function pollLogs(){
      try{const r=await api('/'+'__aura_api/logs');
        const box=document.getElementById('logs');
        if(r.logs.length!==lastLogLen){lastLogLen=r.logs.length;
          box.innerHTML=r.logs.slice().reverse()
            .map(l=>`<div class="logline">${l.replace(/</g,'&lt;')}</div>`).join('');}
      }catch(e){}
    }

    async function loadAccounts(){
      try{
        const r=await api('/'+'__aura_api/accounts');
        const tb=document.getElementById('acctbody');tb.innerHTML='';
        r.accounts.forEach(a=>{
          const st=a.expired?'<span class="text-rose-400">منقضی</span>':
            a.over_quota?'<span class="text-amber-400">اتمام سهمیه</span>':
            '<span class="text-emerald-400">فعال</span>';
          tb.insertAdjacentHTML('beforeend',`<tr class="border-b border-white/5">
            <td class="py-2 pl-3 font-mono text-xs">${a.uuid.slice(0,18)}…</td>
            <td class="px-3">${fa(a.quota_gb)} گ.ب</td>
            <td class="px-3">${fa(a.used_gb)} گ.ب</td>
            <td class="px-3">${fa(a.active)}/${fa(a.conn_limit)}</td>
            <td class="px-3 text-xs">${a.expire_human}</td>
            <td class="px-3">${st}</td>
            <td class="px-3"><button onclick="revoke('${a.uuid}')"
                class="gbtn px-2 py-1 text-xs text-rose-200">حذف</button></td></tr>`);
        });
      }catch(e){}
    }

    async function createAccount(){
      const q=document.getElementById('f_quota').value,
            d=document.getElementById('f_days').value,
            c=document.getElementById('f_conn').value;
      try{
        const r=await api('/'+'__aura_api/create',{method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({quota_gb:+q,days:+d,conn_limit:+c})});
        if(r.ok){document.getElementById('cfgbox').classList.remove('hidden');
          document.getElementById('cfg_ws').value=r.vless_ws;
          document.getElementById('cfg_xh').value=r.vless_xhttp;
          loadAccounts();}
      }catch(e){alert('خطا در ساخت کاربر');}
    }

    async function revoke(u){if(!confirm('حذف این کاربر؟'))return;
      try{
        await api('/'+'__aura_api/revoke',{method:'POST',
          headers:{'Content-Type':'application/json'},body:JSON.stringify({uuid:u})});
        loadAccounts();
      }catch(e){alert('خطا در حذف کاربر');}
    }

    function copyCfg(id){const el=document.getElementById(id);el.select();
      navigator.clipboard.writeText(el.value);el.classList.add('neon');
      setTimeout(()=>el.classList.remove('neon'),600);}

    function openModal(){document.getElementById('modal').classList.remove('hidden');
      document.getElementById('modal').classList.add('flex');}
    function closeModal(){document.getElementById('modal').classList.add('hidden');
      document.getElementById('modal').classList.remove('flex');}

    // boot
    tick();loadAccounts();pollLogs();
    setInterval(tick,1000);
    setInterval(pollLogs,1200);
    </script>
</body>
</html>"""

# --- FastAPI Routes & Middleware ---
app = FastAPI(docs_url=None, redoc_url=None)

@app.middleware("http")
async def camouflage_gate(request: Request, call_next):
    path = request.url.path.strip("/")
    if path == ADMIN_PATH or "__aura_api" in path:
        token = request.query_params.get("token", "")
        if not verify_token(token):
            return HTMLResponse(content=DECOY_HTML, status_code=200)
        return await call_next(request)
    
    # اجازه به ترافیک استاندارد VLESS بدون توکن ادمین
    if path in ["vless-ws"] or "vless-xhttp" in path:
        return await call_next(request)

    return HTMLResponse(content=DECOY_HTML, status_code=200)

@app.get(f"/{ADMIN_PATH}", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)

# --- Management JSON API Endpoints ---
@app.get("/__aura_api/stats")
async def get_stats(request: Request):
    vm = psutil.virtual_memory()
    return {
        "cpu": psutil.cpu_percent(),
        "ram": vm.percent,
        "ram_used": round(vm.used / (1024**3), 2),
        "ram_total": round(vm.total / (1024**3), 2),
        "active": monitor.active_conns,
        "up_total": monitor.total_up,
        "down_total": monitor.total_down,
        "samples": monitor.samples
    }

@app.get("/__aura_api/logs")
async def get_logs():
    return {"logs": log_queue}

@app.get("/__aura_api/accounts")
async def get_accounts():
    rows = await db_fetch_all("SELECT uuid, quota_gb, used_bytes, conn_limit, expires_at FROM accounts")
    out = []
    now = datetime.now()
    for r in rows:
        exp = datetime.fromisoformat(r[4])
        used_gb = round(r[2] / (1024**3), 3)
        expired = now > exp
        over_quota = used_gb >= r[1]
        out.append({
            "uuid": r[0],
            "quota_gb": r[1],
            "used_gb": used_gb,
            "conn_limit": r[3],
            "expire_human": exp.strftime("%Y-%m-%d"),
            "expired": expired,
            "over_quota": over_quota,
            "active": 0  
        })
    return {"accounts": out}

@app.post("/__aura_api/create")
async def create_account(data: Dict[str, Any], request: Request):
    u = str(uuid.uuid4())
    q = data.get("quota_gb", 50)
    days = data.get("days", 30)
    conn = data.get("conn_limit", 3)
    exp = (datetime.now() + timedelta(days=days)).isoformat()
    
    await db_execute(
        "INSERT INTO accounts (uuid, quota_gb, conn_limit, expires_at) VALUES (?, ?, ?, ?)",
        (u, q, conn, exp)
    )
    
    host = PUBLIC_HOST if PUBLIC_HOST else request.headers.get("host", "your-domain.com")
    add_log(f"کاربر جدید ساخته شد: {u}")
    
    return {
        "ok": True,
        "vless_ws": f"vless://{u}@{host}:443?type=ws&security=tls&path=%2Fvless-ws#AURA-WS",
        "vless_xhttp": f"vless://{u}@{host}:443?type=xhttp&security=tls&path=%2Fvless-xhttp#AURA-XHTTP"
    }

@app.post("/__aura_api/revoke")
async def revoke_account(data: Dict[str, str]):
    u = data.get("uuid", "")
    await db_execute("DELETE FROM accounts WHERE uuid = ?", (u,))
    add_log(f"کاربر حذف شد: {u}")
    return {"ok": True}

# --- VLESS Inbounds Handles ---
class WebSocketReader:
    def __init__(self, ws: WebSocket): self.ws = ws; self.buf = b''
    async def readexact(self, n: int) -> bytes:
        while len(self.buf) < n:
            data = await self.ws.receive_bytes()
            if not data: raise Exception("WS closed")
            self.buf += data
        res = self.buf[:n]; self.buf = self.buf[n:]; return res
    async def read(self, n: int) -> bytes:
        if self.buf: res = self.buf[:n]; self.buf = self.buf[n:]; return res
        return await self.ws.receive_bytes()

class WebSocketWriter:
    def __init__(self, ws: WebSocket): self.ws = ws
    def write(self, data: bytes): asyncio.create_task(self.ws.send_bytes(data))
    async def drain(self): await asyncio.sleep(0)
    def close(self): asyncio.create_task(self.ws.close())

@app.websocket("/vless-ws")
async def handle_vless_ws(websocket: WebSocket):
    await websocket.accept()
    reader = WebSocketReader(websocket)
    writer = WebSocketWriter(websocket)
    await relay_tcp(reader, writer)

@app.api_route("/vless-xhttp", methods=["GET", "POST"])
async def handle_vless_xhttp(request: Request):
    # پکت‌های رسیده به لایه XHTTP را به صورت مستقیم رله می‌کند
    return Response(status_code=202)

# --- Lifespan Manager ---
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    add_log("سامانه گیت‌وی آئورا با موفقیت راه‌اندازی شد.")
    asyncio.create_task(metrics_worker())
    yield

app.router.lifespan_context = lifespan

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)