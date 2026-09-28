"""台風データベース（1ファイル版）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT
"""
import csv, io, os, sqlite3, sys
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

URL = ("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
       "v04r01/access/csv/ibtracs.WP.list.v04r01.csv")
SRC = os.environ.get("SOURCE_CSV")  # ローカルテスト用
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "typhoon.db")

# ---------------- DB作成 ----------------
def num(v):
    try:
        f = float(v)
        return f if f > -900 else None
    except (TypeError, ValueError):
        return None

def load_rows():
    if SRC:
        text = open(SRC, encoding="utf-8").read()
    else:
        print("Downloading", URL, flush=True)
        r = requests.get(URL, timeout=300); r.raise_for_status()
        text = r.text
    rd = csv.DictReader(io.StringIO(text))
    next(rd)  # 2行目は単位行なので読み飛ばす
    return rd

def build():
    if os.path.exists(DB):
        os.remove(DB)
    con = sqlite3.connect(DB)
    con.executescript("""
    CREATE TABLE typhoons(sid TEXT PRIMARY KEY, year INT, number INT, name TEXT,
        start_time TEXT, end_time TEXT, max_wind REAL, min_pres REAL);
    CREATE TABLE points(sid TEXT, time TEXT, lat REAL, lon REAL, wind REAL, pres REAL);
    CREATE INDEX idx_points_sid ON points(sid);
    CREATE INDEX idx_ty_year ON typhoons(year);
    """)
    pts, meta = {}, {}
    for r in load_rows():
        lat, lon = num(r["LAT"]), num(r["LON"])
        if lat is None or lon is None:
            continue
        wind = num(r["WMO_WIND"]) or num(r["USA_WIND"])
        pres = num(r["WMO_PRES"]) or num(r["USA_PRES"])
        sid = r["SID"]
        name = r["NAME"].strip()
        meta.setdefault(sid, dict(year=int(r["SEASON"]), number=int(r["NUMBER"] or 0),
                                  name="" if name == "NOT_NAMED" else name))
        pts.setdefault(sid, []).append((sid, r["ISO_TIME"], lat, lon, wind, pres))
    n = 0
    for sid, p in pts.items():
        winds = [x[4] for x in p if x[4]]
        press = [x[5] for x in p if x[5]]
        mw = max(winds) if winds else None
        if mw is None or mw < 34:  # 熱帯低気圧のまま終わったものは除外
            continue
        m = meta[sid]
        con.execute("INSERT INTO typhoons VALUES(?,?,?,?,?,?,?,?)",
                    (sid, m["year"], m["number"], m["name"], p[0][1], p[-1][1], mw,
                     min(press) if press else None))
        con.executemany("INSERT INTO points VALUES(?,?,?,?,?,?)", p)
        n += 1
    con.commit(); con.close()
    print(f"Done: {n} typhoons")

# ---------------- API ----------------
app = FastAPI(title="Typhoon DB")

def q(sql, args=()):
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()

@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE

@app.get("/api/years")
def years():
    return q("SELECT MIN(year) AS min, MAX(year) AS max FROM typhoons")[0]

@app.get("/api/typhoons")
def typhoons(year: int | None = None, name: str | None = None,
             min_wind: float | None = None, limit: int = Query(300, le=1000)):
    where, args = [], []
    if year: where.append("year=?"); args.append(year)
    if name: where.append("name LIKE ?"); args.append(f"%{name.upper()}%")
    if min_wind: where.append("max_wind>=?"); args.append(min_wind)
    w = ("WHERE " + " AND ".join(where)) if where else ""
    return q(f"SELECT * FROM typhoons {w} ORDER BY start_time DESC LIMIT ?", (*args, limit))

@app.get("/api/typhoons/{sid}")
def detail(sid: str):
    t = q("SELECT * FROM typhoons WHERE sid=?", (sid,))
    if not t: raise HTTPException(404, "台風が見つかりません")
    return {**t[0], "track": q(
        "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time", (sid,))}

# ---------------- 画面 ----------------
PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>台風データベース</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link href="https://fonts.googleapis.com/css2?family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0f1b2b;--panel:#16283d;--line:#2a4160;--ink:#e8eef5;--sub:#8fa5bf;--acc:#f2c14e}
*{box-sizing:border-box}
body{margin:0;height:100vh;display:grid;grid-template-columns:340px 1fr;background:var(--bg);color:var(--ink);font-family:"Zen Kaku Gothic New",sans-serif}
aside{display:flex;flex-direction:column;min-height:0;border-right:1px solid var(--line)}
header{padding:18px 18px 12px}
h1{margin:0 0 12px;font-size:22px;font-weight:900;letter-spacing:.04em}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input,.f select{width:100%;padding:8px;background:var(--panel);border:1px solid var(--line);color:var(--ink);border-radius:4px;font:inherit}
.f input[name=name]{grid-column:span 2}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}
#count{padding:0 18px 8px;color:var(--sub);font-size:13px}
#list{flex:1;overflow:auto;margin:0;padding:0;list-style:none}
#list li{padding:10px 18px;border-top:1px solid var(--line);cursor:pointer;display:flex;gap:10px;align-items:center}
#list li:hover,#list li.on{background:var(--panel)}
.sw{width:10px;height:34px;border-radius:2px;flex:none}
.t b{display:block}.t span{color:var(--sub);font-size:12px}
main{position:relative;min-height:0}#map{height:100%;background:#0b1522}
#info{position:absolute;z-index:500;right:14px;top:14px;background:rgba(15,27,43,.92);border:1px solid var(--line);padding:12px 14px;border-radius:6px;font-size:13px;min-width:200px}
#info h2{margin:0 0 4px;font-size:18px}
#legend{position:absolute;z-index:500;left:14px;bottom:24px;background:rgba(15,27,43,.92);border:1px solid var(--line);padding:8px 12px;border-radius:6px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;display:inline-block}
@media(max-width:760px){body{grid-template-columns:1fr;grid-template-rows:45vh 1fr}aside{order:2;border:0}main{order:1}}
</style></head><body>
<aside>
 <header><h1>台風データベース</h1>
  <form class="f" id="f" onsubmit="return false">
   <input name="name" placeholder="名前で検索（例: JEBI）">
   <select name="year"><option value="">全ての年</option></select>
   <select name="min_wind"><option value="">全ての強さ</option>
     <option value="64">台風（64kt以上）</option><option value="85">非常に強い（85kt以上）</option><option value="105">猛烈な（105kt以上）</option></select>
  </form></header>
 <div id="count"></div><ul id="list"></ul>
</aside>
<main><div id="map"></div><div id="info" hidden></div>
 <div id="legend"></div></main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const CLS=[[105,"猛烈な","#e0245e"],[85,"非常に強い","#f0803c"],[64,"強い(台風)","#f2c14e"],[48,"暴風域(TS強)","#7fd1ae"],[34,"熱帯低気圧〜TS","#6fa8dc"],[0,"不明/弱い","#5b6f88"]];
const col=w=>(CLS.find(c=>(w||0)>=c[0])||CLS[5])[2];
const cls=w=>(CLS.find(c=>(w||0)>=c[0])||CLS[5])[1];
const $=s=>document.querySelector(s), f=$("#f");
const map=L.map("map",{worldCopyJump:true}).setView([25,135],4);
L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",{attribution:"© OpenStreetMap © CARTO | IBTrACS (NOAA)",maxZoom:8}).addTo(map);
let layer=L.layerGroup().addTo(map);
$("#legend").innerHTML=CLS.slice(0,5).map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join("");

async function init(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--) f.year.add(new Option(i+"年",i));
  f.addEventListener("input",()=>{clearTimeout(init.t);init.t=setTimeout(load,250)});
  load();
}
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year","min_wind"]) if(f[k].value) p.set(k,f[k].value);
  const rows=await (await fetch("/api/typhoons?"+p)).json();
  $("#count").textContent=rows.length?`${rows.length}件`+(rows.length>=300?"（上限。条件を絞ってください）":""):"該当なし。条件を変えてください";
  $("#list").innerHTML=rows.map(r=>`<li data-sid="${r.sid}"><span class="sw" style="background:${col(r.max_wind)}"></span>
   <div class="t"><b>${r.year}年 ${r.name||"名称なし"}</b><span>${r.start_time.slice(0,10)} ／ 最大 ${r.max_wind??"-"}kt</span></div></li>`).join("");
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li)show(li.dataset.sid,li)});
async function show(sid,li){
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));li.classList.add("on");
  const t=await (await fetch("/api/typhoons/"+sid)).json();
  layer.clearLayers();
  const pts=t.track;
  // 日付変更線をまたぐ場合に備えて経度を連続化
  let prev=null;const ll=pts.map(p=>{let lo=p.lon;if(prev!==null)if(prev!==null){while(lo-prev>180)lo-=360;while(lo-prev<-180)lo+=360}prev=lo;return [p.lat,lo]});
  for(let i=1;i<pts.length;i++) L.polyline([ll[i-1],ll[i]],{color:col(pts[i].wind),weight:4}).addTo(layer);
  pts.forEach((p,i)=>L.circleMarker(ll[i],{radius:3,color:"#fff",weight:1,fillColor:col(p.wind),fillOpacity:1})
    .bindTooltip(`${p.time}<br>${p.wind??"-"}kt / ${p.pres??"-"}hPa`).addTo(layer));
  map.fitBounds(L.latLngBounds(ll).pad(.3));
  const i=$("#info");i.hidden=false;
  i.innerHTML=`<h2>${t.name||"名称なし"}</h2>${t.year}年 第${t.number}号相当<br>期間: ${t.start_time.slice(0,10)} 〜 ${t.end_time.slice(0,10)}<br>最大風速: ${t.max_wind}kt（${cls(t.max_wind)}）<br>最低気圧: ${t.min_pres??"-"}hPa`;
}
init();
</script></body></html>
"""

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build()
    else:
        import uvicorn
        if not os.path.exists(DB):
            build()
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
