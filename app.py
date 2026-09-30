"""台風データベース（1ファイル版・気象庁ベストトラック準拠）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT   (DBが無ければ起動時に自動ビルド)
表記: 「2026年台風第26号 Surigae」。号数は気象庁方式（熱帯低気圧の段階は数えず、
      台風の強さに初めて達した順に年ごとに採番）で再計算する。
風速はm/s表示（DB内部はkt保持、API出力時に換算）。
今年ぶんは 位置表PDF（確定・速報）＋防災情報JSON（発生中の台風）＋デジタル台風（NII、PDF未掲載の消滅済み台風の補完）で随時更新する。
データ出典: 気象庁 / IBTrACS / デジタル台風（国立情報学研究所・北本朗）
"""
import asyncio, csv, io, os, re, sqlite3, sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.gzip import GZipMiddleware
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

URL = ("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
       "v04r01/access/csv/ibtracs.WP.list.v04r01.csv")
SRC = os.environ.get("SOURCE_CSV")  # ローカルテスト用
JMA = "https://www.data.jma.go.jp/typhoon"
F = "%Y-%m-%d %H:%M:%S"
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "typhoon.db")
BOSAI = "https://www.jma.go.jp/bosai/typhoon/data"  # 発生中の台風（防災情報JSON）
UA = {"User-Agent": "typhoon-db/1.0"}
DT = "https://agora.ex.nii.ac.jp/digital-typhoon"  # デジタル台風（気象庁の速報値を号数つきで公開。PDF掲載前の補完用）
INTERVAL = int(os.environ.get("REFRESH_MINUTES", "60")) * 60  # 速報の再取得間隔（秒）
KT2MS, MS2KT = 0.514444, 1.94384

def to_ms(kt):
    """DBはkt保持。表示用にm/s（整数）へ換算する。m/s→kt→m/sは常に元の整数に戻る。"""
    return None if kt is None else int(kt * KT2MS + 0.5)

# 気象庁が正式に名称を付けた台風（年, 国際名）
NICK = {(1954, "MARIE"): "洞爺丸台風", (1958, "IDA"): "狩野川台風", (1959, "SARAH"): "宮古島台風",
        (1959, "VERA"): "伊勢湾台風", (1961, "NANCY"): "第2室戸台風", (1966, "CORA"): "第2宮古島台風",
        (2019, "FAXAI"): "令和元年房総半島台風", (2019, "HAGIBIS"): "令和元年東日本台風"}

def title(year, no, en):
    s = f"{year}年台風第{no}号"
    if en: s += f" {en.title()}"
    if (year, en) in NICK: s += f"（{NICK[(year, en)]}）"
    return s

# ---------------- DB作成 ----------------
def num(v):
    try:
        f = float(v)
        return f if f > -900 else None
    except (TypeError, ValueError):
        return None

def jst(iso):
    return datetime.strptime(iso[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9)

NEED = ("SID", "NAME", "ISO_TIME", "TRACK_TYPE", "TOKYO_LAT", "TOKYO_LON",
        "TOKYO_GRADE", "TOKYO_WIND", "TOKYO_PRES")

def load_rows():
    """CSVを流し読みし、必要な列だけの辞書を返す（列番号で読むのでDictReaderより高速）。"""
    def lines():
        if SRC:
            with open(SRC, encoding="utf-8") as f:
                yield from (ln.rstrip("\n") for ln in f)
        else:
            print("Downloading", URL, flush=True)
            with requests.get(URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                r.encoding = "utf-8"
                yield from r.iter_lines(chunk_size=1 << 20, decode_unicode=True)
    it = lines()
    head = next(csv.reader([next(it)]))
    next(it)  # 2行目は単位行
    idx = [(k, head.index(k)) for k in NEED if k in head]
    last = max(i for _, i in idx)
    # SIDは先頭が西暦。気象庁の号数は1951年からなので、それ以前は読み飛ばす
    for row in csv.reader(ln for ln in it if ln[:4] >= "1951"):
        if len(row) > last:
            yield {k: row[i] for k, i in idx}

def insert(con, sid, year, no, en, p, prov=0):
    """p: [(time_utc, lat, lon, wind_kt, pres_hPa), ...] を1台風ぶんDBへ書き込む。"""
    ws = [x[3] for x in p if x[3]]; ps = [x[4] for x in p if x[4]]
    t0, t1 = (datetime.strptime(p[i][0][:19], F) for i in (0, -1))
    con.execute("INSERT OR REPLACE INTO typhoons VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, year, no, title(year, no, en), en, p[0][0], p[-1][0], max(ws) if ws else None,
                 min(ps) if ps else None, (t0 + timedelta(hours=9)).month,
                 round((t1 - t0).total_seconds() / 86400, 1), prov))
    con.executemany("INSERT INTO points VALUES(?,?,?,?,?,?)", [(sid, *x[:5]) for x in p])

def build():
    tmp = DB + ".tmp"  # 失敗しても既存DBを壊さないよう、一時ファイルに作って最後に置換
    if os.path.exists(tmp):
        os.remove(tmp)
    con = sqlite3.connect(tmp)
    con.executescript("""
    CREATE TABLE typhoons(sid TEXT PRIMARY KEY, year INT, number INT, title TEXT, name_en TEXT,
        start_time TEXT, end_time TEXT, max_wind REAL, min_pres REAL, month INT, days REAL, prov INT);
    CREATE TABLE points(sid TEXT, time TEXT, lat REAL, lon REAL, wind REAL, pres REAL);
    CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
    """)
    con.executescript("PRAGMA synchronous=OFF; PRAGMA journal_mode=OFF;")  # ビルド専用の一時DBなので高速化
    metas = []

    def flush(m, p):
        # 気象庁の階級が「台風の強さ(TS以上)」に一度でも達したものだけ採用（気象庁の台風の定義）
        ts = [x[5] for x in p]
        if any(ts):
            metas.append(dict(m, t0=jst(p[ts.index(True)][0]), p=[x[:5] for x in p]))

    cur, meta, pts = None, None, []  # IBTrACSはSIDごとに連続して並んでいる
    for r in load_rows():
        if r.get("TRACK_TYPE") == "spur-track":
            continue
        lat, lon = num(r.get("TOKYO_LAT")), num(r.get("TOKYO_LON"))  # 気象庁の観測点のみ（補間点を除外）
        if lat is None or lon is None:
            continue
        sid = r["SID"]
        if sid != cur:
            if cur is not None:
                flush(meta, pts)
            name = r["NAME"].strip().upper()
            meta = dict(sid=sid, en="" if name in ("NOT_NAMED", "UNNAMED") else name)
            cur, pts = sid, []
        wind, pres = num(r.get("TOKYO_WIND")), num(r.get("TOKYO_PRES"))
        is_ts = (num(r.get("TOKYO_GRADE")) in (3, 4, 5, 7)) or (wind is not None and wind >= 34)
        pts.append((r["ISO_TIME"], lat, lon, wind or None, pres or None, is_ts))
    if cur is not None:
        flush(meta, pts)

    # 号数: TSの強さに初めて達した日時(JST)の順に、年ごとに1から採番
    metas.sort(key=lambda m: m["t0"])
    seq = {}
    for m in metas:
        y = m["t0"].year
        seq[y] = seq.get(y, 0) + 1
        insert(con, m["sid"], y, seq[y], m["en"], m["p"])
    con.executescript("CREATE INDEX idx_points_sid ON points(sid); CREATE INDEX idx_ty_year ON typhoons(year, number);")
    con.execute("INSERT INTO meta VALUES('built_at',?)", (datetime.now().strftime("%Y-%m-%d"),))
    con.commit(); con.close()
    os.replace(tmp, DB)
    print(f"Done: {len(metas)} typhoons（今年の速報値は起動後に自動で取り込みます）")

# ---------------- 気象庁の速報値（今年ぶん） ----------------
HEAD = re.compile(r"(\d{4})年台風第\s*(\d+)号\s+([A-Za-z][A-Za-z\-]*)")
ROW = re.compile(r"^(?:(\d{1,2})\s+)?(?:(\d{1,2})\s+)?(\d{1,2})\s+(\d+\.\d)(?:\s+N)?\s+(\d+\.\d)(?:\s+E)?\s+(\d{3,4}|--)\s+(\d{1,2}|--)(?=\s|$)")

def parse_pdf(text):
    """気象庁の台風位置表PDF（日本時・風速m/s）→ (年, 号数, 名前, 点列(UTC・kt), 速報か)"""
    h = HEAD.search(text)
    if not h:
        return None
    year, no, name = int(h[1]), int(h[2]), h[3].upper()
    y0 = year  # 号数の年（年またぎでも変えない）
    mo = da = None; pts = []
    for ln in text.splitlines():
        r = ROW.match(ln.strip())
        if not r:
            continue
        ints = [int(x) for x in r.groups()[:3] if x]
        if len(ints) == 3:
            if mo and ints[0] < mo: year += 1
            mo, da, hr = ints
        elif len(ints) == 2:
            da, hr = ints
        else:
            hr = ints[0]
        if mo is None or da is None:
            continue
        t = datetime(year, mo, da) + timedelta(hours=hr - 9)  # JST→UTC
        pres = None if r[6] == "--" else float(r[6])
        wind = None if r[7] == "--" else round(int(r[7]) * MS2KT)  # m/s→kt
        pts.append((t.strftime(F), float(r[4]), float(r[5]), wind, pres))
    return (y0, no, name, pts, "速報値" in text) if pts else None

def fetch_pdf(c, since=None):
    """位置表PDFを取得して解析。戻り値: (コード, 解析結果 or None, Last-Modified)。
    未掲載(404)・変更なし(304)・解析失敗は None（1件の失敗で全体を止めない）。"""
    try:
        import pdfplumber
        h = dict(UA, **({"If-Modified-Since": since} if since else {}))
        r = requests.get(f"{JMA}/data/T{c}.pdf", headers=h, timeout=60)
        if r.status_code != 200 or not r.content.startswith(b"%PDF"):
            if r.status_code not in (200, 304, 404):
                print(f"T{c}.pdf: HTTP {r.status_code}", flush=True)
            return c, None, None
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            text = "\n".join(pg.extract_text() or "" for pg in pdf.pages)
        res = parse_pdf(text)
        if not res:
            print(f"T{c}.pdf: parse failed", flush=True)
        return c, res, r.headers.get("Last-Modified")
    except Exception as e:
        print(f"T{c}.pdf failed:", repr(e), flush=True)
        return c, None, None

def refresh_pdf(con, yr):
    """位置表PDFから取り込む。確定済みは再取得せず、速報はLast-Modifiedが変わった時だけ取り直す。
    一覧ページの掲載は遅れる（数号ぶん）ので、掲載済みの次の号のPDFも探す。"""
    yy = str(yr)[2:]
    html = requests.get(f"{JMA}/position_table/table{yr}.html", headers=UA, timeout=60).text
    codes = sorted({c for c in re.findall(r"T(\d{4})\.pdf", html) if c[:2] == yy})
    top = int(codes[-1][2:]) if codes else 0
    codes += [f"{yy}{n:02d}" for n in range(top + 1, top + 6)]  # 一覧の掲載は遅れるので先の号も探す(404は無視)
    done = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%' AND prov=0")}
    have = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%'")}
    lm = dict(con.execute("SELECT k, v FROM meta WHERE k LIKE 'lm:%'"))
    todo = [c for c in codes if f"JMA{c}" not in done]
    since = lambda c: lm.get(f"lm:{c}") if f"JMA{c}" in have else None
    with ThreadPoolExecutor(4) as ex:
        got = [g for g in ex.map(lambda c: fetch_pdf(c, since(c)), todo) if g[1]]
    for c, (y, no, en, pts, prov), mod in got:
        con.execute("DELETE FROM points WHERE sid=?", (f"JMA{c}",))
        insert(con, f"JMA{c}", y, no, en, pts, int(prov))
        if mod:
            con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"lm:{c}", mod))
        con.execute("DELETE FROM meta WHERE k=?", (f"dt:JMA{c}",))  # 位置表PDFが載ったのでDT補完は不要
    print(f"JMA {yr}: PDF {len(got)}/{len(todo)} updated", flush=True)

TRACK_STEP_H = float(os.environ.get("TRACK_STEP_H", "3"))  # forecast.json の過去軌跡の点間隔（時間）。時刻は推定値

def get_json(url, default=None):
    try:
        r = requests.get(url, headers=UA, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"GET {url} failed:", repr(e), flush=True)
        return default

def _title_en(arr):
    t = next((r for r in arr if isinstance(r, dict) and r.get("part") == "title"), {})
    return ((t.get("name") or {}).get("en") or "").strip().upper()

def past_track(track):
    """forecast.json の実況 track（preTyphoon＋typhoon）→ [(lat, lon)]（古い順・連続重複除去）"""
    out = []
    if isinstance(track, dict):
        for k in ("preTyphoon", "typhoon"):
            for p in track.get(k) or []:
                try:
                    la, lo = float(p[0]), float(p[1])
                except (TypeError, ValueError, IndexError, KeyError):
                    continue
                if abs(la) <= 90 and abs(lo) <= 360 and (not out or out[-1] != (la, lo)):
                    out.append((la, lo))
    return out

def live_point(spec, fc=()):
    """発生中の台風 → (英語名, 実況点(時刻UTC, 緯度, 経度, 風速kt, 気圧), 過去軌跡[(lat, lon)])。実況が無ければ None。
    実況は specifications.json（風速・気圧つき）を優先し、無ければ forecast.json の実況(advancedHours=0)を使う。
    過去軌跡は forecast.json の実況に付いている track（時刻は持たない）。"""
    en = _title_en(spec) or _title_en(fc)
    pt, past = None, []
    for r in spec:
        p = r.get("part") if isinstance(r, dict) else None
        if isinstance(p, dict) and p.get("jp") == "実況":
            try:
                lat, lon = r["position"]["deg"][:2]
                w = num(((r.get("maximumWind") or {}).get("sustained") or {}).get("m/s"))
                t = datetime.strptime(r["validtime"]["UTC"][:19], "%Y-%m-%dT%H:%M:%S")
                pt = (t.strftime(F), float(lat), float(lon),
                      round(w * MS2KT) if w else None, num(r.get("pressure")) or None)
            except (KeyError, TypeError, ValueError, IndexError):
                pass
            break
    for r in fc:
        if isinstance(r, dict) and r.get("advancedHours") == 0:
            past = past_track(r.get("track"))
            if pt is None:
                try:
                    t = datetime.strptime(r["validtime"]["UTC"][:19], "%Y-%m-%dT%H:%M:%S")
                    pt = (t.strftime(F), float(r["center"][0]), float(r["center"][1]), None, None)
                except (KeyError, TypeError, ValueError, IndexError):
                    pass
            break
    return (en, pt, past) if pt else None

def merge_live(con, sid, year, no, en, pt, past=()):
    """発生中の台風の実況を取り込む。
    ・位置表PDF由来の点（無ければ同名のIBTrACS由来の点）があれば、それを土台にする
    ・土台より古い側は、forecast.json の過去軌跡で補う（軌跡は時刻を持たないので、実況時刻から
      TRACK_STEP_H 時間刻みで遡った推定時刻。位置表PDFが載れば、そちらの正しい値で置き換わる）
    ・最新の実況1点を末尾に追記する
    戻り値: 更新したか。"""
    sel = "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time"
    old = [tuple(p) for p in con.execute(sel, (sid,)).fetchall()]
    if not old and en:
        alt = con.execute("SELECT sid FROM typhoons WHERE year=? AND name_en=? AND sid NOT LIKE 'JMA%'",
                          (year, en)).fetchone()
        if alt:
            old = [tuple(p) for p in con.execute(sel, (alt[0],)).fetchall()]
    trk = list(past)
    if trk and abs(trk[-1][0] - pt[1]) < 0.05 and abs(trk[-1][1] - pt[2]) < 0.05:
        trk.pop()  # 末尾が実況と同じ点なら重複させない
    t1 = datetime.strptime(pt[0][:19], F)
    back = [((t1 - timedelta(hours=TRACK_STEP_H * (len(trk) - i))).strftime(F), la, lo, None, None)
            for i, (la, lo) in enumerate(trk)]
    first = old[0][0] if old else pt[0]
    new = [b for b in back if b[0] < first] + old
    if not old or old[-1][0] < pt[0]:
        new.append(pt)
    if new == old:
        return False
    con.execute("DELETE FROM points WHERE sid=?", (sid,))
    insert(con, sid, year, no, en, new, 1)
    return True

def refresh_live(con):
    """発生中の台風を気象庁の防災情報JSONから取り込む（位置表PDFの掲載は遅れるため）。
    注意: JSONのIDは TC2632 のように熱帯低気圧を含む通し番号で、台風の号数(typhoonNumber=2626)とは別。
    1つの台風の失敗で他を止めない。"""
    r = requests.get(f"{BOSAI}/targetTc.json", headers=UA, timeout=30)
    r.raise_for_status()
    lst = r.json()
    print("JMA live list:", [(t.get("tropicalCyclone"), t.get("typhoonNumber"), t.get("category")) for t in lst], flush=True)
    n = 0
    for t in lst:
        tn, tc = str(t.get("typhoonNumber", "")), t.get("tropicalCyclone")
        if not tc or not re.fullmatch(r"\d{4}", tn):  # 熱帯低気圧(号数なし)は対象外
            continue
        try:
            spec = get_json(f"{BOSAI}/{tc}/specifications.json", [])
            fc = get_json(f"{BOSAI}/{tc}/forecast.json", [])
            got = live_point(spec, fc)
            if not got:
                print(f"live {tc} (台風{tn}): 実況なし", flush=True)
                continue
            en, pt, past = got
            n += merge_live(con, f"JMA{tn}", 2000 + int(tn[:2]), int(tn[2:]), en, pt, past)
        except Exception as e:
            print(f"live {tc} failed:", repr(e), flush=True)
    print(f"JMA live: {n} updated", flush=True)

def dt_track(no6):
    """デジタル台風のGeoJSON → (英語名, 点列[(時刻UTC, 緯度, 経度, 風速kt, 気圧)])。取れなければ None。
    6桁番号は 西暦4桁+気象庁の号数2桁（例 202625）。点は3時間刻みで、熱帯低気圧の段階から温帯低気圧化まで含む。"""
    j = get_json(f"{DT}/geojson/wnp/{no6}.en.json")
    if not isinstance(j, dict):
        return None
    pts = []
    for f in j.get("features") or []:
        try:
            p, (lon, lat) = f["properties"], f["geometry"]["coordinates"][:2]
            t = datetime.fromtimestamp(int(p["time"]), timezone.utc).strftime(F)
            pts.append((t, float(lat), float(lon), num(p.get("wind")) or None, num(p.get("pressure")) or None))
        except (KeyError, TypeError, ValueError):
            continue
    pts.sort()
    return ((j.get("properties") or {}).get("name") or "").strip().upper(), pts

def refresh_dt(con, yr):
    """位置表PDFが未掲載の台風（消滅直後など）を、デジタル台風から補う。
    対象: 今年の号数のうち ①DBに無いもの ②以前ここから補ったもの（PDFが載るまで追いかける）。
    PDF確定・速報が取れた台風は対象外。PDFが載れば refresh_pdf 側で置き換わる。"""
    yy = str(yr)[2:]
    html = requests.get(f"{DT}/year/wnp/{yr}.html.en", headers=UA, timeout=60).text
    nums = sorted({int(n) for y, n in re.findall(r"/(\d{4})(\d{2})\.html", html) if y == str(yr)})
    fin = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%' AND prov=0")}
    have = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%'")}
    pend = {r[0][3:] for r in con.execute("SELECT k FROM meta WHERE k LIKE 'dt:%'")}
    todo = [n for n in nums if (f"JMA{yy}{n:02d}" not in have or f"JMA{yy}{n:02d}" in pend)
            and f"JMA{yy}{n:02d}" not in fin]
    with ThreadPoolExecutor(3) as ex:
        got = list(ex.map(lambda n: (n, dt_track(f"{yr}{n:02d}")), todo))
    sel = "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time"
    upd = 0
    for n, g in got:
        if not g or len(g[1]) < 2:
            continue
        en, pts = g
        sid = f"JMA{yy}{n:02d}"
        old = [tuple(p) for p in con.execute(sel, (sid,)).fetchall()]
        new = pts + [p for p in old if p[0] > pts[-1][0]]  # 防災情報JSONの実況がDTより新しければ残す
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"dt:{sid}", pts[-1][0]))
        if new != old:
            con.execute("DELETE FROM points WHERE sid=?", (sid,))
            insert(con, sid, yr, n, en, new, 1)
            upd += 1
    print(f"DT {yr}: {len(todo)} target, {upd} updated", flush=True)

def dedupe(con):
    """気象庁データで置き換わった同名のIBTrACS由来データだけを消す（それ以外は残す）。"""
    dup = ("SELECT t.sid FROM typhoons t WHERE t.sid NOT LIKE 'JMA%' AND t.name_en<>'' AND EXISTS "
           "(SELECT 1 FROM typhoons j WHERE j.sid LIKE 'JMA%' AND j.year=t.year AND j.name_en=t.name_en)")
    con.execute(f"DELETE FROM points WHERE sid IN ({dup})")
    con.execute(f"DELETE FROM typhoons WHERE sid IN ({dup})")

def refresh_recent():
    """今年の台風を取り込む。PDFとJSONは別々に失敗しても、取れたぶんで動かす。"""
    con, ok = None, False
    try:
        con = sqlite3.connect(DB, timeout=60)
        now = datetime.now(timezone(timedelta(hours=9)))
        for yr in [now.year] + ([now.year - 1] if now.month == 1 else []):  # 1月は前年の台風も継続しうる
            try:
                refresh_pdf(con, yr); ok = True
            except Exception as e:
                print(f"JMA table {yr} failed:", repr(e), flush=True)
            try:
                refresh_dt(con, yr); ok = True
            except Exception as e:
                print(f"DT {yr} failed:", repr(e), flush=True)
        try:
            refresh_live(con); ok = True
        except Exception as e:
            print("JMA live failed:", repr(e), flush=True)
        dedupe(con)
        if ok:
            con.execute("INSERT OR REPLACE INTO meta VALUES('jma_at',?)", (datetime.now(timezone.utc).strftime(F),))
        con.commit()
    except Exception as e:
        print("refresh_recent failed:", repr(e), flush=True)
    finally:
        if con: con.close()

def diag():
    """python app.py diag : 台風が取れない時の切り分け用。JMA側の状態とDBの中身を表示する。"""
    now = datetime.now(timezone(timedelta(hours=9))); yy = str(now.year)[2:]
    print("== targetTc.json")
    for t in get_json(f"{BOSAI}/targetTc.json", []):
        tc = t.get("tropicalCyclone"); print(t)
        got = live_point(get_json(f"{BOSAI}/{tc}/specifications.json", []), get_json(f"{BOSAI}/{tc}/forecast.json", []))
        print("   ->", got and (got[0], got[1], f"過去軌跡{len(got[2])}点"))
    print("== 位置表PDF")
    for n in range(max(1, int(os.environ.get("DIAG_FROM", "20"))), 36):
        try:
            r = requests.get(f"{JMA}/data/T{yy}{n:02d}.pdf", headers=UA, timeout=30, stream=True); r.close()
            print(f"T{yy}{n:02d}.pdf", r.status_code, r.headers.get("Last-Modified", ""))
        except Exception as e:
            print(f"T{yy}{n:02d}.pdf", repr(e))
    print("== デジタル台風（PDF未掲載の補完元）")
    for n in range(20, 36):
        g = dt_track(f"{now.year}{n:02d}")
        if g: print(f"{now.year}{n:02d}", g[0], f"{len(g[1])}点", g[1][-1][0] if g[1] else "")
    print("== DB")
    con = sqlite3.connect(DB)
    for r in con.execute("SELECT sid, number, title, prov, start_time, end_time, (SELECT COUNT(*) FROM points p WHERE p.sid=t.sid) "
                         "FROM typhoons t WHERE year=? ORDER BY number", (now.year,)):
        print(r)
    con.close()

# ---------------- API ----------------
@asynccontextmanager
async def lifespan(_):
    if not os.path.exists(DB):
        await asyncio.to_thread(build)

    async def loop():
        while True:  # 速報値・発生中の台風を定期的に再取得（既定60分。REFRESH_MINUTESで変更可）
            await asyncio.to_thread(refresh_recent)
            await asyncio.sleep(INTERVAL)
    task = asyncio.create_task(loop())
    yield
    task.cancel()

app = FastAPI(title="Typhoon DB", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

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
    r = q("SELECT MIN(year) AS min, MAX(year) AS max FROM typhoons")[0]
    for k in ("built_at", "jma_at"):
        r[k] = (q("SELECT v FROM meta WHERE k=?", (k,)) or [{"v": ""}])[0]["v"]
    return r

SORTS = {"number": "year {d}, number {d}", "date": "start_time {d}",
         "wind": "max_wind IS NULL, max_wind {d}", "pres": "min_pres IS NULL, min_pres {d}",
         "days": "days {d}", "name": "name_en = '', name_en {d}"}

@app.get("/api/typhoons")
def typhoons(name: str | None = None, year_from: int | None = None, year_to: int | None = None,
             month: int | None = None, wind_min: float | None = None, wind_max: float | None = None,
             pres_max: float | None = None, days_min: float | None = None, named: bool = False,
             sort: str = "number", order: str = "desc", limit: int = Query(300, le=1000)):
    where, args = [], []
    def add(c, v): where.append(c); args.append(v)
    if name and name.strip():
        where.append("(title LIKE ? OR name_en LIKE ?)")
        args += [f"%{name.strip()}%", f"%{name.strip().upper()}%"]
    if year_from is not None: add("year>=?", year_from)
    if year_to is not None: add("year<=?", year_to)
    if month is not None: add("month=?", month)
    if wind_min is not None: add(f"CAST(max_wind*{KT2MS}+0.5 AS INTEGER)>=?", wind_min)  # m/s指定
    if wind_max is not None: add(f"CAST(max_wind*{KT2MS}+0.5 AS INTEGER)<=?", wind_max)
    if pres_max is not None: add("min_pres<=?", pres_max)
    if days_min is not None: add("days>=?", days_min)
    if named: where.append("name_en<>''")
    w = ("WHERE " + " AND ".join(where)) if where else ""
    ob = SORTS.get(sort, SORTS["number"]).format(d="ASC" if order == "asc" else "DESC")
    total = q(f"SELECT COUNT(*) AS n FROM typhoons {w}", args)[0]["n"]
    rows = q(f"SELECT * FROM typhoons {w} ORDER BY {ob} LIMIT ?", (*args, limit))
    for r in rows: r["max_wind"] = to_ms(r["max_wind"])
    return {"total": total, "rows": rows}

@app.get("/api/typhoons/{sid}")
def detail(sid: str):
    t = q("SELECT * FROM typhoons WHERE sid=?", (sid,))
    if not t: raise HTTPException(404, "台風が見つかりません")
    track = q("SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time", (sid,))
    for p in track: p["wind"] = to_ms(p["wind"])
    return {**t[0], "max_wind": to_ms(t[0]["max_wind"]), "track": track}

# ---------------- 画面 ----------------
PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0a1120">
<title>台風データベース</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a1120;--s1:#101a30;--s2:#17233f;--line:#22314f;--ink:#e9eefb;--sub:#8b9bbb;--acc:#38bdf8;--r:14px;--mh:44dvh}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;height:100dvh;display:grid;grid-template-columns:400px 1fr;background:var(--bg);color:var(--ink);font:14px/1.5 "Zen Kaku Gothic New",system-ui,sans-serif;overflow:hidden}
aside{display:flex;flex-direction:column;min-height:0;background:var(--s1);border-right:1px solid var(--line)}
header{padding:18px 16px 10px;display:grid;gap:10px}
.top{display:flex;justify-content:space-between;align-items:center;gap:8px}
h1{margin:0;font-size:20px;font-weight:900;letter-spacing:.02em}
.pill{font-size:11px;color:var(--sub);border:1px solid var(--line);border-radius:99px;padding:2px 10px;white-space:nowrap}
.pill b{color:var(--acc);font-weight:700}
input,select,button{font:inherit;color:var(--ink)}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input:not([type=checkbox]),.f select{width:100%;padding:9px 12px;background:var(--s2);border:1px solid transparent;border-radius:10px}
.f input:not([type=checkbox]):focus,.f select:focus{border-color:var(--acc);outline:0}
.f .wide,.f details{grid-column:span 2}
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip,#dir,#reset{background:var(--s2);border:1px solid transparent;border-radius:99px;padding:5px 12px;font-size:12px;cursor:pointer}
.chip.on{background:var(--acc);color:#04121f;font-weight:700}
.sort{display:grid;grid-template-columns:1fr auto;gap:8px}
#dir{border-radius:10px;padding:0 14px;font-size:16px}
summary{cursor:pointer;color:var(--sub);font-size:13px;padding:2px 0}
.f2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.f label{font-size:12px;color:var(--sub);display:flex;flex-direction:column;gap:3px}
.f label.chk{flex-direction:row;align-items:center;gap:6px}
#reset{border-radius:10px}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}
#count{padding:0 16px 8px;color:var(--sub);font-size:12px;transition:opacity .2s}
#count.busy{opacity:.5}
#list{flex:1;overflow:auto;overscroll-behavior:contain;margin:0;padding:0 10px 10px;list-style:none;display:grid;gap:6px;align-content:start;-webkit-overflow-scrolling:touch}
#list li{display:flex;gap:12px;align-items:center;padding:10px 12px;border-radius:12px;background:transparent;cursor:pointer;border:1px solid transparent}
#list li:hover{background:var(--s2)}#list li.on{background:var(--s2);border-color:var(--acc)}
.bar{width:4px;align-self:stretch;border-radius:4px;flex:none}
.t{flex:1;min-width:0}.t b{display:block;font-size:14px}
.t span{color:var(--sub);font-size:12px}
.t em{font-style:normal;font-size:10px;margin-left:6px;padding:1px 6px;border-radius:99px;background:#f0803c22;color:#f0a06c;vertical-align:1px}
.m{height:3px;background:var(--line);border-radius:3px;margin-top:6px}.m i{display:block;height:100%;border-radius:3px}
.v{text-align:right;line-height:1.2}.v strong{font:700 20px "Space Grotesk",sans-serif}.v small{display:block;color:var(--sub);font-size:11px}
footer{padding:8px 16px;border-top:1px solid var(--line);color:var(--sub);font-size:11px}
main{position:relative;min-height:0}#map{height:100%;background:#0b1522}
.card{position:absolute;z-index:500;background:rgba(16,26,48,.9);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:var(--r)}
#info{right:14px;top:14px;padding:12px 14px;width:320px;max-width:calc(100% - 28px)}
.hd{display:flex;align-items:flex-start;gap:6px}.hd h2{flex:1;margin:0;font-size:17px;min-width:0}
.ib{flex:none;width:34px;height:34px;border:0;border-radius:10px;background:var(--s2);cursor:pointer;font-size:15px;line-height:1}
.ib:active{background:var(--acc);color:#04121f}
#info .sub{color:var(--sub);font-size:12px}
.g{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}
.g div{background:var(--s2);border-radius:10px;padding:6px 10px}
.g small{display:block;color:var(--sub);font-size:11px}.g strong{font:700 17px "Space Grotesk",sans-serif}.g strong span{font-size:11px;font-weight:400;color:var(--sub)}
.pl{display:flex;align-items:center;gap:8px;margin-top:10px}
#play{width:38px;height:38px;border-radius:50%;background:var(--acc);color:#04121f}
.chart{flex:1;min-width:0;position:relative}
.chart svg{display:block;width:100%;height:40px}
#scrub{width:100%;margin:0;accent-color:var(--acc);height:20px}
#rd{font-size:12px;color:var(--sub);margin-top:4px;font-variant-numeric:tabular-nums}
#legend{left:14px;bottom:24px;padding:8px 12px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;border-radius:2px;display:inline-block}
html{overscroll-behavior:none}button{touch-action:manipulation}
.st{display:none;flex-wrap:wrap;align-items:baseline;gap:2px 12px;margin-top:6px;font-size:13px}.st b{font-size:14px}
.acts{display:none;grid-template-columns:1.2fr 1fr 1fr 1fr;gap:8px;margin-top:10px}
.acts button{height:46px;border:0;border-radius:12px;background:var(--s2);font-weight:700;font-size:13px;cursor:pointer}
.acts button:first-child{background:var(--acc);color:#04121f}.acts button:active{filter:brightness(1.25)}
#fit{position:absolute;z-index:600;left:14px;top:14px;width:40px;height:40px;border:1px solid var(--line);border-radius:12px;background:rgba(16,26,48,.9);font-size:18px;cursor:pointer}
#fab,#bd,.grab,#cl{display:none}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:var(--s2);border:1px solid var(--line);padding:8px 16px;border-radius:99px;font-size:13px;z-index:2000;opacity:0;pointer-events:none;transition:opacity .2s}
#toast.on{opacity:1}
.leaflet-tooltip{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:8px}
.leaflet-control-attribution{font-size:9px}
@media(max-width:760px){
 body{display:block}
 main{position:fixed;inset:0}
 #bd{display:block;position:fixed;inset:0;z-index:1400;background:rgba(0,0,0,.5);opacity:0;pointer-events:none;transition:opacity .28s}
 body.lst #bd{opacity:1;pointer-events:auto}
 aside{position:fixed;left:0;right:0;bottom:0;height:86dvh;z-index:1500;border:0;border-top:1px solid var(--line);border-radius:20px 20px 0 0;box-shadow:0 -12px 40px #000a;transform:translateY(105%);transition:transform .3s cubic-bezier(.2,.8,.2,1);padding-bottom:env(safe-area-inset-bottom);overscroll-behavior:contain}
 body.lst aside{transform:none}
 .grab{display:block;height:24px;position:relative;flex:none}.grab::before{content:"";position:absolute;left:50%;top:10px;width:44px;height:5px;margin-left:-22px;border-radius:3px;background:var(--line)}
 #cl{display:block;flex:none;width:40px;height:40px;border:0;border-radius:12px;background:var(--s2);font-size:16px}
 header{flex:none;max-height:52%;overflow:auto;padding:0 12px 8px;gap:10px;overscroll-behavior:contain}
 h1{font-size:18px}.top .pill{margin-left:auto}footer{padding:10px 14px}
 .f input:not([type=checkbox]),.f select{font-size:16px;padding:12px 14px;min-height:46px}  /* iOSの自動ズーム防止 */
 .chips{flex-wrap:nowrap;overflow-x:auto;margin:0 -12px;padding:0 12px;scrollbar-width:none}.chips::-webkit-scrollbar{display:none}
 .chip{flex:none;min-height:40px;padding:0 16px;font-size:14px}
 #dir{min-width:52px}#reset{min-height:46px}summary{padding:10px 0;font-size:14px}
 #count{padding:4px 14px 8px}
 #list{padding:0 8px 16px;gap:4px}#list li{padding:14px 12px;min-height:64px}.t b{font-size:15px}
 #info{left:0;right:0;bottom:0;top:auto;width:auto;max-width:none;border-radius:20px 20px 0 0;border-width:1px 0 0;padding:12px 14px calc(12px + env(safe-area-inset-bottom))}
 .hd h2{font-size:17px}.hd .dk,.g{display:none}.st{display:flex}.acts{display:grid}
 #play{width:44px;height:44px;font-size:16px}.pl{margin-top:8px}.chart svg{height:26px}#scrub{height:28px}
 .leaflet-control-zoom,.leaflet-control-attribution{display:none}
 #legend{left:10px;top:10px;bottom:auto;display:flex;gap:10px;padding:6px 10px;font-size:11px;max-width:calc(100% - 68px);overflow-x:auto;scrollbar-width:none}
 #legend div{flex:none;gap:4px}
 #fit{left:auto;right:10px;top:10px;width:44px;height:44px}
 #fab{display:block;position:absolute;z-index:600;left:50%;bottom:calc(16px + env(safe-area-inset-bottom));transform:translateX(-50%);padding:0 26px;height:48px;border:0;border-radius:99px;background:var(--acc);color:#04121f;font-weight:700;font-size:15px;box-shadow:0 6px 20px #0008}
 #info:not([hidden])~#fab{display:none}
 #toast{top:64px;bottom:auto}
}
@media(max-width:760px) and (max-height:520px){.st,.chart svg{display:none}}
</style></head><body>
<div id="bd" onclick="closePanel()"></div>
<aside>
 <div class="grab" id="grab"></div>
 <header>
  <div class="top"><h1>台風データベース</h1><span class="pill" id="st"></span><button type="button" id="cl" aria-label="閉じる">✕</button></div>
  <form class="f" id="f" onsubmit="return false">
   <input class="wide" name="name" type="search" enterkeyhint="search" autocomplete="off" placeholder="名前・号数で検索（例: SURIGAE / 15号）">
   <div class="chips wide">
    <button type="button" class="chip" data-w="">全ての強さ</button><button type="button" class="chip" data-w="33">強い〜</button>
    <button type="button" class="chip" data-w="44">非常に強い〜</button><button type="button" class="chip" data-w="54">猛烈な</button></div>
   <div class="sort wide"><select name="sort"><option value="number">号数順</option><option value="date">発生日順</option><option value="wind">最大風速順</option><option value="pres">最低気圧順</option><option value="days">継続日数順</option><option value="name">名前順</option></select>
    <button type="button" id="dir" title="昇順・降順">↓</button><input type="hidden" name="order" value="desc"></div>
   <details><summary>詳細条件</summary><div class="f2">
    <label>年（から）<select name="year_from"><option value="">指定なし</option></select></label>
    <label>年（まで）<select name="year_to"><option value="">指定なし</option></select></label>
    <label>発生月<select name="month"><option value="">全て</option></select></label>
    <label>表示件数<select name="limit"><option>100</option><option selected>300</option><option>1000</option></select></label>
    <label>最大風速 m/s 以上<input type="number" inputmode="numeric" name="wind_min" min="0"></label>
    <label>最大風速 m/s 以下<input type="number" inputmode="numeric" name="wind_max" min="0"></label>
    <label>最低気圧 hPa 以下<input type="number" inputmode="numeric" name="pres_max" placeholder="例: 930"></label>
    <label>継続日数 以上<input type="number" inputmode="decimal" name="days_min" min="0" step="0.5"></label>
    <label class="chk"><input type="checkbox" name="named">名前付きのみ</label>
    <button type="button" id="reset">条件をリセット</button>
   </div></details>
  </form></header>
 <div id="count"></div><ul id="list"></ul>
 <footer>出典: 気象庁（ベストトラック／位置表。IBTrACS経由）。風速は10分平均、時刻は日本時間。「速報」は速報値で後日修正されます。地図タイル: Esri。</footer>
</aside>
<main><div id="map"></div><button id="fit" type="button" aria-label="進路全体を表示">⌖</button><div id="info" class="card" hidden></div><button id="fab" type="button">☰ 台風一覧・検索</button><div id="legend" class="card"></div></main>
<div id="toast"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const CLS=[[54,"猛烈な","#ff4d7d"],[44,"非常に強い","#ff8a3d"],[33,"強い","#ffd24a"],[17,"台風","#4fc3f7"],[0,"熱帯低気圧","#64789a"]];  // m/s
const UNK=[null,"不明","#3d4f66"];
const K=w=>w==null?UNK:CLS.find(c=>w>=c[0]), col=w=>K(w)[2], cls=w=>K(w)[1];
const spd=w=>w==null?"-":`${w}m/s`;
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const jst=(s,full)=>{const d=new Date(new Date(s.replace(" ","T")+"Z").getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");
  return `${full?d.getUTCFullYear()+"/":""}${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}時`};
const $=s=>document.querySelector(s), f=$("#f"), chips=document.querySelectorAll(".chip");
const mobile=()=>matchMedia("(max-width:760px)").matches;
const store={get(){try{return JSON.parse(localStorage.getItem("tf")||"{}")}catch(e){return{}}},set(v){try{localStorage.setItem("tf",JSON.stringify(v))}catch(e){}}};
function toast(m){const t=$("#toast");t.textContent=m;t.classList.add("on");clearTimeout(toast.t);toast.t=setTimeout(()=>t.classList.remove("on"),1800)}
const km=(a,b)=>{const r=Math.PI/180,dl=(b[1]-a[1])*r,p1=a[0]*r,p2=b[0]*r;
  return 6371*Math.acos(Math.min(1,Math.sin(p1)*Math.sin(p2)+Math.cos(p1)*Math.cos(p2)*Math.cos(dl)))};
const map=L.map("map",{worldCopyJump:true,zoomControl:false}).setView([25,135],4);
L.control.zoom({position:"bottomright"}).addTo(map);
const ESRI="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/";
L.tileLayer(ESRI+"World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",{attribution:"Tiles © Esri — Esri, DeLorme, NAVTEQ | 気象庁 / IBTrACS (NOAA)",maxZoom:12}).addTo(map);
map.createPane("labels").style.zIndex=450;map.getPane("labels").style.pointerEvents="none";
L.tileLayer(ESRI+"World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}",{pane:"labels",maxZoom:12}).addTo(map);
let layer=L.layerGroup().addTo(map), first=true, cur=null, playT=null;
$("#legend").innerHTML=[...CLS,UNK].map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join("");
// 一覧パネル（スマホ: 下から出るシート）
const openPanel=()=>{document.body.classList.add("lst");setTimeout(()=>{const l=$("#list li.on");l&&reveal(l)},60)};
const closePanel=()=>document.body.classList.remove("lst");
$("#cl").onclick=closePanel;$("#fab").onclick=openPanel;
{let sy=null,dy=0;const a=$("aside");   // シートを下へスワイプして閉じる
 for(const el of [$("#grab"),$(".top")]){
  el.addEventListener("touchstart",e=>{sy=e.touches[0].clientY;dy=0;a.style.transition="none"},{passive:true});
  el.addEventListener("touchmove",e=>{if(sy==null)return;dy=Math.max(0,e.touches[0].clientY-sy);a.style.transform=`translateY(${dy}px)`},{passive:true});
  el.addEventListener("touchend",()=>{if(sy==null)return;sy=null;a.style.transition="";a.style.transform="";if(dy>80)closePanel()})}}
f.name.addEventListener("keydown",e=>{if(e.key=="Enter")e.target.blur()});  // 検索確定でキーボードを閉じる
function reveal(li){const L2=$("#list"),a=li.getBoundingClientRect(),b=L2.getBoundingClientRect();
  if(a.top<b.top)L2.scrollTop-=b.top-a.top+8;else if(a.bottom>b.bottom)L2.scrollTop+=a.bottom-b.bottom+8}
const barH=()=>mobile()&&!$("#info").hidden?$("#info").offsetHeight:0;
const fitOpt=()=>mobile()?{paddingTopLeft:[16,54],paddingBottomRight:[16,barH()+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[360,40]};
$("#fit").onclick=()=>cur&&map.fitBounds(cur.bounds,fitOpt());
addEventListener("resize",()=>map.invalidateSize());

let jmaAt="";
async function init(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--){f.year_from.add(new Option(i+"年",i));f.year_to.add(new Option(i+"年",i))}
  for(let m=1;m<=12;m++) f.month.add(new Option(m+"月",m));
  const sv=store.get();for(const k in sv) if(f[k]&&f[k].type!="checkbox") f[k].value=sv[k];  // 前回の絞り込みを復元
  f.named.checked=sv.named==="true";
  jmaAt=y.jma_at||"";if(jmaAt) $("#st").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
  $("#reset").onclick=()=>{f.reset();f.order.value="desc";load()};
  $("#dir").onclick=()=>{f.order.value=f.order.value=="desc"?"asc":"desc";load()};
  chips.forEach(c=>c.onclick=()=>{f.wind_min.value=c.dataset.w;f.wind_max.value="";load()});
  f.addEventListener("input",e=>{if(e.isComposing)return;clearTimeout(init.t);init.t=setTimeout(load,250)});
  f.addEventListener("compositionend",()=>{clearTimeout(init.t);init.t=setTimeout(load,250)});  // 日本語入力の確定後に検索
  load();
}
let ctl;
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year_from","year_to","month","wind_min","wind_max","pres_max","days_min","sort","order","limit"]) if(f[k].value) p.set(k,f[k].value);
  if(f.named.checked) p.set("named","true");
  store.set(Object.fromEntries(p));
  chips.forEach(c=>c.classList.toggle("on",c.dataset.w===f.wind_min.value&&!f.wind_max.value));
  $("#dir").textContent=f.order.value=="desc"?"↓":"↑";
  ctl&&ctl.abort();ctl=new AbortController();$("#count").classList.add("busy");  // 古いリクエストを破棄
  try{
    const {total,rows}=await (await fetch("/api/typhoons?"+p,{signal:ctl.signal})).json();
    $("#count").classList.remove("busy");
    $("#count").textContent=total?`${total}件`+(total>rows.length?`中 ${rows.length}件を表示（件数を増やすか条件を絞ってください）`:""):"該当なし。条件を変えてください";
    $("#list").innerHTML=rows.map(r=>`<li data-sid="${esc(r.sid)}"><span class="bar" style="background:${col(r.max_wind)}"></span>
     <div class="t"><b>${esc(r.title)}${r.prov?"<em>速報</em>":""}</b><span>${jst(r.start_time,1)}〜 ／ ${r.days}日</span>
     <div class="m"><i style="width:${Math.min(100,(r.max_wind||0)/0.67)}%;background:${col(r.max_wind)}"></i></div></div>
     <div class="v"><strong>${r.max_wind??"-"}</strong><small>m/s</small><small>${r.min_pres??"-"}hPa</small></div></li>`).join("");
    const s0=location.hash.slice(1);const on=s0&&$(`#list li[data-sid="${CSS.escape(s0)}"]`);if(on)on.classList.add("on");
    if(first){first=false;const s=s0||(rows[0]&&rows[0].sid);if(s)show(s)}  // 共有リンク or 最新の台風を自動表示
  }catch(e){if(e.name!="AbortError"){$("#count").classList.remove("busy");$("#count").textContent="読み込みに失敗しました。再読み込みしてください"}}
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li){if(mobile())closePanel();show(li.dataset.sid,li)}});
function nav(d){const a=[...document.querySelectorAll("#list li")],i=a.findIndex(x=>x.classList.contains("on")),n=a[i+d];if(n)show(n.dataset.sid,n)}
addEventListener("keydown",e=>{if(/INPUT|SELECT|TEXTAREA/.test(e.target.tagName))return;
  if(e.key=="ArrowDown"||e.key=="j"){e.preventDefault();nav(1)}else if(e.key=="ArrowUp"||e.key=="k"){e.preventDefault();nav(-1)}else if(e.key==" "&&cur){e.preventDefault();play()}});
// 進行アニメーション
function setPos(i){if(!cur)return;cur.i=i;const p=cur.pts[i];cur.mk.setLatLng(cur.ll[i]);
  $("#scrub").value=i;$("#rd").textContent=`${jst(p.time,1)} ・ ${spd(p.wind)} ・ ${p.pres??"-"}hPa`;
  const c=$("#cur");if(c){c.setAttribute("x1",cur.xs[i]);c.setAttribute("x2",cur.xs[i])}
  const pt=map.latLngToContainerPoint(cur.ll[i]),s=map.getSize(),bh=barH(),rx=mobile()?0:350,top=mobile()?50:20;
  if(pt.x<24||pt.x>s.x-rx-24||pt.y<top||pt.y>s.y-bh-24)map.panBy([pt.x-(s.x-rx)/2,pt.y-(top+(s.y-bh-top)/2)])}
function stop(){clearInterval(playT);playT=null;const b=$("#play");if(b)b.textContent="▶"}
function play(){if(!cur)return;if(playT)return stop();$("#play").textContent="❚❚";if(cur.i>=cur.pts.length-1)setPos(0);
  playT=setInterval(()=>{if(cur.i>=cur.pts.length-1)return stop();setPos(cur.i+1)},Math.max(60,Math.min(250,6000/cur.pts.length)))}
async function share(){const u=location.href,t=$("#info h2").textContent;
  try{if(navigator.share)await navigator.share({title:t,url:u});else{await navigator.clipboard.writeText(u);toast("リンクをコピーしました")}}catch(e){}}
let seq=0;
async function show(sid,li,keep){
  const my=++seq;stop();
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));
  li=li||document.querySelector(`#list li[data-sid="${CSS.escape(sid)}"]`);
  if(li){li.classList.add("on");if(!keep)reveal(li)}
  history.replaceState(null,"","#"+sid);
  const r=await fetch("/api/typhoons/"+encodeURIComponent(sid));if(!r.ok||my!=seq)return;
  const t=await r.json();if(my!=seq)return;
  layer.clearLayers();
  const pts=t.track;
  // 日付変更線をまたぐ場合に備えて経度を連続化
  let prev=null;const ll=pts.map(p=>{let lo=p.lon;if(prev!==null){while(lo-prev>180)lo-=360;while(lo-prev<-180)lo+=360}prev=lo;return [p.lat,lo]});
  L.polyline(ll,{color:"#fff",weight:9,opacity:.08}).addTo(layer);
  for(let i=1;i<pts.length;i++) L.polyline([ll[i-1],ll[i]],{color:col(pts[i].wind),weight:4,lineCap:"round"}).addTo(layer);
  pts.forEach((p,i)=>{const e=i==0||i==pts.length-1;
    L.circleMarker(ll[i],{radius:e?6:3,color:"#fff",weight:1,fillColor:col(p.wind),fillOpacity:1,interactive:false}).addTo(layer);
    L.circleMarker(ll[i],{radius:14,stroke:false,fillOpacity:0,bubblingMouseEvents:false}).on("click",()=>{stop();setPos(i)})
     .bindTooltip(`${i==0?"発生 ":e?"終了 ":""}${jst(p.time,1)}<br>${spd(p.wind)} / ${p.pres??"-"}hPa`).addTo(layer)});
  const n=pts.length,W=300,xs=pts.map((_,i)=>n>1?i/(n-1)*W:0),mw=Math.max(1,...pts.map(p=>p.wind||0));
  const wl=pts.map((p,i)=>p.wind==null?null:`${xs[i].toFixed(1)},${(38-p.wind/mw*34).toFixed(1)}`).filter(Boolean).join(" ");
  const dist=Math.round(ll.slice(1).reduce((s,p,i)=>s+km(ll[i],p),0)/10)*10;
  cur={pts,ll,xs,i:0,bounds:L.latLngBounds(ll),mk:L.circleMarker(ll[0],{radius:9,color:"#fff",weight:2,fillColor:"#38bdf8",fillOpacity:.9,interactive:false}).addTo(layer)};
  const i=$("#info");i.hidden=false;
  i.innerHTML=`<div class="hd"><h2>${esc(t.title)}${t.prov?'<span class="pill" style="margin-left:8px">速報値</span>':""}</h2>
    <button class="ib dk" onclick="nav(-1)" aria-label="前の台風">◀</button><button class="ib dk" onclick="nav(1)" aria-label="次の台風">▶</button>
    <button class="ib dk" onclick="share()" aria-label="共有">⤴</button><button class="ib dk" onclick="$('#info').hidden=true;stop()" aria-label="閉じる">✕</button></div>
   <div class="sub">${jst(t.start_time,1)} 〜 ${jst(t.end_time,1)}（日本時間）${dist?` ・ 約${dist.toLocaleString()}km`:""}</div>
   <div class="g"><div><small>最大風速</small><strong>${t.max_wind??"-"}<span> m/s</span></strong></div>
   <div><small>強さ</small><strong style="color:${col(t.max_wind)}">${cls(t.max_wind)}</strong></div>
   <div><small>最低気圧</small><strong>${t.min_pres??"-"}<span> hPa</span></strong></div>
   <div><small>継続</small><strong>${t.days}<span> 日</span></strong></div></div>
   <div class="st"><b style="color:${col(t.max_wind)}">${cls(t.max_wind)}</b><span>最大 ${spd(t.max_wind)}</span><span>${t.min_pres??"-"}hPa</span><span>${t.days}日</span></div>
   <div class="pl"><button class="ib" id="play" onclick="play()" aria-label="再生">▶</button><div class="chart"><div id="rd"></div>
    <svg viewBox="0 0 ${W} 40" preserveAspectRatio="none"><polyline points="${wl}" fill="none" stroke="${col(t.max_wind)}" stroke-width="2" vector-effect="non-scaling-stroke"/>
    <line id="cur" x1="0" x2="0" y1="0" y2="40" stroke="#fff" stroke-width="1" vector-effect="non-scaling-stroke"/></svg>
    <input id="scrub" type="range" min="0" max="${Math.max(0,n-1)}" value="0" aria-label="時刻"></div></div>
   <div class="acts"><button type="button" onclick="openPanel()">☰ 一覧</button><button type="button" onclick="nav(-1)">◀ 前</button><button type="button" onclick="nav(1)">次 ▶</button><button type="button" onclick="share()">⤴ 共有</button></div>`;
  $("#scrub").oninput=e=>{stop();setPos(+e.target.value)};
  if(!keep)map.fitBounds(cur.bounds,fitOpt());
  setPos(0);
}
init();
// 開いたままでも、サーバー側で速報が更新されたら一覧と表示中の台風を自動で更新する
async function poll(){
  try{
    const y=await (await fetch("/api/years")).json();
    if(y.jma_at&&y.jma_at!==jmaAt){
      jmaAt=y.jma_at;$("#st").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
      await load();const s=location.hash.slice(1);if(s)show(s,null,true);
    }
  }catch(e){}
}
setInterval(poll,5*60*1000);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)poll()});  // スマホでアプリに戻った時にも確認
</script></body></html>
"""

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build()
    elif len(sys.argv) > 1 and sys.argv[1] == "diag":
        diag()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
