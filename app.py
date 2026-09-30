"""台風データベース（1ファイル版・気象庁ベストトラック準拠）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT   (DBが無ければ起動時に自動ビルド)
表記: 「2026年台風第26号 Surigae」。号数は気象庁方式（熱帯低気圧の段階は数えず、
      台風の強さに初めて達した順に年ごとに採番）で再計算する。
風速はm/s表示（DB内部はkt保持、API出力時に換算）。
今年ぶんは 位置表PDF（確定・速報）＋防災情報JSON（発生中の台風）＋デジタル台風（NII、PDF未掲載の消滅済み台風の補完）で随時更新する。
データ出典: 気象庁 / IBTrACS / デジタル台風（国立情報学研究所・北本朗）
画面: 1ページ・地図共有の3タブ構成（予報 / 過去の台風 / 統計）。旧 /forecast は /#fc へ転送。
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

# ---------------- 予報モデル比較：取得元の診断 ----------------
RAL = "https://hurricanes.ral.ucar.edu/realtime"  # UCAR RAL TCGP：台風ごとの ATCF a-deck（各機関の予報進路）を公開
# a-deck の技術ID → モデル名の「推定」。実データでの確認前なので、fdiag の出力を見て本実装で確定する
GUESS = {"JTWC": "JTWC公式", "AVNO": "GFS", "AVNI": "GFS(補間)", "GFSO": "GFS", "EMX": "ECMWF", "EMXI": "ECMWF(補間)",
         "ECMF": "ECMWF", "UKM": "UKMET", "UKMI": "UKMET(補間)", "UKX": "UKMET", "UKXI": "UKMET(補間)",
         "JGSM": "JMA-GSM", "JGSI": "JMA-GSM(補間)", "NVGM": "NAVGEM", "NGX": "NAVGEM", "CTCX": "COAMPS-TC",
         "HWRF": "HWRF", "HAFS": "HAFS", "BEST": "ベストトラック"}

def parse_adeck(text):
    """ATCF a-deck → [(初期時刻yyyymmddhh, 技術ID, 予報時間tau[h], 緯度, 経度, 風速kt, 気圧hPa)]。壊れた行は捨てる。"""
    out = []
    for ln in text.splitlines():
        c = [x.strip() for x in ln.split(",")]
        if len(c) < 10 or not c[2].isdigit() or not c[5].lstrip("-").isdigit():
            continue
        try:
            la = int(c[6][:-1]) / 10 * (-1 if c[6][-1] == "S" else 1)
            lo = int(c[7][:-1]) / 10 * (-1 if c[7][-1] == "W" else 1)
        except (ValueError, IndexError):
            continue
        v, p = (int(x) if x.lstrip("-").isdigit() and int(x) > 0 else None for x in (c[8], c[9]))
        out.append((c[2], c[4], int(c[5]), la, lo, v, p))
    return out

def fdiag(P=print, storms=None):
    """python app.py fdiag : 予報比較の取得元を確認する（JMA予報円・a-deckに載っているモデルID・更新時刻）。
    環境変数 FDIAG_STORMS="wp25,wp26"（または /api/fdiag?storms=wp25）で対象を指定できる（未指定なら UCAR の「現在の活動中」ページから自動検出）。"""
    P("== JMA 予報（防災情報JSON forecast.json）")
    for t in get_json(f"{BOSAI}/targetTc.json", []):
        tc = t.get("tropicalCyclone")
        fc = get_json(f"{BOSAI}/{tc}/forecast.json", [])
        pts = [(r.get("advancedHours"), r.get("center")) for r in fc if isinstance(r, dict) and r.get("advancedHours") is not None]
        P(tc, t.get("typhoonNumber"), t.get("category"), f"予報{len(pts)}点", pts[:8])
    P("== UCAR RAL a-deck")
    yr = datetime.now(timezone.utc).year
    ids = [x.strip().lower() for x in (storms or os.environ.get("FDIAG_STORMS", "")).split(",") if x.strip()]
    if not ids:
        try:
            html = requests.get(f"{RAL}/current/", headers=UA, timeout=30).text
            ids = sorted(set(re.findall(r"northwestpacific/\d{4}/(wp\d{2})\d{4}/", html)))
        except Exception as e:
            P("活動中ページの取得に失敗:", repr(e))
    P("対象:", ids or "なし（FDIAG_STORMS=wp25 のように指定できます）")
    for sid in ids:
        url = f"{RAL}/plots/northwestpacific/{yr}/{sid}{yr}/a{sid}{yr}.dat"
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except Exception as e:
            P(sid, "取得失敗", repr(e)); continue
        P(f"\n{sid}: HTTP {r.status_code}  Last-Modified: {r.headers.get('Last-Modified', '-')}  {len(r.content)//1024}KB\n  {url}")
        rows = parse_adeck(r.text) if r.ok else []
        if not rows:
            P("  パースできる行なし"); continue
        latest = max(x[0] for x in rows)
        P(f"  最新の初期時刻: {latest} UTC ／ 行数 {len(rows)} ／ 初期時刻の種類 {len({x[0] for x in rows})}")
        P("  技術ID  推定モデル      初期時刻数  最新初期時刻  最新の点数  最大tau  +72h位置")
        for tech in sorted({x[1] for x in rows}):
            mine = [x for x in rows if x[1] == tech]
            li = max(x[0] for x in mine); cur = [x for x in mine if x[0] == li]
            p72 = next(((x[3], x[4]) for x in cur if x[2] == 72), None)
            P(f"  {tech:<7} {GUESS.get(tech, '?'):<14} {len({x[0] for x in mine}):>8}  {li:>12}  {len(cur):>8}  {max(x[2] for x in cur):>6}  {p72 or '-'}")
    P("\n== Google DeepMind Weather Lab（AIアンサンブル）")
    now = datetime.now(timezone.utc)
    base = now.replace(hour=now.hour // 6 * 6, minute=0, second=0, microsecond=0)
    for model in WL_MODELS:
        for i in range(6):
            cyc = base - timedelta(hours=6 * i)
            url = WL_URL.format(model=model, kind="ensemble", ts=cyc.strftime("%Y_%m_%dT%H_00"))
            try:
                r = requests.get(url, headers=UA, timeout=60)
            except Exception as e:
                P(model, cyc.strftime("%m/%d %HZ"), "取得失敗", repr(e)); break
            if r.status_code != 200 or r.text.lstrip()[:1] == "<":
                P(model, cyc.strftime("%m/%d %HZ"), f"HTTP {r.status_code}"); continue
            lines = r.text.splitlines()
            g = wl_parse(r.text)
            P(f"{model} {cyc.strftime('%m/%d %HZ')} OK {len(r.content)//1024}KB  {url}")
            P("  列名:", lines[0][:300])
            P("  1行目:", lines[1][:300] if len(lines) > 1 else "-")
            P("  パース結果:", {k: len(v) for k, v in list(g.items())[:12]}, "（台風ID: メンバー数）")
            break

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

_fd = {"t": 0, "k": None, "txt": ""}
@app.get("/api/fdiag")
def api_fdiag(storms: str = ""):
    """予報比較の取得元診断をブラウザで見る用（Renderの無料プランはShellが使えないため）。5分間は結果を使い回す。"""
    from fastapi.responses import PlainTextResponse
    import time
    if _fd["k"] != storms or time.time() - _fd["t"] > 300:
        out = []
        try: fdiag(lambda *a: out.append(" ".join(str(x) for x in a)), storms)
        except Exception as e: out.append(f"エラー: {e!r}")
        _fd.update(t=time.time(), k=storms, txt="\n".join(out))
    return PlainTextResponse(_fd["txt"])

@app.get("/api/years")
def years():
    r = q("SELECT MIN(year) AS min, MAX(year) AS max FROM typhoons")[0]
    for k in ("built_at", "jma_at"):
        r[k] = (q("SELECT v FROM meta WHERE k=?", (k,)) or [{"v": ""}])[0]["v"]
    return r

SORTS = {"number": "year {d}, number {d}", "date": "start_time {d}",
         "wind": "max_wind IS NULL, max_wind {d}", "pres": "min_pres IS NULL, min_pres {d}",
         "days": "days {d}", "name": "name_en = '', name_en {d}"}

import math, json
from fastapi import Depends
from fastapi.responses import Response, JSONResponse, RedirectResponse

def flt(name: str | None = None, year_from: int | None = None, year_to: int | None = None,
        month: int | None = None, wind_min: float | None = None, wind_max: float | None = None,
        pres_max: float | None = None, days_min: float | None = None, named: bool = False,
        near: str | None = None):
    """絞り込み条件（一覧・重ね表示・統計で共通）→ (WHERE句, 引数)。near は 'lat,lon,km'"""
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
    if near:
        try: la, lo, km = (float(x) for x in near.split(","))
        except ValueError: raise HTTPException(422, "near は lat,lon,km の形式で指定してください")
        dla, dlo = km / 111.0, km / (111.0 * max(0.05, math.cos(math.radians(la))))
        ids = set()
        for sh in (0, 360, -360):  # 経度が -180..180 でも 0..360 でも拾えるように
            for r in q("SELECT sid, lat, lon FROM points WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
                       (la - dla, la + dla, lo + sh - dlo, lo + sh + dlo)):
                if r["sid"] in ids: continue
                p1, p2, d = math.radians(r["lat"]), math.radians(la), math.radians((r["lon"] - lo + 540) % 360 - 180)
                c = math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(d)
                if 6371 * math.acos(max(-1.0, min(1.0, c))) <= km: ids.add(r["sid"])
        where.append("sid IN (SELECT value FROM json_each(?))"); args.append(json.dumps(sorted(ids)))
    return (("WHERE " + " AND ".join(where)) if where else ""), args

@app.get("/api/typhoons")
def typhoons(fl=Depends(flt), sort: str = "number", order: str = "desc", limit: int = Query(300, le=1000)):
    w, args = fl
    ob = SORTS.get(sort, SORTS["number"]).format(d="ASC" if order == "asc" else "DESC")
    total = q(f"SELECT COUNT(*) AS n FROM typhoons {w}", args)[0]["n"]
    rows = q(f"SELECT * FROM typhoons {w} ORDER BY {ob} LIMIT ?", (*args, limit))
    for r in rows: r["max_wind"] = to_ms(r["max_wind"])
    return {"total": total, "rows": rows}

@app.get("/api/tracks")
def tracks(fl=Depends(flt), limit: int = Query(300, le=600)):
    """条件に合う台風の進路をまとめて返す（地図への重ね表示用。点は間引く）"""
    w, args = fl
    sub = f"SELECT sid, title, max_wind FROM typhoons {w} ORDER BY year DESC, number DESC LIMIT ?"
    ts = {r["sid"]: {"sid": r["sid"], "title": r["title"], "w": to_ms(r["max_wind"]), "p": []} for r in q(sub, (*args, limit))}
    for r in q(f"SELECT sid, lat, lon FROM points WHERE sid IN (SELECT sid FROM ({sub})) ORDER BY sid, time", (*args, limit)):
        ts[r["sid"]]["p"].append([round(r["lat"], 2), round(r["lon"], 2)])
    for t in ts.values(): t["p"] = t["p"][::2] + ([t["p"][-1]] if len(t["p"]) % 2 == 0 else [])
    return list(ts.values())

@app.get("/api/stats")
def stats(fl=Depends(flt)):
    w, args = fl
    rows = q(f"SELECT year, month, max_wind, days, title FROM typhoons {w}", args)
    yrs, mon, cls = {}, [0] * 12, {}
    for r in rows:
        yrs[r["year"]] = yrs.get(r["year"], 0) + 1
        if r["month"] and 1 <= r["month"] <= 12: mon[r["month"] - 1] += 1
        v = to_ms(r["max_wind"])
        k = "不明" if v is None else next(n for t, n in ((54, "猛烈な"), (44, "非常に強い"), (33, "強い"), (17, "台風"), (0, "熱帯低気圧")) if v >= t)
        cls[k] = cls.get(k, 0) + 1
    ds = [r for r in rows if r["days"] is not None]
    mx = max(ds, key=lambda r: r["days"], default=None)
    return {"total": len(rows), "months": mon, "classes": cls,
            "years": [[y, yrs.get(y, 0)] for y in range(min(yrs), max(yrs) + 1)] if yrs else [],
            "avg_days": round(sum(r["days"] for r in ds) / len(ds), 1) if ds else None,
            "max_row": {"title": mx["title"], "days": mx["days"]} if mx else None}

# ---- PWA（ホーム画面に追加・オフライン時は直近のデータを表示）----
ICON = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" fill="#0a1120"/>'
        '<g fill="none" stroke="#38bdf8" stroke-linecap="round" stroke-width="32"><circle cx="256" cy="256" r="26" fill="#38bdf8"/>'
        '<path d="M256 170a86 86 0 0 1 86 86M256 342a86 86 0 0 1-86-86M256 110a146 146 0 0 1 146 146M256 402a146 146 0 0 1-146-146" opacity=".85"/></g></svg>')
SW = r"""const V="tf-v2";
self.addEventListener("install",e=>{e.waitUntil(caches.open(V).then(c=>c.addAll(["/"])));self.skipWaiting()});
self.addEventListener("activate",e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!=V).map(x=>caches.delete(x)))).then(()=>clients.claim())));
self.addEventListener("fetch",e=>{const r=e.request,u=new URL(r.url);
  if(r.method!="GET"||/arcgisonline/.test(u.host))return;  // 地図タイルはキャッシュしない
  e.respondWith(fetch(r).then(x=>{if(x.ok||x.type=="opaque"){const c=x.clone();caches.open(V).then(k=>k.put(r,c))}return x}).catch(()=>caches.match(r)))});
"""
@app.get("/manifest.webmanifest")
def manifest():
    return JSONResponse({"name": "台風データベース", "short_name": "台風DB", "start_url": "/", "display": "standalone",
                         "background_color": "#0a1120", "theme_color": "#0a1120",
                         "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}]},
                        media_type="application/manifest+json")
@app.get("/icon.svg")
def icon(): return Response(ICON, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})
@app.get("/sw.js")
def sw(): return Response(SW, media_type="application/javascript", headers={"Cache-Control": "no-cache"})

@app.get("/api/typhoons/{sid}")
def detail(sid: str):
    t = q("SELECT * FROM typhoons WHERE sid=?", (sid,))
    if not t: raise HTTPException(404, "台風が見つかりません")
    track = q("SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time", (sid,))
    for p in track: p["wind"] = to_ms(p["wind"])
    return {**t[0], "max_wind": to_ms(t[0]["max_wind"]), "track": track}

# ---------------- 予報モデル比較（別タブ /forecast）----------------
import time as _t
_fc_cache = {}
def _cached(key, ttl, fn):
    v = _fc_cache.get(key)
    if v and _t.time() - v[0] < ttl: return v[1]
    r = fn(); _fc_cache[key] = (_t.time(), r); return r

def ll_dist(a, b):
    """2点間の距離[km]"""
    r = math.radians
    c = math.sin(r(a[0])) * math.sin(r(b[0])) + math.cos(r(a[0])) * math.cos(r(b[0])) * math.cos(r((a[1] - b[1] + 540) % 360 - 180))
    return 6371 * math.acos(max(-1.0, min(1.0, c)))

# a-deck の技術ID。実データ（fdiag）で確認できたもの: UKM=UKMET, NGX=NAVGEM, CMC=CMC。
# アンサンブルは ATCF の慣例: GEFS=AC00/AP01-30/AEMN、CMC=CC00/CP01-20/CEMN、NAVGEM=NC00/NP01-20/NEMN
DET = {"UKM": "UKMET", "NGX": "NAVGEM", "CMC": "CMC", "JTWC": "JTWC公式", "AVNO": "GFS", "GFSO": "GFS",
       "EMX": "ECMWF", "ECMF": "ECMWF", "JGSM": "JMA-GSM", "HWRF": "HWRF", "HAFS": "HAFS", "CTCX": "COAMPS-TC"}
# アンサンブルの技術ID（ATCF慣例）: 先頭1文字=機関、メンバー=xP01〜、コントロール=xC00/xE00、平均=xEMN
#   A=GEFS(NCEP) C=CMC-EPS N=NAVGEM-EPS E=ECMWF-ENS U=UKMET(MOGREPS) J=JMA-GEPS
# a-deck に載っているものを自動検出する（載っていない機関は黙って飛ばし、画面の注記に「無いモデル」として出す）
ENS_LET = {"A": "GEFS", "C": "CMC-EPS", "N": "NAVGEM-EPS", "E": "ECMWF-ENS", "U": "UKMET-ENS", "J": "JMA-GEPS"}
ENS_ID = re.compile(r"([A-Z])(P\d\d|C00|E00)")

# Google DeepMind Weather Lab（AIの台風アンサンブル。WNV3=64メンバー、FNV3=50メンバー、GENC=GenCast）
# URLは環境変数 WEATHERLAB_URL で差し替え可（{model} {kind} {ts} が置換される）。/api/fdiag で取得状況とCSVの列名を確認できる
WL_URL = os.environ.get("WEATHERLAB_URL",
    "https://deepmind.google.com/science/weatherlab/download/cyclones/{model}/{kind}/paired/csv/{model}_{kind}_{ts}_paired.csv")
WL_MODELS = {"WNV3": "WeatherNext3(AI)", "FNV3": "WeatherNext2(AI)", "GENC": "GenCast(AI)"}

# 強さの階級（気象庁・10分間平均風速m/s）。モデルの風速は1分間平均(ATCF)なので 0.88 倍して10分間平均相当にする
CATS = [(54, "猛烈な"), (44, "非常に強い"), (33, "強い"), (17.2, "台風"), (0, "熱帯低気圧")]
W10 = 0.88

def _w10(kt, is10=False):
    """kt → 10分間平均相当のm/s（JMA公式はそのまま）"""
    return None if not kt else kt * KT2MS * (1 if is10 else W10)

def _pct(a, p):
    a = sorted(a); k = (len(a) - 1) * p; f = int(k); c = min(f + 1, len(a) - 1)
    return a[f] + (a[c] - a[f]) * (k - f)

def _r1(v):
    return None if v is None else round(v, 1)

def intensity_rows(tracks, is10=False, probs=False):
    """tracks: [{tau: (風速kt, 気圧hPa)}]（1本=1メンバー or 1モデル）→ 12時間刻みの強さ統計。
    行: [tau, 風速平均, 風速P10, 風速P90, 気圧平均, 気圧P10, 気圧P90, 本数, [強い以上%, 非常に強い以上%, 猛烈な%]|None, 風速最小, 風速最大]（風速はm/s）"""
    rows = []
    for t in sorted({t for tr in tracks for t in tr if t % 12 == 0}):
        ws = [w for tr in tracks if t in tr and (w := _w10(tr[t][0], is10))]
        ps = [tr[t][1] for tr in tracks if t in tr and tr[t][1]]
        if not ws and not ps:
            continue
        pr = [round(100 * sum(w >= th for w in ws) / len(ws)) for th in (33, 44, 54)] if (probs and ws) else None
        rows.append([t,
                     _r1(sum(ws) / len(ws)) if ws else None, _r1(_pct(ws, .1)) if ws else None, _r1(_pct(ws, .9)) if ws else None,
                     round(sum(ps) / len(ps)) if ps else None, round(_pct(ps, .1)) if ps else None, round(_pct(ps, .9)) if ps else None,
                     max(len(ws), len(ps)), pr, _r1(min(ws)) if ws else None, _r1(max(ws)) if ws else None])
    return rows

def mean_track(members):
    """メンバー（[[tau, lat, lon, kt, hPa], ...]）から平均進路を作る。存続メンバーが1/3未満の時刻は捨てる。"""
    by = {}
    for m in members:
        for x in m:
            by.setdefault(x[0], []).append(x)
    out = []
    for t in sorted(by):
        g = by[t]
        if len(g) < max(2, len(members) // 3):
            continue
        lo0 = g[0][2]
        vs = [x[3] for x in g if x[3]]; ps = [x[4] for x in g if x[4]]
        out.append([t, round(sum(x[1] for x in g) / len(g), 1),
                    round(lo0 + sum((x[2] - lo0 + 540) % 360 - 180 for x in g) / len(g), 1),
                    round(sum(vs) / len(vs)) if vs else None, round(sum(ps) / len(ps)) if ps else None])
    return out

def _wl_time(s):
    return datetime.strptime(s.strip().replace("Z", "")[:19].replace("T", " "), F)

def wl_parse(text):
    """Weather Lab の CSV → {track_id: {member: {tau: (lat, lon, kt, hPa)}}}。列名は部分一致で探す（列の順序や細かい名前に依存しない）。
    観測（paired の実況）行はメンバー列が空か source が観測系なので除外する。"""
    rd = csv.reader(io.StringIO(text))
    head = next(rd, None)
    if not head:
        return {}
    h = [c.strip().lower() for c in head]
    def col(*keys, bad=()):
        for k in keys:
            for i, c in enumerate(h):
                if k in c and not any(b in c for b in bad):
                    return i
        return None
    i_tr, i_mem = col("track_id", "storm_id", "track", "sid"), col("sample", "member", "ensemble")
    i_ini, i_val, i_lead = col("init"), col("valid"), col("lead")
    i_lat, i_lon = col("lat"), col("lon")
    i_w = col("wind", bad=("radius", "r34", "r50", "r64", "rmw"))
    i_p = col("pressure", "pres", "slp", bad=("radius",))
    i_src = col("source")
    if None in (i_tr, i_lat, i_lon):
        return {}
    ms_unit = i_w is not None and any(k in h[i_w] for k in ("m_s", "ms", "mps")) and "knot" not in h[i_w] and "kt" not in h[i_w]
    out = {}
    for row in rd:
        try:
            if i_mem is None or not row[i_mem].strip():
                continue
            if i_src is not None and any(k in row[i_src].lower() for k in ("obs", "best", "vital", "truth", "anal")):
                continue
            tau = None
            if i_lead is not None:
                try: tau = int(round(float(row[i_lead])))
                except ValueError: tau = None
            if tau is None and i_ini is not None and i_val is not None:
                tau = int(round((_wl_time(row[i_val]) - _wl_time(row[i_ini])).total_seconds() / 3600))
            if tau is None or tau < 0:
                continue
            la, lo = float(row[i_lat]), float(row[i_lon])
            v = num(row[i_w]) if i_w is not None else None
            p = num(row[i_p]) if i_p is not None else None
            if v is not None and ms_unit:
                v = v * MS2KT
            out.setdefault(row[i_tr].strip().upper(), {}).setdefault(row[i_mem].strip(), {})[tau] = (
                round(la, 1), round(lo, 1), round(v) if v and v > 0 else None, round(p) if p and p > 0 else None)
        except (ValueError, IndexError):
            continue
    return out

def wl_fetch(model, cyc):
    """Weather Lab の1サイクルぶん（全台風）を取得してパース。無ければ None。30分キャッシュ。"""
    def go():
        url = WL_URL.format(model=model, kind="ensemble", ts=cyc.strftime("%Y_%m_%dT%H_00"))
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except requests.RequestException as e:
            print("WeatherLab", model, cyc, repr(e), flush=True)
            return None
        if r.status_code != 200 or r.text.lstrip()[:1] == "<":
            return None
        return wl_parse(r.text)
    if len(_fc_cache) > 40:  # 古いキャッシュを捨てる
        for k, _ in sorted(_fc_cache.items(), key=lambda kv: kv[1][0])[:15]:
            _fc_cache.pop(k, None)
    return _cached(f"wl:{model}:{cyc:%Y%m%d%H}", 1800, go)

def wl_ens(sid):
    """Weather Lab のAIアンサンブル → (ens要素のリスト, 取得状況)。"""
    nn, now = sid[2:], datetime.now(timezone.utc)
    yr = now.year
    base = now.replace(hour=now.hour // 6 * 6, minute=0, second=0, microsecond=0)
    cycles = [base - timedelta(hours=6 * i) for i in range(6)]
    def one(model):
        for cyc in cycles:
            g = wl_fetch(model, cyc)
            if not g:
                continue
            key = next((k for k in g if k in (f"WP{nn}{yr}".upper(), f"{sid}{yr}".upper())), None)
            if key:
                return model, cyc, g[key]
        return model, None, None
    with ThreadPoolExecutor(len(WL_MODELS)) as ex:
        res = list(ex.map(one, WL_MODELS))
    ens, st = [], []
    for model, cyc, tr in res:
        mem = [[[t, *m[t]] for t in sorted(m)] for m in (tr or {}).values()]
        mem = [m for m in mem if len(m) > 1]
        st.append({"name": WL_MODELS[model], "ok": bool(mem), "n": len(mem)})
        if mem:
            ens.append({"key": model, "label": WL_MODELS[model], "init": cyc.strftime("%Y%m%d%H"), "n": len(mem),
                        "members": mem, "mean": mean_track(mem)})
    return ens, st

def _adeck(sid):
    yr = datetime.now(timezone.utc).year
    r = requests.get(f"{RAL}/plots/northwestpacific/{yr}/{sid}{yr}/a{sid}{yr}.dat", headers=UA, timeout=60)
    r.raise_for_status()
    return r.headers.get("Last-Modified", ""), parse_adeck(r.text)

def jma_fc(tc):
    """気象庁の予報円（位置＋風速・気圧）。風速は forecast.json の maximumWind.sustained（m/s）をktに戻して保持。"""
    pts, init = [], ""
    for r in get_json(f"{BOSAI}/{tc}/forecast.json", []) or []:
        if isinstance(r, dict) and r.get("advancedHours") is not None:
            try:
                w = num(((r.get("maximumWind") or {}).get("sustained") or {}).get("m/s"))
                pts.append([int(r["advancedHours"]), float(r["center"][0]), float(r["center"][1]),
                            round(w * MS2KT, 1) if w else None, num(r.get("pressure")) or None])
            except (KeyError, TypeError, ValueError, IndexError): continue
            if r["advancedHours"] == 0: init = re.sub(r"\D", "", str((r.get("validtime") or {}).get("UTC", "")))[:10]
    return {"init": init, "pts": sorted(pts), "is10": True} if len(pts) > 1 else None

def build_forecast(sid, jma=""):
    errs, lm, rows = [], "", []
    try:
        lm, rows = _cached("ad:" + sid, 900, lambda: _adeck(sid))
    except requests.RequestException as e:
        errs.append(f"UCAR RAL: {e}")
    top = datetime.strptime(max(x[0] for x in rows), "%Y%m%d%H") if rows else datetime.now(timezone.utc).replace(tzinfo=None)
    by = {}
    for init, tech, tau, la, lo, v, p in rows:
        if 0 <= tau <= 240: by.setdefault(tech, {}).setdefault(init, {})[tau] = (tau, round(la, 1), round(lo, 1), v, p)
    def last(tech):  # その技術IDの最新の初期時刻の予報（古すぎるものは捨てる）
        d = by.get(tech)
        if not d: return None
        i = max(d)
        if datetime.strptime(i, "%Y%m%d%H") < top - timedelta(hours=48) or len(d[i]) < 2: return None
        return i, [d[i][t] for t in sorted(d[i])]
    models = [{"key": t, "label": lb, "init": g[0], "pts": [list(x) for x in g[1]]} for t, lb in DET.items() if (g := last(t))]
    # --- a-deck のアンサンブル（機関ごとに自動検出）
    fam = {}
    for tech in sorted(by):
        m = ENS_ID.fullmatch(tech)
        if m and (g := last(tech)):
            fam.setdefault(m.group(1), []).append(g)
    ens = []
    for L, mem in fam.items():
        if L not in ENS_LET and len(mem) < 5:  # 未知の頭文字は5本以上そろった時だけ採用（誤検出防止）
            continue
        init = max(g[0] for g in mem)
        cur = [[[x[0], x[1], x[2], x[3], x[4]] for x in g[1]] for g in mem if g[0] == init]
        gm = last(L + "EMN")
        name = ENS_LET.get(L, f"ENS-{L}")
        ens.append({"key": name, "label": name, "init": init, "n": len(cur), "members": cur,
                    "mean": [[x[0], x[1], x[2], x[3], x[4]] for x in gm[1]] if gm and gm[0] == init else mean_track(cur)})
    # --- Weather Lab（AIアンサンブル）
    wl_ok = False
    try:
        wens, wst = wl_ens(sid)
        ens += wens
        wl_ok = bool(wens)
    except Exception as e:  # 取得元が変わっても他のモデルは止めない
        wst = []
        errs.append(f"Weather Lab: {e!r}")
    car = by.get("CARQ"); ana = None
    if car:
        i = max(car); x = car[i][min(car[i])]; ana = {"init": i, "lat": x[1], "lon": x[2], "w": x[3], "p": x[4]}
    jm = jma_fc(jma) if jma else None
    cand = [(m["label"], {x[0]: (x[1], x[2]) for x in m["pts"]}) for m in models]
    cand += [(e["label"] + "平均", {x[0]: (x[1], x[2]) for x in e["mean"]}) for e in ens if e["mean"]]
    if jm: cand.append(("JMA公式", {x[0]: (x[1], x[2]) for x in jm["pts"]}))
    taus = [24, 48, 72, 96, 120]; srows = [[lb, []] for lb, _ in cand]; mx, nn = [], []
    for t in taus:  # 各モデルの予報位置が、全モデルの平均位置から何km離れているか
        ps = [(i, tp[t]) for i, (_, tp) in enumerate(cand) if t in tp]
        col = [None] * len(cand)
        if len(ps) >= 2:
            lo0 = ps[0][1][1]
            la_m = sum(p[1][0] for p in ps) / len(ps)
            lo_m = lo0 + sum((p[1][1] - lo0 + 540) % 360 - 180 for p in ps) / len(ps)
            for i, pos in ps: col[i] = ll_dist(pos, (la_m, lo_m))
        for i, v in enumerate(col): srows[i][1].append(v)
        mx.append(max((v for v in col if v is not None), default=None)); nn.append(len(ps))
    # --- 強さの予報（風速m/s＝10分間平均相当、中心気圧hPa）
    ser = []
    def add_int(label, kind, init, tracks, is10=False):
        r = intensity_rows(tracks, is10, probs=(kind == "ens"))
        if r: ser.append({"label": label, "kind": kind, "init": init, "n": len(tracks), "rows": r})
    if jm: add_int("JMA公式", "det", jm["init"], [{x[0]: (x[3], x[4]) for x in jm["pts"]}], True)
    for m in models: add_int(m["label"], "det", m["init"], [{x[0]: (x[3], x[4]) for x in m["pts"]}])
    for e in ens: add_int(e["label"], "ens", e["init"], [{x[0]: (x[3], x[4]) for x in mem} for mem in e["members"]])
    if not (models or ens or jm):
        raise requests.RequestException("; ".join(errs) or "予報データがありません")
    return {"id": sid, "updated": lm, "analysis": ana, "models": models, "ens": ens, "jma": jm,
            "spread": {"taus": taus, "rows": srows, "max": mx, "n": nn},
            "intensity": {"series": ser},
            "sources": {"adeck": bool(rows), "weatherlab": wst, "weatherlab_ok": wl_ok, "errors": errs,
                        "members": sum(e["n"] for e in ens)}}

@app.get("/api/forecast/storms")
def fc_storms():
    """予報比較の対象（UCARが公開している活動中の西太平洋の台風）。JMAの台風と名前で対応づける。"""
    def go():
        html = requests.get(f"{RAL}/current/", headers=UA, timeout=30).text
        ids = sorted(set(re.findall(r"northwestpacific/\d{4}/(wp\d{2})\d{4}/", html)))
        jm = []
        for t in get_json(f"{BOSAI}/targetTc.json", []) or []:
            tc = t.get("tropicalCyclone")
            if tc: jm.append({"tc": tc, "num": str(t.get("typhoonNumber", "")),
                              "name": _title_en(get_json(f"{BOSAI}/{tc}/specifications.json", []) or [])})
        out = []
        for i in ids:
            m = re.search(r">\s*([^<>]*\(%s\))\s*<" % i.upper(), html)
            label = re.sub(r"\s+", " ", m.group(1)).strip() if m else i.upper()
            out.append({"id": i, "label": label, "jma": next((x for x in jm if x["name"] and x["name"] in label.upper()), None)})
        return sorted(out, key=lambda s: s["id"][2:] >= "90")  # 番号のついた台風を先、invest(90番台)を後ろに
    try: return _cached("fc:storms", 300, go)
    except requests.RequestException as e: raise HTTPException(502, f"台風の一覧を取得できません: {e}")

@app.get("/api/forecast/{sid}")
def fc_detail(sid: str, jma: str = ""):
    if not re.fullmatch(r"wp\d{2}", sid) or not re.fullmatch(r"[A-Za-z0-9]*", jma): raise HTTPException(404, "対象外です")
    try: return _cached(f"fc:{sid}:{jma}", 600, lambda: build_forecast(sid, jma))
    except requests.RequestException as e: raise HTTPException(502, f"予報データを取得できません: {e}")

# 予報は本体ページの「予報」タブに統合した。旧URL(/forecast)は新しいタブへ転送する
@app.get("/forecast")
def forecast_page():
    return RedirectResponse("/#fc")

# ---------------- 画面 ----------------
PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0a1120">
<link rel="manifest" href="/manifest.webmanifest"><link rel="icon" href="/icon.svg"><link rel="apple-touch-icon" href="/icon.svg">
<title>台風データベース</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a1120;--s1:#101a30;--s2:#17233f;--line:#22314f;--ink:#e9eefb;--sub:#8b9bbb;--acc:#38bdf8;--r:14px;--tb:0px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{overscroll-behavior:none}
[hidden]{display:none!important}
body{margin:0;height:100dvh;display:grid;grid-template-columns:400px 1fr;background:var(--bg);color:var(--ink);font:14px/1.5 "Zen Kaku Gothic New",system-ui,sans-serif;overflow:hidden}
input,select,button{font:inherit;color:var(--ink);touch-action:manipulation}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}

/* ---- 画面切り替えタブ（PC: 左パネル上部 / スマホ: 画面下部） ---- */
#tabs{position:fixed;left:0;top:0;width:400px;height:56px;z-index:1700;display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:7px 10px;background:var(--s1);border-bottom:1px solid var(--line);border-right:1px solid var(--line)}
#tabs button{border:0;border-radius:10px;background:transparent;color:var(--sub);display:flex;flex-direction:column;align-items:center;justify-content:center;line-height:1.25;cursor:pointer}
#tabs b{font-size:14px}#tabs small{font-size:10px;opacity:.85}
#tabs button:hover{background:var(--s2)}
#tabs button[aria-selected=true]{background:var(--acc);color:#04121f}

/* ---- 左パネル（3つの画面が同じ場所に入る） ---- */
aside{display:flex;flex-direction:column;min-height:0;background:var(--s1);border-right:1px solid var(--line);padding-top:56px}
.sec{display:none;flex-direction:column;min-height:0;flex:1}
body[data-tab=fc] #s-fc,body[data-tab=db] #s-db,body[data-tab=st] #s-st{display:flex}
.shd,.grab{display:none}
.pad{padding:12px 14px 6px;display:grid;gap:8px;flex:none}
.scroll{flex:1;min-height:0;overflow:auto;overscroll-behavior:contain;padding:2px 14px 18px;-webkit-overflow-scrolling:touch}
.sh{margin:16px 0 6px;font-size:12px;font-weight:700;color:var(--sub);letter-spacing:.04em}
.note{color:var(--sub);font-size:11px;line-height:1.6}
.btn{height:38px;padding:0 14px;border:0;border-radius:10px;background:var(--s2);font-size:13px;cursor:pointer;white-space:nowrap}
.btn.sm{height:34px;padding:0 12px}
.btn.on{background:var(--acc);color:#04121f;font-weight:700}
.btn:active{filter:brightness(1.25)}
.btns{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.in,.f input:not([type=checkbox]),.f select,.sel select,#oStep{background:var(--s2);border:1px solid transparent;border-radius:10px;padding:9px 12px}
.f input:not([type=checkbox]):focus,.f select:focus{border-color:var(--acc);outline:0}
.pill{font-size:11px;color:var(--sub);border:1px solid var(--line);border-radius:99px;padding:2px 10px;white-space:nowrap}
.pill b{color:var(--acc);font-weight:700}
.chk{display:flex;align-items:center;gap:8px;font-size:13px;margin-top:10px}

/* ---- 過去の台風（検索・一覧） ---- */
header{padding:12px 14px 6px;display:grid;gap:10px;flex:none}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input:not([type=checkbox]),.f select{width:100%}
.f .wide,.f details{grid-column:span 2}
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip,#dir,#reset{background:var(--s2);border:1px solid transparent;border-radius:99px;padding:5px 12px;font-size:12px;cursor:pointer}
.chip.on{background:var(--acc);color:#04121f;font-weight:700}
.sort{display:grid;grid-template-columns:1fr auto;gap:8px}
#dir{border-radius:10px;padding:0 14px;font-size:13px}
summary{cursor:pointer;color:var(--sub);font-size:13px;padding:2px 0}
.f2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.f label{font-size:12px;color:var(--sub);display:flex;flex-direction:column;gap:3px}
.f label.chk{flex-direction:row;align-items:center;gap:6px;margin-top:0}
#reset{border-radius:10px;grid-column:span 2}
.pl2{display:grid;gap:8px;margin-top:8px}
.sw{display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer}
#meta{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:2px 16px 8px;flex:none}
#count{color:var(--sub);font-size:12px;transition:opacity .2s}#count.busy{opacity:.5}
#list{flex:1;overflow:auto;overscroll-behavior:contain;margin:0;padding:0 10px 10px;list-style:none;display:grid;gap:6px;align-content:start;-webkit-overflow-scrolling:touch;min-height:0}
#list li{display:flex;gap:12px;align-items:center;padding:10px 12px;border-radius:12px;background:transparent;cursor:pointer;border:1px solid transparent}
#list li:hover{background:var(--s2)}#list li.on{background:var(--s2);border-color:var(--acc)}
.bar{width:4px;align-self:stretch;border-radius:4px;flex:none}
.t{flex:1;min-width:0}.t b{display:block;font-size:14px}
.t span{color:var(--sub);font-size:12px}
.t em{font-style:normal;font-size:10px;margin-left:6px;padding:1px 6px;border-radius:99px;background:#f0803c22;color:#f0a06c;vertical-align:1px}
.m{height:3px;background:var(--line);border-radius:3px;margin-top:6px}.m i{display:block;height:100%;border-radius:3px}
.v{text-align:right;line-height:1.2}.v strong{font:700 20px "Space Grotesk",sans-serif}.v small{display:block;color:var(--sub);font-size:11px}
footer{padding:8px 16px;border-top:1px solid var(--line);color:var(--sub);font-size:11px;flex:none}

/* ---- 予報 ---- */
.sel{display:flex;gap:8px}.sel select{flex:1;min-width:0;height:44px;font-size:16px}
.rw{display:flex;align-items:center;gap:10px}.rw .lb{font-size:12px;color:var(--sub);flex:none}
#tau{flex:1;min-width:0;height:32px;accent-color:var(--acc)}#tv{min-width:56px;text-align:right;font:700 18px "Space Grotesk",system-ui,sans-serif}
#pos{display:grid;gap:6px}
.pr{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:center;background:var(--s2);border-radius:10px;padding:6px 12px 6px 8px}
.pn b{display:block;font-size:13px}.pn small{display:block;color:var(--sub);font-size:11px;font-variant-numeric:tabular-nums}
.iv{text-align:right;line-height:1.2}.iv strong{font:700 20px "Space Grotesk",system-ui,sans-serif}.iv small{display:block;color:var(--sub);font-size:11px}
.rg{display:inline-block;width:12px;height:12px;margin:5px;border-radius:50%;background:var(--f);box-shadow:0 0 0 2px var(--s2),0 0 0 5px var(--r)}
.rg.e,.mk.e{border-radius:3px;transform:rotate(45deg) scale(.85)}
.gh{display:flex;justify-content:space-between;align-items:center;width:100%;margin:12px 0 4px;padding:0;border:0;background:none;color:var(--sub);font-size:11px;cursor:pointer}
.srcs{display:flex;flex-wrap:wrap;gap:6px}
.src{border:1px solid var(--c);background:transparent;border-radius:99px;min-height:36px;padding:0 12px;font-size:13px;display:inline-flex;gap:8px;align-items:center;cursor:pointer}
.src small{color:var(--sub);font-size:11px}
.src.off{opacity:.35}.src.off .ln{opacity:.4}
.ln{display:inline-block;width:18px;height:0;border-top:3px solid var(--c)}.ln.d{border-top-style:dashed}
.mkw{background:none;border:0;display:flex;align-items:center;justify-content:center}
.mk{display:block;width:var(--s);height:var(--s);border-radius:50%;background:var(--f);box-shadow:0 0 0 2px #0a1120,0 0 0 5px var(--r)}
.mk.cur{box-shadow:0 0 0 2px #0a1120,0 0 0 5px var(--r),0 0 0 7px #fff}
.mlab2{background:none;border:0}
.mlab2 span{position:absolute;left:12px;top:-9px;padding:0 5px;border-radius:5px;background:rgba(10,17,32,.88);color:#fff;font:700 11px system-ui,sans-serif;white-space:nowrap}

/* ---- 統計 ---- */
.cond{display:grid;gap:6px;justify-items:start;margin-top:8px}
.kpi{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:12px 0}
.kpi div{background:var(--s2);border-radius:10px;padding:8px 12px}.kpi .w2{grid-column:span 2}
.kpi small{display:block;color:var(--sub);font-size:11px}
.kpi strong{font:700 20px "Space Grotesk",system-ui,sans-serif}.kpi strong span{font-size:11px;font-weight:400;color:var(--sub);margin-left:3px}
.bs{width:100%;height:90px;display:block;background:var(--s2);border-radius:10px}.ax{display:flex;justify-content:space-between;color:var(--sub);font-size:11px;margin-top:2px}
.cb{display:flex;height:14px;border-radius:7px;overflow:hidden;background:var(--s2)}.cb i{display:block}
.lg{display:flex;flex-wrap:wrap;gap:4px 12px;margin-top:6px;font-size:12px}.lg i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:4px}

/* ---- 地図まわり ---- */
main{position:relative;min-height:0}#map{height:100%;background:#0b1522;z-index:0}
.card{position:absolute;z-index:500;background:rgba(16,26,48,.92);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:var(--r)}
#info{right:14px;top:14px;padding:12px 14px;width:320px;max-width:calc(100% - 28px)}
body[data-tab=fc] #info,body[data-tab=st] #info{display:none}
.hd{display:flex;align-items:flex-start;gap:8px}.hd h2{flex:1;margin:0;font-size:17px;min-width:0}
#info .sub{color:var(--sub);font-size:12px}
.g{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}
.g div{background:var(--s2);border-radius:10px;padding:6px 10px}
.g small{display:block;color:var(--sub);font-size:11px}.g strong{font:700 17px "Space Grotesk",sans-serif}.g strong span{font-size:11px;font-weight:400;color:var(--sub)}
.st{display:none;flex-wrap:wrap;align-items:baseline;gap:2px 12px;margin-top:6px;font-size:13px}.st b{font-size:14px}
.pl{display:flex;align-items:center;gap:10px;margin-top:10px}
#play{min-width:64px;background:var(--acc);color:#04121f;font-weight:700}
.chart{flex:1;min-width:0;position:relative}
.chart svg{display:block;width:100%;height:40px}
#scrub{width:100%;margin:0;accent-color:var(--acc);height:20px}
#rd{font-size:12px;color:var(--sub);margin-top:4px;font-variant-numeric:tabular-nums}
.acts{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}
.acts .btn{height:42px;padding:0}.mo{display:none}
#fit{position:absolute;z-index:600;left:14px;top:14px;height:38px;padding:0 14px;border:1px solid var(--line);border-radius:12px;background:rgba(16,26,48,.92);font-size:13px;cursor:pointer}
#legend{left:14px;bottom:24px;padding:8px 12px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;border-radius:2px;display:inline-block;flex:none}
#legend i.dot{width:var(--s);height:var(--s);border-radius:50%}
#legend .lgh{color:var(--sub);font-size:11px;margin-top:4px}
#fab{display:none}
dialog{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:18px;width:min(560px,94vw);max-height:86dvh;padding:16px;overflow:auto}
dialog::backdrop{background:#000b}dialog h2{margin:0;font-size:17px}
table{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}th,td{padding:6px 4px;text-align:right;border-bottom:1px solid var(--line)}th:first-child,td:first-child{text-align:left}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:var(--s2);border:1px solid var(--line);padding:8px 16px;border-radius:99px;font-size:13px;z-index:2000;opacity:0;pointer-events:none;transition:opacity .2s}
#toast.on{opacity:1}
#bd{display:none}
.leaflet-tooltip{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:8px}
.leaflet-control-attribution{font-size:9px}

/* ---- スマホ ---- */
@media(max-width:760px){
 :root{--tb:calc(58px + env(safe-area-inset-bottom))}
 body{display:block}
 #tabs{left:0;right:0;top:auto;bottom:0;width:auto;height:var(--tb);padding:6px 10px calc(6px + env(safe-area-inset-bottom));border-right:0;border-bottom:0;border-top:1px solid var(--line)}
 main{position:fixed;inset:0 0 var(--tb) 0}
 #bd{display:block;position:fixed;inset:0 0 var(--tb) 0;z-index:1400;background:rgba(0,0,0,.5);opacity:0;pointer-events:none;transition:opacity .28s}
 body.lst #bd{opacity:1;pointer-events:auto}
 aside{position:fixed;left:0;right:0;bottom:var(--tb);height:calc(86dvh - var(--tb));z-index:1500;padding-top:0;border:0;border-top:1px solid var(--line);border-radius:20px 20px 0 0;box-shadow:0 -12px 40px #000a;transform:translateY(115%);transition:transform .3s cubic-bezier(.2,.8,.2,1);overscroll-behavior:contain}
 body.lst aside{transform:none}
 body[data-tab=fc] aside{height:auto;max-height:46dvh;transform:none;z-index:900;box-shadow:0 -8px 30px #0008}
 .grab{display:block;height:20px;position:relative;flex:none}.grab::before{content:"";position:absolute;left:50%;top:9px;width:44px;height:5px;margin-left:-22px;border-radius:3px;background:var(--line)}
 .shd{display:flex;justify-content:space-between;align-items:center;padding:0 14px 8px;flex:none}.shd b{font-size:16px}
 body[data-tab=fc] .grab,body[data-tab=fc] .shd{display:none}
 #s-fc .pad{padding-top:14px}
 header{max-height:52%;overflow:auto;padding:0 12px 8px;gap:10px;overscroll-behavior:contain}
 .f input:not([type=checkbox]),.f select{font-size:16px;padding:12px 14px;min-height:46px}  /* iOSの自動ズーム防止 */
 .chips{flex-wrap:nowrap;overflow-x:auto;margin:0 -12px;padding:0 12px;scrollbar-width:none}.chips::-webkit-scrollbar{display:none}
 .chip{flex:none;min-height:40px;padding:0 16px;font-size:14px}
 #dir{min-width:56px}#reset{min-height:46px}summary{padding:10px 0;font-size:14px}
 #meta{padding:4px 14px 8px}
 #list{padding:0 8px 16px;gap:4px}#list li{padding:14px 12px;min-height:64px}.t b{font-size:15px}
 #info{left:0;right:0;bottom:0;top:auto;width:auto;max-width:none;border-radius:20px 20px 0 0;border-width:1px 0 0;padding:12px 14px 12px}
 .g{display:none}.st{display:flex}.mo{display:block}.acts{grid-template-columns:repeat(5,1fr)}
 #play{height:44px}.pl{margin-top:8px}.chart svg{height:26px}#scrub{height:28px}
 .leaflet-control-zoom,.leaflet-control-attribution{display:none}
 #legend{left:10px;top:10px;bottom:auto;display:flex;gap:10px;padding:6px 10px;font-size:11px;max-width:calc(100% - 100px);overflow-x:auto;scrollbar-width:none;align-items:center}
 #legend div{flex:none;gap:4px}#legend .lgh{margin-top:0}
 #fit{left:auto;right:10px;top:10px;height:40px}
 #fab{position:absolute;z-index:600;left:50%;bottom:16px;transform:translateX(-50%);padding:0 26px;height:48px;border:0;border-radius:99px;background:var(--acc);color:#04121f;font-weight:700;font-size:15px;box-shadow:0 6px 20px #0008}
 body[data-tab=db] #fab{display:block}
 #info:not([hidden])~#fab{display:none}
 #toast{top:64px;bottom:auto}
}
@media(max-width:760px) and (max-height:520px){.st,.chart svg{display:none}}
</style></head><body data-tab="db">
<div id="bd"></div>
<nav id="tabs" role="tablist" aria-label="画面の切り替え">
 <button type="button" role="tab" data-t="fc" aria-selected="false"><b>予報</b><small>現在の台風</small></button>
 <button type="button" role="tab" data-t="db" aria-selected="true"><b>過去の台風</b><small>検索・進路</small></button>
 <button type="button" role="tab" data-t="st" aria-selected="false"><b>統計</b><small>集計</small></button>
</nav>
<aside>
 <div class="grab" id="grab"></div>
 <div class="shd"><b id="shT">台風を探す</b><button type="button" class="btn sm" id="cl">閉じる</button></div>

 <!-- 画面1: 予報 -->
 <section id="s-fc" class="sec">
  <div class="pad">
   <div class="sel"><select id="fsel" aria-label="予報を見る台風"></select><button type="button" class="btn" id="frf">更新</button></div>
   <div class="rw"><span class="lb">予報時間</span><input id="tau" type="range" min="0" max="120" step="6" value="48" aria-label="予報時間"><b id="tv">+48h</b></div>
   <div id="fnote" class="note"></div>
  </div>
  <div class="scroll">
   <h3 class="sh">選んだ時間の予報（位置と強さ）</h3><div id="pos"></div>
   <h3 class="sh">強さの表示</h3>
   <div class="btns"><button type="button" class="btn" id="oInlay">線にも強さの色</button><button type="button" class="btn" id="oNum">風速を数字で</button>
    <select id="oStep" aria-label="印の間隔"><option value="12">12時間ごと</option><option value="24" selected>24時間ごと</option></select></div>
   <div class="note" style="margin-top:6px">印の中の色と大きさが強さ、外側の輪の色が予報の出どころ（モデル）です。円は単独モデル、菱形はアンサンブル平均。</div>
   <h3 class="sh">ソース（タップで表示・非表示）</h3><div id="fchips"></div>
   <label class="chk"><input type="checkbox" id="mem" checked>アンサンブルの各メンバーの進路も表示</label>
   <h3 class="sh">詳しく見る</h3>
   <div class="btns"><button type="button" class="btn" id="tbl">位置のばらつき表</button><button type="button" class="btn" id="int">強さ予報のグラフ</button></div>
   <div id="fsrc" class="note" style="margin-top:16px"></div>
  </div>
 </section>

 <!-- 画面2: 過去の台風 -->
 <section id="s-db" class="sec">
  <header>
   <form class="f" id="f" onsubmit="return false">
    <input class="wide" name="name" type="search" enterkeyhint="search" autocomplete="off" placeholder="名前・号数で検索（例: SURIGAE / 15号）">
    <div class="chips wide">
     <button type="button" class="chip" data-w="">全ての強さ</button><button type="button" class="chip" data-w="33">強い〜</button>
     <button type="button" class="chip" data-w="44">非常に強い〜</button><button type="button" class="chip" data-w="54">猛烈な</button></div>
    <div class="sort wide"><select name="sort"><option value="number">号数順</option><option value="date">発生日順</option><option value="wind">最大風速順</option><option value="pres">最低気圧順</option><option value="days">継続日数順</option><option value="name">名前順</option></select>
     <button type="button" id="dir" title="昇順・降順">降順</button><input type="hidden" name="order" value="desc"></div>
    <details id="dPl"><summary>地点から探す（近くを通った台風）</summary><div class="pl2">
     <div class="btns"><button type="button" class="btn" id="tNear">現在地から</button><button type="button" class="btn" id="tPick">地図で指定</button><button type="button" class="btn" id="tClr" hidden>指定を解除</button></div>
     <label>近くを通った範囲<select name="km"><option value="100">100km以内</option><option value="300" selected>300km以内</option><option value="500">500km以内</option><option value="1000">1000km以内</option></select></label>
     <div class="note" id="nearTxt">地点を指定すると、その近くを通った台風だけを表示します。</div></div></details>
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
   </form>
   <label class="sw"><input type="checkbox" id="ovSw"><span>検索結果の進路を全て地図に重ねる</span></label>
  </header>
  <div id="meta"><span id="count"></span><span class="pill" id="upd"></span></div>
  <ul id="list"></ul>
  <footer>出典: 気象庁（ベストトラック／位置表。IBTrACS経由）。風速は10分平均、時刻は日本時間。「速報」は速報値で後日修正されます。地図タイル: Esri。</footer>
 </section>

 <!-- 画面3: 統計 -->
 <section id="s-st" class="sec"><div class="scroll" id="stBody"></div></section>
</aside>
<main><div id="map"></div><button id="fit" type="button">全体を表示</button><div id="info" class="card" hidden></div><button id="fab" type="button">台風を探す</button><div id="legend" class="card"></div></main>
<dialog id="dlg"></dialog>
<div id="toast"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
// ================= 共通 =================
const TABS={fc:"予報",db:"過去の台風",st:"統計"};
const $=s=>document.querySelector(s);
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const mobile=()=>matchMedia("(max-width:760px)").matches;
const store={get(){try{return JSON.parse(localStorage.getItem("tf")||"{}")}catch(e){return{}}},set(v){try{localStorage.setItem("tf",JSON.stringify(v))}catch(e){}}};
const tabMem={get(){try{return localStorage.getItem("tf_tab")}catch(e){return null}},set(v){try{localStorage.setItem("tf_tab",v)}catch(e){}}};
function toast(m){const t=$("#toast");t.textContent=m;t.classList.add("on");clearTimeout(toast.t);toast.t=setTimeout(()=>t.classList.remove("on"),1800)}
// 強さの階級（気象庁・10分間平均風速 m/s）。過去の台風・予報で同じ色を使う
const CLS=[[54,"猛烈な","#ff4d7d"],[44,"非常に強い","#ff8a3d"],[33,"強い","#ffd24a"],[17,"台風","#4fc3f7"],[0,"熱帯低気圧","#64789a"]];
const UNK=[null,"不明","#3d4f66"];
const FCLS=[[54,"猛烈な","#ff4d7d",19],[44,"非常に強い","#ff8a3d",16],[33,"強い","#ffd24a",13],[17.2,"台風","#4fc3f7",10],[0,"熱帯低気圧","#64789a",8]];  // 4番目=予報マーカーの大きさ(px)
const UNKF=[null,"不明","#3d4f66",8];
const initHash=location.hash.slice(1);
const initTab=TABS[initHash]?initHash:(initHash?"db":(tabMem.get()||"db"));
let curSid=TABS[initHash]?"":initHash;   // 過去の台風タブで表示中の台風
let tab="";

const map=L.map("map",{worldCopyJump:true,zoomControl:false}).setView([25,135],4);
L.control.zoom({position:"bottomright"}).addTo(map);
const ESRI="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/";
L.tileLayer(ESRI+"World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",{attribution:"Tiles © Esri — Esri, DeLorme, NAVTEQ | 気象庁 / IBTrACS (NOAA) / UCAR RAL / Google DeepMind",maxZoom:12}).addTo(map);
map.createPane("labels").style.zIndex=450;map.getPane("labels").style.pointerEvents="none";
L.tileLayer(ESRI+"World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}",{pane:"labels",maxZoom:12}).addTo(map);
addEventListener("resize",()=>map.invalidateSize());

// ================= 画面の切り替え =================
const views={};
const dbLayerList=[];   // 過去の台風の地図レイヤー（予報タブでは外す）
function dbLayers(on){dbLayerList.forEach(l=>on?l.addTo(map):l.remove())}
function legend(){
  const el=$("#legend");
  el.innerHTML=tab=="fc"
    ?`<div class="lgh">強さ（印の中）</div>`+FCLS.map(c=>`<div><i class="dot" style="--s:${c[3]}px;background:${c[2]}"></i>${c[1]}</div>`).join("")+`<div class="lgh">外側の輪 = モデル</div>`
    :[...CLS,UNK].map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join("");
}
function setTab(t){
  if(!TABS[t])t="db";
  const pg=tab?(tab=="fc"?"fc":"db"):"",g=t=="fc"?"fc":"db";
  tab=t;document.body.dataset.tab=t;tabMem.set(t);
  document.querySelectorAll("#tabs button").forEach(b=>b.setAttribute("aria-selected",String(b.dataset.t==t)));
  $("#shT").textContent=t=="st"?"統計":"台風を探す";
  if(g!=pg){
    if(pg)views[pg]={c:map.getCenter(),z:map.getZoom()};
    if(g=="fc"){dbLayers(false);FC.show()}else{FC.hide();dbLayers(true)}
    map.invalidateSize();
    const v=views[g];
    if(v)map.setView(v.c,v.z,{animate:false});
    else if(g=="fc")FC.fit();
    else if(cur)map.fitBounds(cur.bounds,fitOpt());
  }
  if(mobile()){if(t=="st")openPanel();else closePanel()}
  if(t=="st")loadStats();
  legend();
  history.replaceState(null,"",t=="db"?(curSid?"#"+curSid:location.pathname+location.search):"#"+t);
}
$("#tabs").onclick=e=>{const b=e.target.closest("button");if(!b)return;const t=b.dataset.t;
  if(t==tab&&mobile()&&t!="fc"){openPanel();return}
  setTab(t)};
$("#fit").onclick=()=>{if(tab=="fc")FC.fit();else if(cur)map.fitBounds(cur.bounds,fitOpt())};

// ================= 過去の台風 =================
const K=w=>w==null?UNK:CLS.find(c=>w>=c[0]),col=w=>K(w)[2],cls=w=>K(w)[1];
const spd=w=>w==null?"-":`${w}m/s`;
const jst=(s,full)=>{const d=new Date(new Date(s.replace(" ","T")+"Z").getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");
  return `${full?d.getUTCFullYear()+"/":""}${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}時`};
const f=$("#f"),chips=document.querySelectorAll(".chip[data-w]");
const km=(a,b)=>{const r=Math.PI/180,dl=(b[1]-a[1])*r,p1=a[0]*r,p2=b[0]*r;
  return 6371*Math.acos(Math.min(1,Math.sin(p1)*Math.sin(p2)+Math.cos(p1)*Math.cos(p2)*Math.cos(dl)))};
const layer=L.layerGroup(),nearL=L.layerGroup(),ovL=L.layerGroup();dbLayerList.push(layer,nearL,ovL);
map.createPane("ov").style.zIndex=380;
let first=true,cur=null,playT=null,near=null,pick=false,ovOn=false,lastP=new URLSearchParams();

// 一覧パネル（スマホ: 下から出るシート）
function openPanel(){document.body.classList.add("lst");setTimeout(()=>{const l=$("#list li.on");l&&reveal(l)},60)}
function closePanel(){document.body.classList.remove("lst")}
$("#cl").onclick=closePanel;$("#fab").onclick=openPanel;$("#bd").onclick=closePanel;
{let sy=null,dy=0;const a=$("aside");   // シートを下へスワイプして閉じる
 for(const el of [$("#grab"),$(".shd")]){
  el.addEventListener("touchstart",e=>{sy=e.touches[0].clientY;dy=0;a.style.transition="none"},{passive:true});
  el.addEventListener("touchmove",e=>{if(sy==null)return;dy=Math.max(0,e.touches[0].clientY-sy);a.style.transform=`translateY(${dy}px)`},{passive:true});
  el.addEventListener("touchend",()=>{if(sy==null)return;sy=null;a.style.transition="";a.style.transform="";if(dy>80)closePanel()})}}
f.name.addEventListener("keydown",e=>{if(e.key=="Enter")e.target.blur()});  // 検索確定でキーボードを閉じる
function reveal(li){const L2=$("#list"),a=li.getBoundingClientRect(),b=L2.getBoundingClientRect();
  if(a.top<b.top)L2.scrollTop-=b.top-a.top+8;else if(a.bottom>b.bottom)L2.scrollTop+=a.bottom-b.bottom+8}
const barH=()=>mobile()&&!$("#info").hidden?$("#info").offsetHeight:0;
const fitOpt=()=>mobile()?{paddingTopLeft:[16,54],paddingBottomRight:[16,barH()+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[360,40]};

let jmaAt="";
async function dbInit(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--){f.year_from.add(new Option(i+"年",i));f.year_to.add(new Option(i+"年",i))}
  for(let m=1;m<=12;m++) f.month.add(new Option(m+"月",m));
  const sv=store.get();for(const k in sv) if(f[k]&&f[k].type!="checkbox") f[k].value=sv[k];  // 前回の絞り込みを復元
  f.named.checked=sv.named==="true";
  jmaAt=y.jma_at||"";if(jmaAt) $("#upd").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
  $("#reset").onclick=()=>{f.reset();f.order.value="desc";near=null;load()};
  $("#dir").onclick=()=>{f.order.value=f.order.value=="desc"?"asc":"desc";load()};
  chips.forEach(c=>c.onclick=()=>{f.wind_min.value=c.dataset.w;f.wind_max.value="";load()});
  f.addEventListener("input",e=>{if(e.isComposing)return;clearTimeout(dbInit.t);dbInit.t=setTimeout(load,250)});
  f.addEventListener("compositionend",()=>{clearTimeout(dbInit.t);dbInit.t=setTimeout(load,250)});  // 日本語入力の確定後に検索
  load();
}
let ctl;
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year_from","year_to","month","wind_min","wind_max","pres_max","days_min","sort","order","limit"]) if(f[k].value) p.set(k,f[k].value);
  if(f.named.checked) p.set("named","true");
  store.set(Object.fromEntries(p));
  if(near)p.set("near",`${near[0]},${near[1]},${f.km.value}`);
  lastP=p;drawNear();if(ovOn)drawOverlay(p);if(tab=="st")loadStats();
  chips.forEach(c=>c.classList.toggle("on",c.dataset.w===f.wind_min.value&&!f.wind_max.value));
  $("#dir").textContent=f.order.value=="desc"?"降順":"昇順";
  ctl&&ctl.abort();ctl=new AbortController();$("#count").classList.add("busy");  // 古いリクエストを破棄
  try{
    const {total,rows}=await (await fetch("/api/typhoons?"+p,{signal:ctl.signal})).json();
    $("#count").classList.remove("busy");
    $("#count").textContent=total?`${total}件`+(total>rows.length?`中 ${rows.length}件を表示（件数を増やすか条件を絞ってください）`:""):"該当なし。条件を変えてください";
    $("#list").innerHTML=rows.map(r=>`<li data-sid="${esc(r.sid)}"><span class="bar" style="background:${col(r.max_wind)}"></span>
     <div class="t"><b>${esc(r.title)}${r.prov?"<em>速報</em>":""}</b><span>${jst(r.start_time,1)}〜 ／ ${r.days}日</span>
     <div class="m"><i style="width:${Math.min(100,(r.max_wind||0)/0.67)}%;background:${col(r.max_wind)}"></i></div></div>
     <div class="v"><strong>${r.max_wind??"-"}</strong><small>m/s</small><small>${r.min_pres??"-"}hPa</small></div></li>`).join("");
    const on=curSid&&$(`#list li[data-sid="${CSS.escape(curSid)}"]`);if(on)on.classList.add("on");
    if(first){first=false;const s=curSid||(rows[0]&&rows[0].sid);if(s)show(s)}  // 共有リンク or 最新の台風を自動表示
  }catch(e){if(e.name!="AbortError"){$("#count").classList.remove("busy");$("#count").textContent="読み込みに失敗しました。再読み込みしてください"}}
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li){if(mobile())closePanel();show(li.dataset.sid,li)}});
function nav(d){const a=[...document.querySelectorAll("#list li")],i=a.findIndex(x=>x.classList.contains("on")),n=a[i+d];if(n)show(n.dataset.sid,n)}
addEventListener("keydown",e=>{if(tab!="db"||/INPUT|SELECT|TEXTAREA/.test(e.target.tagName))return;
  if(e.key=="ArrowDown"||e.key=="j"){e.preventDefault();nav(1)}else if(e.key=="ArrowUp"||e.key=="k"){e.preventDefault();nav(-1)}else if(e.key==" "&&cur){e.preventDefault();play()}});
// 進行アニメーション
function setPos(i){if(!cur)return;cur.i=i;const p=cur.pts[i];cur.mk.setLatLng(cur.ll[i]);
  $("#scrub").value=i;$("#rd").textContent=`${jst(p.time,1)} ・ ${spd(p.wind)} ・ ${p.pres??"-"}hPa`;
  const c=$("#cur");if(c){c.setAttribute("x1",cur.xs[i]);c.setAttribute("x2",cur.xs[i])}
  if(tab!="db")return;
  const pt=map.latLngToContainerPoint(cur.ll[i]),s=map.getSize(),bh=barH(),rx=mobile()?0:350,top=mobile()?50:20;
  if(pt.x<24||pt.x>s.x-rx-24||pt.y<top||pt.y>s.y-bh-24)map.panBy([pt.x-(s.x-rx)/2,pt.y-(top+(s.y-bh-top)/2)])}
function stop(){clearInterval(playT);playT=null;const b=$("#play");if(b)b.textContent="再生"}
function play(){if(!cur)return;if(playT)return stop();$("#play").textContent="停止";if(cur.i>=cur.pts.length-1)setPos(0);
  playT=setInterval(()=>{if(cur.i>=cur.pts.length-1)return stop();setPos(cur.i+1)},Math.max(60,Math.min(250,6000/cur.pts.length)))}
async function share(){const u=location.origin+location.pathname+"#"+curSid,t=$("#info h2").textContent;
  try{if(navigator.share)await navigator.share({title:t,url:u});else{await navigator.clipboard.writeText(u);toast("リンクをコピーしました")}}catch(e){}}
function csv(){if(!cur)return toast("台風を選択してください");
  const nm=($("#info h2").firstChild.textContent||"typhoon").replace(/[\\/:*?"<>|\s]+/g,"_");
  const rows=["time_utc,lat,lon,wind_ms,pres_hpa",...cur.pts.map(p=>[p.time,p.lat,p.lon,p.wind??"",p.pres??""].join(","))];
  const a=document.createElement("a");a.href=URL.createObjectURL(new Blob(["\ufeff"+rows.join("\n")],{type:"text/csv"}));a.download=nm+".csv";a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1e3)}
$("#info").addEventListener("click",e=>{const b=e.target.closest("[data-a]");if(!b)return;
  ({prev:()=>nav(-1),next:()=>nav(1),share,csv,play,list:openPanel,close:()=>{$("#info").hidden=true;stop()}})[b.dataset.a]()});
let seq=0;
async function show(sid,li,keep){
  const my=++seq;stop();curSid=sid;
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));
  li=li||document.querySelector(`#list li[data-sid="${CSS.escape(sid)}"]`);
  if(li){li.classList.add("on");if(!keep)reveal(li)}
  if(tab=="db")history.replaceState(null,"","#"+sid);
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
  i.innerHTML=`<div class="hd"><h2>${esc(t.title)}${t.prov?'<span class="pill" style="margin-left:8px">速報値</span>':""}</h2><button type="button" class="btn sm" data-a="close">閉じる</button></div>
   <div class="sub">${jst(t.start_time,1)} 〜 ${jst(t.end_time,1)}（日本時間）${dist?` ・ 約${dist.toLocaleString()}km`:""}</div>
   <div class="g"><div><small>最大風速</small><strong>${t.max_wind??"-"}<span> m/s</span></strong></div>
   <div><small>強さ</small><strong style="color:${col(t.max_wind)}">${cls(t.max_wind)}</strong></div>
   <div><small>最低気圧</small><strong>${t.min_pres??"-"}<span> hPa</span></strong></div>
   <div><small>継続</small><strong>${t.days}<span> 日</span></strong></div></div>
   <div class="st"><b style="color:${col(t.max_wind)}">${cls(t.max_wind)}</b><span>最大 ${spd(t.max_wind)}</span><span>${t.min_pres??"-"}hPa</span><span>${t.days}日</span></div>
   <div class="pl"><button type="button" class="btn" id="play" data-a="play">再生</button><div class="chart"><div id="rd"></div>
    <svg viewBox="0 0 ${W} 40" preserveAspectRatio="none"><polyline points="${wl}" fill="none" stroke="${col(t.max_wind)}" stroke-width="2" vector-effect="non-scaling-stroke"/>
    <line id="cur" x1="0" x2="0" y1="0" y2="40" stroke="#fff" stroke-width="1" vector-effect="non-scaling-stroke"/></svg>
    <input id="scrub" type="range" min="0" max="${Math.max(0,n-1)}" value="0" aria-label="時刻"></div></div>
   <div class="acts"><button type="button" class="btn mo" data-a="list">一覧</button><button type="button" class="btn" data-a="prev">前へ</button><button type="button" class="btn" data-a="next">次へ</button><button type="button" class="btn" data-a="share">共有</button><button type="button" class="btn" data-a="csv">CSV</button></div>`;
  $("#scrub").oninput=e=>{stop();setPos(+e.target.value)};
  if(!keep&&tab=="db")map.fitBounds(cur.bounds,fitOpt());
  setPos(0);
}
// ---- 地点から探す / 重ね表示 ----
function drawNear(){nearL.clearLayers();$("#tNear").classList.toggle("on",!!near);$("#tClr").hidden=!near;
  $("#nearTxt").textContent=near?`北緯${near[0].toFixed(1)}度・東経${near[1].toFixed(1)}度から ${f.km.value}km 以内を通った台風を表示中`:"地点を指定すると、その近くを通った台風だけを表示します。";
  if(near){$("#dPl").open=true;L.circle(near,{radius:f.km.value*1000,color:"#38bdf8",weight:1,fillOpacity:.07,interactive:false}).addTo(nearL);
    L.circleMarker(near,{radius:5,color:"#fff",weight:2,fillColor:"#38bdf8",fillOpacity:1,interactive:false}).addTo(nearL)}}
function setNear(la,lo){near=la==null?null:[+la.toFixed(3),+lo.toFixed(3)];load();
  if(near)map.fitBounds(L.circle(near,{radius:f.km.value*1000}).getBounds(),fitOpt())}
$("#tNear").onclick=()=>{if(near)return setNear(null);
  if(!navigator.geolocation)return toast("位置情報を使えません");toast("現在地を取得中…");
  navigator.geolocation.getCurrentPosition(p=>setNear(p.coords.latitude,p.coords.longitude),()=>toast("現在地を取得できません。「地図で指定」をお試しください"),{timeout:10000})};
$("#tClr").onclick=()=>setNear(null);
$("#tPick").onclick=()=>{pick=true;toast("地図をタップして地点を指定");if(mobile())closePanel()};
map.on("click",e=>{if(!pick)return;pick=false;const l=e.latlng.wrap();setNear(l.lat,l.lng);if(mobile())openPanel()});
async function drawOverlay(p){const q=new URLSearchParams(p);["limit","sort","order"].forEach(k=>q.delete(k));
  try{const r=await (await fetch("/api/tracks?"+q)).json();ovL.clearLayers();
    r.forEach(t=>{let pv=null;const ll=t.p.map(([la,lo])=>{if(pv!==null){while(lo-pv>180)lo-=360;while(lo-pv<-180)lo+=360}pv=lo;return [la,lo]});
      L.polyline(ll,{pane:"ov",color:col(t.w),weight:2,opacity:.5}).bindTooltip(esc(t.title)).on("click",()=>{if(mobile())closePanel();show(t.sid)}).addTo(ovL)});
    return r.length}catch(e){}}
$("#ovSw").onchange=async e=>{ovOn=e.target.checked;
  if(!ovOn){ovL.clearLayers();return}
  const n=await drawOverlay(lastP);toast(n?`${n}本を重ねて表示（タップで選択）`:"該当なし");if(n&&mobile())closePanel()};

// ================= 統計 =================
function condText(p){const a=[],g=k=>p.get(k);
  if(g("name"))a.push(`名前「${g("name")}」`);
  if(g("year_from")||g("year_to"))a.push(`${g("year_from")||"最初"}年〜${g("year_to")||"最新"}年`);
  if(g("month"))a.push(`${g("month")}月に発生`);
  if(g("wind_min"))a.push(`最大風速${g("wind_min")}m/s以上`);
  if(g("wind_max"))a.push(`最大風速${g("wind_max")}m/s以下`);
  if(g("pres_max"))a.push(`最低気圧${g("pres_max")}hPa以下`);
  if(g("days_min"))a.push(`継続${g("days_min")}日以上`);
  if(g("named"))a.push("名前付きのみ");
  if(g("near")){const[la,lo,k]=g("near").split(",");a.push(`地点（北緯${(+la).toFixed(1)}度・東経${(+lo).toFixed(1)}度）から${k}km以内`)}
  return a.length?a.join(" ／ "):"条件なし（全ての台風）"}
let stSeq=0;
async function loadStats(){
  const my=++stSeq,b=$("#stBody");b.innerHTML='<p class="note">集計中…</p>';
  const q=new URLSearchParams(lastP);["limit","sort","order"].forEach(k=>q.delete(k));
  try{const s=await (await fetch("/api/stats?"+q)).json();if(my!=stSeq)return;
    const tot=s.total||1,ys=s.years,cl=[...CLS,UNK].map(c=>[c[1],s.classes[c[1]]||0,c[2]]).filter(c=>c[1]);
    const bars=(a,c)=>{const mx=Math.max(1,...a.map(x=>x[1])),w=100/a.length;
      return `<svg class="bs" viewBox="0 0 100 40" preserveAspectRatio="none">${a.map((x,i)=>`<rect x="${(i*w+.15).toFixed(2)}" y="${(38-x[1]/mx*36).toFixed(2)}" width="${(w-.3).toFixed(2)}" height="${(x[1]/mx*36).toFixed(2)}" fill="${c}"><title>${x[0]}: ${x[1]}</title></rect>`).join("")}</svg>`};
    b.innerHTML=`<div class="cond"><div class="note">集計の対象（「過去の台風」で絞り込んだ条件）</div><b>${esc(condText(lastP))}</b><button type="button" class="btn sm" data-a="gotodb">条件を変更する</button></div>
     <div class="kpi"><div><small>台風の数</small><strong>${s.total}<span>個</span></strong></div><div><small>平均の継続日数</small><strong>${s.avg_days??"-"}<span>日</span></strong></div>
      <div class="w2"><small>最も長く続いた台風</small><strong style="font-size:14px">${s.max_row?esc(s.max_row.title)+"（"+s.max_row.days+"日）":"-"}</strong></div></div>
     <h3 class="sh">強さ別</h3><div class="cb">${cl.map(c=>`<i style="width:${c[1]/tot*100}%;background:${c[2]}"></i>`).join("")}</div>
     <div class="lg">${cl.map(c=>`<span><i style="background:${c[2]}"></i>${c[0]} ${c[1]}</span>`).join("")}</div>
     <h3 class="sh">月別の発生数</h3>${bars(s.months.map((n,i)=>[(i+1)+"月",n]),"#38bdf8")}<div class="ax"><span>1月</span><span>6月</span><span>12月</span></div>
     ${ys.length>1?`<h3 class="sh">年別の発生数</h3>${bars(ys,"#ff8a3d")}<div class="ax"><span>${ys[0][0]}</span><span>${ys[ys.length-1][0]}</span></div>`:""}`;
  }catch(e){if(my==stSeq)b.innerHTML='<p class="note">集計に失敗しました。時間をおいて再度お試しください</p>'}}
$("#stBody").addEventListener("click",e=>{if(e.target.closest("[data-a=gotodb]"))setTab("db")});

// ================= 予報 =================
// 表現のルール:  線と外側の輪の色 = 予報の出どころ(モデル) / 印の中の色と大きさ = 強さ / 円 = 単独モデル、菱形 = アンサンブル平均
const FC=(()=>{
const COL={"JMA公式":"#ffffff",UKMET:"#ff8a3d",NAVGEM:"#a78bfa",CMC:"#34d399",GEFS:"#38bdf8","CMC-EPS":"#10b981","NAVGEM-EPS":"#c4b5fd",ECMWF:"#f43f5e",GFS:"#facc15","JMA-GSM":"#f9a8d4","JTWC公式":"#ef4444","ECMWF-ENS":"#fb7185","UKMET-ENS":"#fb923c","JMA-GEPS":"#e879f9","WeatherNext3(AI)":"#22d3ee","WeatherNext2(AI)":"#2dd4bf","GenCast(AI)":"#a3e635"};
const colr=k=>COL[k]||"#94a3b8";
const GN={off:"公式予報",det:"単独モデル",ens:"アンサンブル平均",ai:"AIモデル（アンサンブル）"};
const KT=0.514444;
const w10=(kt,is10)=>kt?kt*KT*(is10?1:.88):null;   // ATCFの1分間平均を気象庁の10分間平均相当(×0.88)
const kls=(kt,is10)=>{const v=w10(kt,is10);return v==null?UNKF:FCLS.find(c=>v>=c[0])};
const itxt=(kt,hp,is10)=>{const v=w10(kt,is10),k=kls(kt,is10);return [v!=null?`${v.toFixed(0)}m/s`:"",hp?`${Math.round(hp)}hPa`:"",v!=null?k[1]:""].filter(Boolean).join(" ")};
const tm=L.layerGroup(),ana=L.layerGroup();
let items=[],d=null,ref=140,ready=false,active=false,met="w",fitFor="";
const opt={step:24,inlay:false,num:false};
const w=lo=>lo+360*Math.round((ref-lo)/360);   // 日付変更線をまたいでも線が飛ばないよう経度を連続化
const fmt=i=>i?`${i.slice(4,6)}/${i.slice(6,8)} ${i.slice(8,10)}Z`:"";
const avg=(a,b)=>a&&b?(a+b)/2:(a||b||null);
const at=(p,t)=>{for(let i=0;i<p.length;i++){if(p[i][0]==t)return[p[i][1],w(p[i][2])];
  if(p[i][0]>t){if(!i)return null;const a=p[i-1],b=p[i],f=(t-a[0])/(b[0]-a[0]);return[a[1]+(b[1]-a[1])*f,w(a[2])+(w(b[2])-w(a[2]))*f]}}return null};
const atv=(p,t)=>{for(let i=0;i<p.length;i++){if(p[i][0]==t)return[p[i][3],p[i][4]];
  if(p[i][0]>t){if(!i)return[null,null];const a=p[i-1],b=p[i],f=(t-a[0])/(b[0]-a[0]),l=(x,y)=>x==null||y==null?null:x+(y-x)*f;return[l(a[3],b[3]),l(a[4],b[4])]}}return[null,null]};
const mrange=(it,t)=>{if(!it.members)return"";const v=it.members.map(m=>w10(atv(m,t)[0],false)).filter(x=>x!=null);
  return v.length<2?"":` ・ メンバー${Math.min(...v).toFixed(0)}〜${Math.max(...v).toFixed(0)}m/s（${v.length}本）`};
// 予報の印: 中の色と大きさ=強さ、外側の輪=モデル、形=円(単独)/菱形(アンサンブル平均)
function mk(lat,lon,c,kt,is10,o){const k=kls(kt,is10),s=k[3]+(o.cur?4:0),S=s+(o.cur?22:16);
  return L.marker([lat,lon],{icon:L.divIcon({className:"mkw",html:`<span class="mk${o.e?" e":""}${o.cur?" cur":""}" style="--s:${s}px;--f:${k[2]};--r:${c}"></span>`,iconSize:[S,S]}),interactive:!o.cur,keyboard:false})}
function draw(it){
  const g=it.g;g.clearLayers();
  const ll=it.pts.map(p=>[p[1],w(p[2])]);
  if(opt.inlay){   // 外側=モデルの色、内側=その区間の強さの色
    L.polyline(ll,{color:it.c,weight:7,opacity:.95,lineCap:"butt",dashArray:it.dash,interactive:false}).addTo(g);
    for(let i=1;i<it.pts.length;i++)L.polyline([ll[i-1],ll[i]],{color:kls(avg(it.pts[i-1][3],it.pts[i][3]),it.is10)[2],weight:3,lineCap:"butt",interactive:false}).addTo(g);
  }else L.polyline(ll,{color:it.c,weight:it.big?4:3,dashArray:it.dash,opacity:.95,interactive:false}).addTo(g);
  it.pts.forEach(p=>{if(!p[0]||p[0]%opt.step)return;
    mk(p[1],w(p[2]),it.c,p[3],it.is10,{e:it.ens}).bindTooltip(`${esc(it.label)} +${p[0]}h ${itxt(p[3],p[4],it.is10)}`).addTo(g);
    if(opt.num){const v=w10(p[3],it.is10);if(v!=null)L.marker([p[1],w(p[2])],{icon:L.divIcon({className:"mlab2",html:`<span>${Math.round(v)}</span>`,iconSize:[0,0]}),interactive:false,keyboard:false}).addTo(g)}});
}
function redraw(){items.forEach(draw);apply()}
function setNote(t){$("#fnote").textContent=t}
function build(){
  items.forEach(i=>{i.g.remove();i.mem&&i.mem.remove()});items=[];ana.clearLayers();
  ref=d.analysis?d.analysis.lon:140;
  const add=(label,init,pts,o={})=>{const it={label,init,pts,c:colr(label),g:L.layerGroup(),visible:true,is10:!!o.is10,ens:!!o.ens,dash:o.dash,big:o.big,grp:o.grp,tag:o.tag||""};items.push(it);return it};
  if(d.jma)add("JMA公式",d.jma.init,d.jma.pts,{big:1,is10:true,grp:"off"});
  d.models.forEach(m=>add(m.label,m.init,m.pts,{grp:m.label=="JTWC公式"?"off":"det"}));
  d.ens.forEach(e=>{const it=add(e.label,e.init,e.mean,{dash:"6 5",ens:true,grp:/\(AI\)$/.test(e.label)?"ai":"ens",tag:`${e.n}メンバー`});it.members=e.members;
    it.mem=L.layerGroup();e.members.forEach(m=>L.polyline(m.map(x=>[x[1],w(x[2])]),{color:it.c,weight:1,opacity:.3,interactive:false}).addTo(it.mem))});
  items.forEach(draw);
  if(d.analysis)L.circleMarker([d.analysis.lat,w(d.analysis.lon)],{radius:6,color:"#fff",weight:2,fillColor:"#0a1120",fillOpacity:1}).bindTooltip("解析位置 "+fmt(d.analysis.init)).addTo(ana);
  $("#fchips").innerHTML=["off","det","ens","ai"].map(g=>{const a=items.map((it,i)=>[it,i]).filter(([it])=>it.grp==g);
    return a.length?`<button type="button" class="gh" data-g="${g}"><span>${GN[g]}</span><span>まとめて切り替え</span></button><div class="srcs">${a.map(([it,i])=>`<button type="button" class="src" data-i="${i}" style="--c:${it.c}"><i class="ln${it.ens?" d":""}"></i>${esc(it.label)}<small>${fmt(it.init)}${it.tag?" "+it.tag:""}</small></button>`).join("")}</div>`:""}).join("");
  const mx=Math.min(240,Math.max(24,...items.map(i=>i.pts.length?i.pts[i.pts.length-1][0]:0)));
  $("#tau").max=mx;if(+$("#tau").value>mx)$("#tau").value=48;
  const have=items.map(i=>i.label),miss=["ECMWF","GFS","JMA-GSM","ECMWF-ENS","UKMET-ENS","JMA-GEPS","GEFS"].filter(x=>!have.includes(x)),u=new Date(d.updated),s=d.sources||{};
  $("#fsrc").textContent=`取得元: UCAR RAL（ATCF a-deck）${isNaN(u)?"":" 更新 "+u.toLocaleString("ja-JP",{month:"numeric",day:"numeric",hour:"2-digit",minute:"2-digit"})}`+
    (s.weatherlab_ok?" ＋ Google DeepMind Weather Lab（AI）":"")+"。"+(d.jma?"公式予報は気象庁。":"")+`アンサンブル計${s.members||0}メンバー。`+
    (miss.length?`この取得元に無いモデル: ${miss.join("・")}。`:"")+"風速は10分間平均相当（モデルは1分間平均×0.88）。予報は参考値です。防災には気象庁の情報を確認してください。";
  setNote(`${items.length}種類のソースを表示中`);
  apply();
  if(active&&fitFor!=d.id){fitFor=d.id;fit()}
}
function apply(){items.forEach(it=>{const on=active&&it.visible;on?it.g.addTo(map):it.g.remove();
  if(it.mem)(on&&$("#mem").checked)?it.mem.addTo(map):it.mem.remove()});
  document.querySelectorAll("#fchips .src").forEach(b=>b.classList.toggle("off",!items[+b.dataset.i].visible));marks()}
function marks(){tm.clearLayers();const t=+$("#tau").value,out=[];$("#tv").textContent=`+${t}h`;
  items.forEach(it=>{if(!it.visible)return;const p=at(it.pts,t);if(!p)return;const q=atv(it.pts,t);
    mk(p[0],p[1],it.c,q[0],it.is10,{e:it.ens,cur:true}).addTo(tm);
    const k=kls(q[0],it.is10),v=w10(q[0],it.is10);
    out.push(`<div class="pr"><i class="rg${it.ens?" e":""}" style="--r:${it.c};--f:${k[2]}"></i><div class="pn"><b>${esc(it.label)}</b><small>${p[0].toFixed(1)}N ${((p[1]%360+360)%360).toFixed(1)}E${mrange(it,t)}</small></div>
     <div class="iv"><strong style="color:${k[2]}">${v!=null?v.toFixed(0):"-"}</strong><small>m/s${v!=null?" "+k[1]:""}${q[1]?" "+Math.round(q[1])+"hPa":""}</small></div></div>`)});
  $("#pos").innerHTML=out.join("")||'<p class="note">この時間の予報はありません</p>'}
const pad=()=>mobile()?{paddingTopLeft:[16,60],paddingBottomRight:[16,$("aside").offsetHeight+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[40,40]};
function fit(){const ll=[];items.forEach(it=>it.pts.forEach(p=>ll.push([p[1],w(p[2])])));if(d&&d.analysis)ll.push([d.analysis.lat,w(d.analysis.lon)]);
  if(ll.length)map.fitBounds(L.latLngBounds(ll),pad())}
function show(){active=true;ana.addTo(map);tm.addTo(map);if(!ready){ready=true;init()}else apply()}
function hide(){active=false;items.forEach(i=>{i.g.remove();i.mem&&i.mem.remove()});ana.remove();tm.remove()}
// 操作
$("#fchips").onclick=e=>{
  const g=e.target.closest(".gh");
  if(g){const a=items.filter(i=>i.grp==g.dataset.g),on=!a.some(i=>i.visible);a.forEach(i=>i.visible=on);apply();return}
  const b=e.target.closest(".src");if(!b)return;const it=items[+b.dataset.i];it.visible=!it.visible;apply()};
$("#mem").onchange=apply;$("#tau").oninput=marks;
const tog=(id,k)=>$(id).onclick=e=>{opt[k]=!opt[k];e.currentTarget.classList.toggle("on",opt[k]);redraw()};
tog("#oInlay","inlay");tog("#oNum","num");
$("#oStep").onchange=e=>{opt.step=+e.target.value;redraw()};
// ばらつき表・強さ予報
$("#tbl").onclick=()=>{if(!d)return;const s=d.spread,dl=$("#dlg"),n=v=>v==null?"-":Math.round(v);
  dl.innerHTML=`<div style="display:flex;justify-content:space-between;align-items:center"><b>各時刻の位置のばらつき</b><button type="button" class="btn sm" data-close>閉じる</button></div>
   <p style="color:var(--sub);font-size:12px">各モデルの予報位置が、全モデルの平均位置から何km離れているか。大きいほどモデル間で意見が割れています。</p>
   <table><tr><th></th>${s.taus.map(t=>`<th>+${t}h</th>`).join("")}</tr>${s.rows.map(r=>`<tr><td>${esc(r[0])}</td>${r[1].map(v=>`<td>${n(v)}</td>`).join("")}</tr>`).join("")}
   <tr><td>平均からの最大</td>${s.max.map(v=>`<td><b>${n(v)}</b></td>`).join("")}</tr><tr><td>モデル数</td>${s.n.map(v=>`<td>${v}</td>`).join("")}</tr></table>`;dl.showModal()};
function chart(){
 const S=(d.intensity||{series:[]}).series,W=520,H=250,L0=42,R0=10,T0=10,B0=26,isW=met=="w",ix=isW?[1,2,3]:[4,5,6],vs=[];
 S.forEach(s=>s.rows.forEach(r=>ix.forEach(i=>{if(r[i]!=null)vs.push(r[i])})));
 if(!vs.length)return"<p style='color:var(--sub)'>この項目の予報値がありません</p>";
 let lo,hi;
 if(isW){lo=0;hi=Math.max(60,Math.ceil(Math.max(...vs)/10)*10)}else{lo=Math.floor((Math.min(...vs)-4)/10)*10;hi=Math.ceil((Math.max(...vs)+4)/10)*10}
 const tmax=Math.max(24,...S.map(s=>s.rows[s.rows.length-1][0])),
  X=t=>L0+(W-L0-R0)*t/tmax,Y=v=>isW?T0+(H-T0-B0)*(1-(v-lo)/(hi-lo)):T0+(H-T0-B0)*(v-lo)/(hi-lo),st=isW?10:20;
 let g="";
 for(let v=Math.ceil(lo/st)*st;v<=hi;v+=st)g+=`<line x1="${L0}" x2="${W-R0}" y1="${Y(v)}" y2="${Y(v)}" stroke="#22314f"/><text x="${L0-5}" y="${Y(v)+4}" fill="#8b9bbb" font-size="10" text-anchor="end">${v}</text>`;
 for(let t=0;t<=tmax;t+=24)g+=`<line x1="${X(t)}" x2="${X(t)}" y1="${T0}" y2="${H-B0}" stroke="#22314f"/><text x="${X(t)}" y="${H-8}" fill="#8b9bbb" font-size="10" text-anchor="middle">+${t}h</text>`;
 if(isW)[[33,"強い"],[44,"非常に強い"],[54,"猛烈な"]].forEach(([v,n])=>{if(v<hi)g+=`<line x1="${L0}" x2="${W-R0}" y1="${Y(v)}" y2="${Y(v)}" stroke="#8b9bbb" stroke-dasharray="4 4"/><text x="${W-R0-2}" y="${Y(v)-3}" fill="#8b9bbb" font-size="10" text-anchor="end">${n}</text>`});
 S.forEach(s=>{const c=colr(s.label),rs=s.rows.filter(r=>r[ix[0]]!=null);if(!rs.length)return;
  if(s.kind=="ens"&&rs.length>1){const a=rs.map(r=>`${X(r[0])},${Y(r[ix[2]])}`),b=rs.map(r=>`${X(r[0])},${Y(r[ix[1]])}`).reverse();
   g+=`<polygon points="${a.concat(b).join(" ")}" fill="${c}" opacity=".16"/>`}
  g+=`<polyline points="${rs.map(r=>`${X(r[0])},${Y(r[ix[0]])}`).join(" ")}" fill="none" stroke="${c}" stroke-width="${s.kind=="ens"?2:2.5}"${s.kind=="ens"?' stroke-dasharray="6 4"':""}/>`});
 return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto">${g}</svg>`}
function openInt(){if(!d)return;const S=(d.intensity||{series:[]}).series,dl=$("#dlg"),fv=v=>v==null?"-":Math.round(v),T=[24,48,72,96,120],ens=S.filter(s=>s.kind=="ens");
 const legendH=S.map(s=>`<span class="src" style="--c:${colr(s.label)};min-height:26px;font-size:11px;cursor:default"><i class="ln${s.kind=="ens"?" d":""}"></i>${esc(s.label)}${s.kind=="ens"?`<small>${s.n}本</small>`:""}</span>`).join("");
 const cell=(s,t)=>{const r=s.rows.find(x=>x[0]==t);if(!r)return"<td>-</td>";
  return`<td><b>${fv(r[1])}</b><br><small style="color:var(--sub)">${s.kind=="ens"?`${fv(r[2])}〜${fv(r[3])}`:(r[4]?r[4]+"hPa":"")}</small></td>`};
 const pr=ens.length?`<p style="color:var(--sub);font-size:12px;margin-top:14px">各階級以上になるメンバーの割合（強い以上／非常に強い以上／猛烈な, %）</p>
  <table><tr><th></th>${T.map(t=>`<th>+${t}h</th>`).join("")}</tr>${ens.map(s=>`<tr><td>${esc(s.label)}</td>${T.map(t=>{const r=s.rows.find(x=>x[0]==t);return`<td>${r&&r[8]?r[8].join("/"):"-"}</td>`}).join("")}</tr>`).join("")}</table>`:"";
 dl.innerHTML=`<div style="display:flex;justify-content:space-between;align-items:center"><b>強さの予報</b><button type="button" class="btn sm" data-close>閉じる</button></div>
  <div class="btns" style="margin:8px 0"><button type="button" class="btn sm${met=="w"?" on":""}" data-met="w">風速(m/s)</button><button type="button" class="btn sm${met=="p"?" on":""}" data-met="p">中心気圧(hPa)</button></div>
  ${chart()}<div style="display:flex;flex-wrap:wrap;gap:4px;margin:6px 0">${legendH}</div>
  <p style="color:var(--sub);font-size:12px">実線=単独モデル、破線＋帯=アンサンブル平均と10〜90%範囲。風速は10分間平均相当（モデルは1分間平均×0.88）。全球モデルは分解能の都合で猛烈な台風を弱めに予報しがちです。</p>
  <table><tr><th></th>${T.map(t=>`<th>+${t}h</th>`).join("")}</tr>${S.map(s=>`<tr><td style="color:${colr(s.label)}">${esc(s.label)}</td>${T.map(t=>cell(s,t)).join("")}</tr>`).join("")}</table>${pr}`;
 if(!dl.open)dl.showModal()}
$("#int").onclick=openInt;
$("#dlg").addEventListener("click",e=>{const dl=$("#dlg");
  if(e.target===dl||e.target.closest("[data-close]"))return dl.close();
  const b=e.target.closest("[data-met]");if(b){met=b.dataset.met;openInt()}});
// 読み込み
async function load(){const o=$("#fsel").selectedOptions[0];if(!o)return;setNote("読み込み中…");
  try{const r=await fetch(`/api/forecast/${o.value}?jma=${o.dataset.jma||""}`);if(!r.ok)throw 0;d=await r.json();build()}
  catch(e){setNote("予報データを取得できませんでした。少し待ってから「更新」を押してください")}}
async function init(){setNote("台風の一覧を取得中…");
  try{const r=await fetch("/api/forecast/storms");if(!r.ok)throw 0;const s=await r.json(),keep=$("#fsel").value;
    $("#fsel").innerHTML=s.map(x=>`<option value="${esc(x.id)}" data-jma="${esc(x.jma?x.jma.tc:"")}">${esc(x.label)}${x.jma?` ／ 台風${+x.jma.num.slice(2)}号`:""}</option>`).join("");
    if(!s.length){setNote("現在、活動中の西太平洋の台風はありません");return}
    if(keep&&s.some(x=>x.id==keep))$("#fsel").value=keep;load()}
  catch(e){setNote("台風の一覧を取得できませんでした。「更新」で再試行してください")}}
$("#fsel").onchange=load;$("#frf").onclick=init;
return{show,hide,fit};
})();

// ================= 起動 =================
$("#dlg").addEventListener("click",e=>{if(e.target.id=="dlg")e.target.close()});
if("serviceWorker" in navigator)addEventListener("load",()=>navigator.serviceWorker.register("/sw.js").catch(()=>{}));
legend();
setTab(initTab);
dbInit();
// 開いたままでも、サーバー側で速報が更新されたら一覧と表示中の台風を自動で更新する
async function poll(){
  try{
    const y=await (await fetch("/api/years")).json();
    if(y.jma_at&&y.jma_at!==jmaAt){
      jmaAt=y.jma_at;$("#upd").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
      await load();if(curSid)show(curSid,null,true);
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
    elif len(sys.argv) > 1 and sys.argv[1] == "fdiag":
        fdiag()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
