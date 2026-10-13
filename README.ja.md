# briskapi

[English](README.md) | [日本語](README.ja.md)

BRiSK の板寄せデータを扱う、非公式の Python API と `brisk` コマンドラインツールです。
Databento 風の `Live` と `Historical` クライアントを備えています。ライブフィードの購読、記録データの任意時点での照会、
自分専用の非公開アーカイブへの保存に加え、ご自身の口座で SBI証券や立花証券
e支店（BRiSK Next）の BRiSK も利用できます（パスキーでのサインインにも対応）。本プロジェクトは独立したもので、BRiSK、立花証券、SBI証券、
東京証券取引所（TSE）、日本取引所グループ（JPX）とは提携しておらず、承認も
受けていません。

> **データ源:**
> - **口座不要:** 2021年9月27日の公開 BRiSK Next デモ（寄り前のスナップショット
>   1件と寄付から3分間）を、記録時のペースで再生したもの。リアルタイムの市場
>   データではありません。
> - **お使いの証券会社の BRiSK（SBI証券、または立花証券 e支店の BRiSK Next）を
>   使える場合:** BRiSK の市場データ（ローソク足、信用残、アラート、取引
>   スケジュール、ウォッチリスト）と、試験的なライブフィード。

## どの API を使うか

| したいこと | 使うもの | 読むもの |
| --- | --- | --- |
| 気配・約定・板寄せをリアルタイムで | `briskapi.connect()` のあと `Ticker`/`Market` | ライブフィード |
| 同じものを Databento 風のレコード（`trades`、`mbp-10`、`imbalance` など）として、このコンピューター上のどのプログラムからでも | `briskapi.Live()` | このコンピューターで `brisk live` が動かしているフィード（自身のログインは不要） |
| 手元の記録をオフラインで、または NautilusTrader で | `briskapi.load(dir)` か `briskapi.pull(prefix)` のあと `Ticker`/`Market`、`brisk export` | その記録（記録したとおり） |
| 複数日にわたる履歴や OHLCV バーを、ダウンロードせずに | `briskapi.Historical()`、`brisk query` | クラウドにある自分のアップロード（デモは誰でも） |
| BRiSK 自身のローソク足、信用残、売買代金、リスト、イベント | `Ticker.candles()`、`.margin()`、`Market().turnover()` など | 証券会社の BRiSK のサーバー（ログインが必要） |
| アップロードしたものの管理 | `briskapi.Archive()`、`brisk list`、`pull`、`delete`、`restore`、`prune` | 自分専用のアーカイブ |

`Ticker.history()` と `.trades()` はネットワークを使いません。読み込んだ記録かライブフィードを
読みます。
- 記録なら、その記録の範囲がそのまま返ります。
- ライブフィードは `history=True` が必要です。接続してからの更新をすべて保持し、約定は BRiSK の
  スナップショットが復元できる範囲でその日の始めまでさかのぼります。

証券会社の BRiSK に問い合わせるのは `Ticker.candles()` です。`5m` は当日のみで、`1d`・`1w`・
`1mo` がどこまでさかのぼるかは BRiSK 次第で、まだ分かっていません。自分の記録から複数日の
バーがほしい場合は `Historical`（`ohlcv-1s` 〜 `ohlcv-1d`）を使ってください。

手元の呼び出しとクラウド API では、同じデータでも名前が異なります（たとえば手元の
`indicative_price` はクラウドでは `ind_match_price`）。対応表は
[docs/data-format.md](docs/data-format.md#local-and-cloud-names)（英語）にあります。`briskapi.Live` は
クラウド API の名前とレコードを使います。

## インストール

Python 3.12 以上が必要です。ライブフィードと記録には Node 22 以上も必要です
（BRiSK 自身のデコーダーが Node 上で動く WebAssembly モジュールのため）。

```sh
pip install 'briskapi[pandas]'      # `import briskapi` と `brisk` コマンド
```

BRiSK のデコーダーとデモデータは実行時にダウンロードされ、パッケージには
含まれていません。Linux、macOS、Windows 向けのビルド済み Rust ツール
（`brisk_quote_ingest`、`brisk_recording`）は任意で、各
[GitHub リリース](https://github.com/honvl/BRiSKapi/releases)に添付されています。
ソースから使う場合は、リポジトリをクローンして `pip install -e '.[pandas]'` を
実行してください。

## ライブフィード

```python
import briskapi

feed = briskapi.connect(web=True, codes=["7203", "6758"])   # 初期状態を受信してから戻る
toyota = briskapi.Ticker("7203")
toyota.quote()        # 現在の気配（フレームが届くたびに更新）
toyota.auction()      # 予想約定価格・数量と成行注文の売買差

feed.on_quote(lambda q: print(q["code"], q["indicative_price"]), codes="6758")
for q in feed.quotes("7203"):       # 更新ごとに1件。セッションが終わると終了
    if q["last_price"]:
        print("寄付:", q["last_price"], q["time"])
        break

briskapi.Market().imbalances(top=10).to_pandas()
feed.wait()           # または feed.close()。`with briskapi.connect(...) as feed:` も使えます
```

主なオプション: `web=True`（デモサイトから取得）または `cache=DIR`（デモ
データのローカルコピー）、`codes`（銘柄コード）、`speed`（`1` で実時間、`0` で
最速）、`history=True`（`Ticker.history()` のために更新を保持）。コールバックと
イテレーターは、まず各銘柄の現在の気配を受け取り、その後すべての更新を順番
どおりに受け取ります。受け取る側の処理が遅い場合、更新を捨てずにフィードの
ほうが待ちます。

## ライブのレコード（Databento 風）

`brisk live` は、そのフィードをこのコンピューター上の他のプログラムにも、クラウド API が返すのと
同じレコードとして配信します。`briskapi.Live` は Databento のライブクライアントと同じ形で、
キーもサインインも要りません。証券会社にサインインするのはフィードだけです。

```sh
brisk live --sbi --quiet        # 止めるまで記録・アップロードし、XTKS.BRISK を配信（--web ならデモの XTKS.BRISKDEMO）
```

```python
import briskapi

live = briskapi.Live()          # このコンピューターで動いているフィードを見つける
live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203", "6758"])
live.subscribe(dataset="XTKS.BRISK", schema="trades", symbols=["7203"], start=0)   # その日のここまで、続けてライブ
live.subscribe(dataset="XTKS.BRISK", schema="ohlcv-1m", symbols=["7203"])          # 1分ごとのバーを、その1分が終わりしだい
for record in live:             # または live.add_callback(fn); live.start(); live.block_for_close()
    print(record.schema, record.symbol, record.ts_event)
```

- **スキーマ:** `trades`、`mbp-10`、`imbalance`、`definition`、`brisk-quotes`、OHLCV バーの
  `ohlcv-1s` 〜 `ohlcv-1d`。同じセッションなら `Historical` が返すのと同じ列・同じ値です。`Live` は
  受け取った約定から、クラウドと同じ規則でバーを作ります。
- **バー**は、その区間がフィードの時計で終わりしだい届きます。終わりより後の時刻のバッチが届いたとき、
  またはこのコンピューターの時計から、BRiSK からこのセッションまでに測った遅延（直近10秒の最大値）を
  引いた時刻が終わりを過ぎたときです。自分のバーが終わったあとに届いた約定は、まだ終わっていない次の
  バーに入ります（クラウドでは約定自身の時刻のバーに入ります）。どの約定も数えられ、バーは順に届きます。
  バーは、セッションがその取引時間内の全期間を見ている場合にだけ渡します。
  `start` を指定すればフィードの開始から、指定しなければ購読より後に始まるバーです。
- **セッションの始まり:** その日の銘柄定義、続いて届いた更新を順に受け取ります。`start`（時刻、
  日付、UTC ナノ秒、またはフィードが持つすべてを表す 0）を指定すると、まずフィードのその取引日を
  その時点から再生します。フィードが持っているのは、起動してからの分と、スナップショットが復元
  したその日の約定です。`snapshot=True` を指定すると、`mbp-10`、`imbalance`、`brisk-quotes` を
  各銘柄の現在の気配から始めます（`flags` は 32）。
- **レコード**は辞書で、属性としても読めます（`record.price`）。スキーマは `record.schema` です。
  フィードがバッチを受け取った順に届き、バッチ内は `Historical` と同じ順です。
  `add_callback(fn, exception_callback)` と `add_stream(path)`（JSON Lines。Databento では DBN）
  でも受け取れます。`block_for_close(timeout)` はセッションの終わりを待ち、`stop()` は終わらせます。
  購読は `start()` の前でも後でもかまいません。後からの購読にもそれぞれの再生やスナップショットがあり、
  その銘柄はその時点から届きます。1つのセッションの購読は同じデータセットを共有します。
  `symbology_map` は `instrument_id` から銘柄コードへの対応です。
- **このコンピューターだけ:** フィードは 127.0.0.1 でのみ待ち受け、所有者だけが読めるファイル
  （`~/.config/brisk/gateway/XTKS.BRISK.json`）にポート番号とともに保存したキーを持つセッション
  だけを受け入れます。`Live()` はこのファイルを読みます。配信する内容がこのコンピューターの外に
  出ることはありません。1 GB を超えて遅れたセッションは、フィードやその記録を遅らせる代わりに
  切断されます。
- **配信:** `brisk live` は `--no-gateway` を指定しない限り配信します。1つのデータセットを配信できる
  フィードは同時に1つです。Python では `briskapi.connect()` か `briskapi.brisk.connect()` に
  `gateway=True` を渡します。

## 記録データと自分のアーカイブ

```python
recordings = briskapi.recordings(source="historical_mock")   # 自分の記録だけ。アカウント不要
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # 検証・展開・キャッシュし、既定のデータにする
else:
    briskapi.record("recordings/my-session", web=True)   # デモを記録（オプトアウトしない限りアップロード）
# または briskapi.load("recordings/my-session") でローカルの記録フォルダー（以前の events.jsonl[.gz] も可）を読む。

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # 寄り前の気配（日本時間）
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # 期間内のすべての更新
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

記録は NautilusTrader v2 のカタログと同じ構成の Parquet テーブルのフォルダーです。
`data/order_book_depths.parquet`（すべての気配更新。v2 の `OrderBookDepth` に BRiSK の
ほかの気配項目を加えたもの）、`data/trades.parquet`（すべての約定、v2 の `TradeTick`）、
`data/equity.parquet`（銘柄マスター、v2 の `Equity`）、
`data/custom/BriskBatch.parquet`（フレームごとの計測値）からなります。デモ全体で約
41 MB です。pandas、Polars、DuckDB でそのまま読めます。`brisk export` で
NautilusTrader のカタログや JSONL に書き出せます。全列の説明と pandas・Polars・DuckDB での読み方は
[docs/data-format.md](docs/data-format.md)（英語）にあります。

時刻の照会には、バッチではなく各気配のタイムスタンプを使います。デモの
7203 の最初の気配は日本時間 08:59:59.993551 で、それより前は `NotFoundError`
になります。`Market().summary()` はローカルの記録やライブフィードでも `start`
と `end` を返します。アーカイブには自分がアップロードしたものだけが、自分の ID（お使いの
コンピューターで作られ、どこにも登録しないランダムな鍵。[自分専用のアーカイブ](#自分専用のアーカイブ)
を参照）の下に保存されます。一覧では、不正または未対応の記録を警告付きでスキップします。
サンプルが1つだけ、誰でもダウンロードできるよう公開されています。[通信をそのまま記録したもの](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/samples/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz)
（2026-03-11、日本時間 12:12:40 から）で、このツールがもう読まない旧形式
`brisk-sbi-wire-jsonl-v1` です。

## お使いの証券会社の BRiSK

同じクライアントで、SBI証券の BRiSK（[sbi.brisk.jp](https://sbi.brisk.jp)、既定のサイト）と
立花証券 e支店の BRiSK Next（専用の項が続きます）を使えます。証券会社と BRiSK の契約が
必要です。SBI では、ブラウザで sbi.brisk.jp にログインし、そのセッション Cookie を
渡します。Cookie は DevTools からコピーするか、`pycookiecheat` の
`chrome_cookies("https://sbi.brisk.jp")` で取得できます。パスキーで `briskapi` に
サインインさせることもできます（[下記](#パスキーでのサインイン試験的)）。

```python
from briskapi import brisk

brisk.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True で保存（本人のみ読み取り可）
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # ローソク足: 5分（当日）、日、週、月（5m, 1d, 1w, 1mo）
toyota.margin(days=30)             # 信用残と貸株料
market = briskapi.Market()
market.turnover()     # 全銘柄の売買代金と発行済株式数
market.lists()        # 日経225、直近 IPO など
market.events()       # バスケット注文、ストップ高・安、出来高急増などのイベント
market.schedule()     # 取引日、状態、取引時間
market.watchlist()    # 保存済みの銘柄コード

feed = brisk.connect(codes=["7203"])        # ライブ（試験的）。オプトアウトしない限り、終わった日ごとにアップロード
toyota.quote()                              # 他のフィードと同じ呼び出し
# upload_market_data=False で各日の市場データの記録を、upload=False ですべてをローカルに残す
```

証券会社のフィードは連続して記録し、取引日ごとに1つの記録を作ります。日をまたいでも動き続けます。
サイトが次の取引日を配信し始めたとき（10分ごとに確認）、またはサーバーが市場の終了後にストリームを
閉じたときにその日が終わり、パッケージ化されてアップロードされ（市場データの記録とタイミングの要約。
オプトアウトした場合を除く）、次の日は新しいスナップショットから記録を始めます。`output=DIR`
（`brisk.connect(output=...)`、`briskapi.connect(output=...)`、`brisk live --output DIR`）を指定すると、
終わった日ごとに、`SOURCE_YYYYMMDD_FROM-TO`（その日の最初と最後のソース時刻、日本時間 HHMMSS）という名前の
フォルダー（例: `sbi_live_20261013_085800-153512`）が DIR に残ります。`feed.recordings` は終わった日の一覧
（`trading_date`、`name`、`path`、`uploaded`、`timing_uploaded`）で、`on_day=callback` はその日ごとに呼ばれます。
フィードを止めた時点でまだ終わっていない日はアップロードされず、タイミングの要約だけがアップロードされます。

結果は下記の規約に従います。エラーは `brisk.SessionExpiredError`（再ログインが
必要）、`briskapi.NotFoundError`、`brisk.RateLimitError`、`brisk.APIError` です。
リクエストは1秒に1回までに制限しています。

ライブフィードは、ご自身のセッションでダウンロードした BRiSK 自身のデコーダーを
Node 上で動かします。ブラウザは使いません。ベンダーのクライアントと同じ接続手順
（最初のフレームからデコーダーに入力し、スナップショットをストリームに追いつかせ、
サーバーの ping にデコーダーを通じて応答し、サーバーのハートビートを監視）に従い
ます。まだ実際の SBI セッションでは検証できていません。ストリームは Next のクライ
アントと同じく素のバイナリ WebSocket と想定しています。サーバーが Socket.IO で始
まる場合は、未検証のパラメーターで `/v2/user` 名前空間に参加します（パラメーター
は `brisk.connect(profile={...})` で設定できます）。`trace_protocol=True`（トークン
はすべて伏せ字）でサーバーの応答を確認してください。想定外の応答は、推測で動かさ
ず、明示的なエラーで止まります。
結果をぜひお知らせください。実際のセッションでは現在、サインイン、データの取得、
デコーダーのダウンロード、銘柄マスター、スナップショット（セッションのスケジュール
付きで読み込み）までは通ります。その後、`api.brisk.jp` への WebSocket の Upgrade が
HTTP 502 で返されました。それらのセッションは、ベンダーのクライアントが求める
サブプロトコル `flex` を求めていませんでした。このバージョンは求めますが、それで
足りるかはまだ分かっていません。SBI の Cookie は sbi.brisk.jp にのみ送られます。

### 立花証券 e支店の BRiSK Next

e支店の BRiSK Next（[next.e-shiten.brisk.jp](https://next.e-shiten.brisk.jp)）は
同じアプリなので、同じクライアントで使えます。ブラウザでログインし、その Cookie を
`site="e-shiten"` を付けて渡すか、e支店のパスキーで `briskapi` にサインインさせてください
（[下記](#パスキーでのサインイン試験的)）。初回のみ `brisk enroll --site e-shiten`、その後は
`brisk live --site e-shiten` の前に `brisk login` を実行します。

```python
brisk.login(cookies={...}, site="e-shiten")  # または BRISK_ESHITEN_COOKIES。remember=True で SBI とは別に保存
feed = brisk.connect(codes=["7203"])         # ライブ（試験的）
```

上記の説明は日ごとの記録とアップロードも含めてそのまま当てはまります（終わった日ごとの市場データの
記録とタイミングの要約をアップロードします）。Cookie は
next.e-shiten.brisk.jp にのみ送られます。e支店の
BRiSK は同時に1セッションまでで、別のタブやブラウザで開くと先に開いていた方が
止まりました（メッセージ 10011）。そのため、フィードとブラウザの BRiSK は互いに
相手を止めることがあります。記録やアップロードの前に、立花証券の BRiSK Next 利用規定で
データの蓄積や二次利用についての定めを確認してください。

## パスキーでのサインイン（試験的）

近年は、パスキーでのサインインを求める証券会社が増えています。お使いの証券会社がそうなら、
Cookie をブラウザからコピーする代わりに、`briskapi` がサインインして BRiSK のセッション
Cookie を保存できます。SBI証券、松井証券、マネックス証券、SMBC日興証券、立花証券 e支店の
BRiSK に対応し、他の証券会社も追加できます。Chrome、Node 22 以上、そしてパスキーの安全な保管先（macOS のキー
チェーンまたは Windows の資格情報マネージャー。Linux では選択式の本人のみ読み取り可のファイル）が
必要です。macOS と Windows で動作し（Windows では PowerShell か `cmd` から実行し、WSL ではなく Windows 側の
Chrome と Node を使います）、Linux でもテストしています。

### 仕組み

パスキーは1組の鍵です。公開鍵は証券会社が持ち、秘密鍵は通常、スマートフォンやパソコン、
パスワードマネージャーの中にあります。指紋や PIN で秘密鍵が使われ、証券会社が送る
チャレンジに署名することで、本人であることを証明します。`briskapi` はこの仕組みを借ります。
専用の Chrome を起動し、その Chrome が持つ*仮想オーセンティケーター*（スマートフォンの代わりを
務め、確認なしで署名するソフトウェア）が署名します。証券会社のサイトは、その違いを見分け
られません。

1. **初回のみ `brisk enroll`。** 証券会社を選ぶと、別の Chrome ウィンドウがそのログインページで
   開きます（普段の Chrome ではなく、一時的なプロファイルです）。いつもどおりサインインし、
   新しいスマートフォンに登録するときと同じように、証券会社のセキュリティ設定でパスキーを
   登録します。仮想オーセンティケーターがそれを受け取ります。サイトに登録完了と表示された
   ら、ターミナルで Enter を押してください。`briskapi` がパスキーと選んだ証券会社をキー
   チェーン（Windows では資格情報マネージャー）に保存し、Chrome は閉じます。
   e支店では、パスキーの前にユーザIDを入力するフォームのため、`enroll` が最初にユーザIDを尋ね、
   パスキーと一緒に保存します。ウィンドウは通常のログイン（ユーザID、暗証番号、電話番号認証）で
   開きます。パスキーはログイン後に表示される登録画面か、お客様情報 › 設定情報 › パスキー認証
   から登録してください。
2. **毎回 `brisk login`。** 保存したパスキーを読み込んだ同じ Chrome が開き、ログインページで
   パスキーのボタンを押すと、仮想オーセンティケーターが署名します。そのウィンドウで証券会社の
   サイトから BRiSK を開いてください（`--launch-url` を指定すれば自動で開きます）。`briskapi` が
   BRiSK のセッション Cookie を読み取って保存し、Chrome を閉じます。
3. **あとは使うだけ。** SBI証券と e支店では、保存した Cookie を `brisk live --sbi` または
   `brisk live --site e-shiten` と `briskapi.brisk` API が使います。他の証券会社には `briskapi` の
   データクライアントがまだないため、Cookie はご自身の利用のために保存される
   （`~/.config/brisk/cookies/`）だけです。

スマートフォンやパスワードマネージャーにある既存のパスキーには手を加えません。証券会社の
一覧にパスキーが1つ増えるだけで、セキュリティ設定からいつでも削除できます。

### 注意点

- **保存したパスキーは認証情報です。** 持っている人は誰でも、追加の確認なしにあなたの
  証券口座へサインインできます。パスキーはキーチェーン／資格情報マネージャーにのみ保存され（`BRISK_PASSKEY_STORE=file`
  なら本人のみ読み取り可のファイルに保存しますが、安全性は下がります。Windows ではそのファイルの
  保護はユーザープロファイルのフォルダー（本人と管理者のみ読み取り可）に依存します）、お使いのマシンの外には
  出ず、ログにも表示されません。e支店ではユーザIDも同じ場所に一緒に保存します。`brisk forget` で
  ここから削除できます。証券会社側でも削除してください。
- **試験的な機能です。** 各証券会社のログインページ、パスキーボタンの文言、BRiSK の起動方法は
  公開ページをもとにしたもので、実際の口座では試していません。`--login-url`、`--passkey-button`、
  `--launch-url` や下記の `sites.json` で上書きでき、うまくいかない点はぜひお知らせください。
- **証券会社に拒否される可能性があります。** Chrome は自動操作されていることをサイトに伝えます。
  利用前に、証券会社の規約でこの使い方が認められているかご確認ください。

### コマンド

```sh
brisk sites                    # 選べる証券会社と BRiSK のアドレス
brisk enroll [--site matsui]   # 初回のみ。指定しなければ証券会社を選ぶよう求められます
brisk login                    # 毎回。enroll で登録した証券会社にサインインします
brisk forget                   # 保存したパスキーを削除
```

Python では `passkey.enroll(site="matsui")`（e支店では `passkey.enroll(site="e-shiten", user_id="...")`）
と `signin = passkey.login()` を使います。`signin.cookies` が BRiSK の Cookie、`signin.client` が
データクライアントのある証券会社でのクライアントです。

証券会社を追加したり、組み込みの設定を直したりするには、`~/.config/brisk/sites.json` を編集します。

```json
{"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
            "cookie_host": "mybroker.brisk.jp", "passkey_button": "パスキーでログイン"}]}
```

ほかのキー: `cookie_prefix`、`launch_url`、`enroll_url`（初回のサインインがパスキーのページとは
別のページの場合に `enroll` を始める場所）、`user_field`（パスキーのボタンを押す前に入力が必要な
ユーザID欄の CSS セレクター。指定すると `enroll` がユーザIDを尋ねます）。

内部の仕組みは [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md#passkey-sign-in) を参照してください。

## API リファレンス

| 呼び出し | 戻り値 |
| --- | --- |
| `briskapi.connect(...)` | ライブの `Feed`（既定のデータ源になる。`gateway=True` で `Live` にも配信） |
| `briskapi.Live()` | このコンピューターのフィードから Databento 風のライブのレコード: `subscribe(dataset, schema, symbols='ALL_SYMBOLS', start=None, snapshot=False)` のあと、イテレーション、または `add_callback(fn)` / `add_stream(path)` と `start()`、`block_for_close(timeout)`。`stop()`、`symbology_map`（[上記](#ライブのレコードdatabento-風)） |
| `Ticker(code).info()` | 銘柄名、売買単位、呼値の種別、基準値、値幅制限 |
| `Ticker(code).quote(at=None)` | 買い・売り気配とその数量・件数、板の各側の上位10本（`bids`、`asks`）と引け条件付き注文の上位10本（`closing_bids`、`closing_asks`）、板全体の注文合計、予想約定価格・数量、成行・引け条件付きの数量、直近の約定、売買代金、銘柄の状態、`special_quote`（特別気配の側・価格・タイマーの終了時刻） |
| `Ticker(code).auction(at=None, closing=False)` | 板寄せの予想状態、成行注文、板の合計と `market_order_imbalance`（成行の買い数量 − 売り数量）。`closing=True` で引けの板寄せ |
| `Ticker(code).history(start, end)` | すべての更新（時系列順）。読み込んだ記録かフィードから（ネットワークは使わない） |
| `Ticker(code).trades(start, end)` | すべての約定（歩み値）：時刻、価格、株数、仕掛けた側（`buy`、`sell`。寄付きの約定の多くは None）。読み込んだ記録かフィードから |
| `Market().stocks()` | 全銘柄のマスター |
| `Market().snapshot(at=None)` | 全銘柄の気配 |
| `Market().imbalances(at=None, top=None, closing=False)` | 成行注文の売買差（絶対値）が大きい順の銘柄（引けの板寄せも可） |
| `Market().special_quotes(at=None)` | 特別気配の銘柄：側、価格、タイマーの終了時刻 |
| `Market().trades(start, end)` | 全銘柄の約定（時刻順） |
| `Market().frames()` | 記録のバッチ：時刻、計測値、気配と約定の件数 |
| `Market().summary()` | データ源、日付、銘柄数、時刻の範囲 |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | 届いた順のライブ更新 |
| `Feed.recordings` | ライブフィードの終わった日の一覧：`trading_date`、`name`、`path`、`uploaded`、`timing_uploaded` |
| `briskapi.recordings()` / `.pull()` / `.load()` | 自分のアーカイブの一覧、検証付きダウンロード、ローカルファイル |
| `briskapi.Archive().upload(dir)` / `.delete(prefix)` / `.deleted()` / `.restore(prefix)` / `.timing()` / `.owner` | アップロード、削除、過去30日間に削除したもの（`deleted_at`、`restore_until` 付き）、元に戻す、自分のタイミングレポート、自分の owner ID |
| `briskapi.record(output, web=True, ...)` | デモの記録（設定に従ってアップロード） |
| `briskapi.consent(...)` | アップロードの設定 |
| `briskapi.Historical()` | クラウド API のクライアント（Databento 風）。API キーは自分のアーカイブの ID（[下記](#クラウドでアーカイブを照会する)） |
| `Historical().timeseries.get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None, path=None)` | `XTKS.BRISK`（自分の記録）または `XTKS.BRISKDEMO` のレコードを返すストア：Databento と同じ変換：`to_df()`（`ts_recv` をインデックスにする。バーは `ts_event`）、`to_ndarray()`、`to_csv(path)`、`to_json(path)`、`to_parquet(path)`、`to_file(path)`、`replay(callback)`、反復、`len()` |
| `Historical().metadata.list_datasets()` / `.list_schemas(dataset)` / `.list_fields(schema)` / `.list_publishers()` / `.get_dataset_range(dataset)` | カタログ、データセットにデータのある最初と最後の日付 |
| `Historical().metadata.get_dataset_condition(dataset, start_date, end_date)` | 各日付が available か missing か、自分の記録がカバーする時間帯（日本時間） |
| `Historical().metadata.get_record_count(...)` / `.get_cost(...)` | リクエストが返すレコード数、そのコスト（常に 0。無料で、上限あり） |
| `Ticker(code).candles(interval)` / `.margin(days)` | 証券会社の BRiSK（SBI、e支店）自身のローソク足（`5m` は当日のみ、`1d`/`1w`/`1mo` がどこまでさかのぼるかは BRiSK 次第）、信用残と貸株料 |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | 証券会社の BRiSK の市場データ |
| `briskapi.brisk.login(site=...)` / `.connect()` | SBI（既定）または e支店の BRiSK のセッションとライブフィード |
| `briskapi.passkey.enroll()` / `.login()` / `.forget()` | 証券会社の BRiSK へのパスキーでのサインイン |

価格は円単位の浮動小数点数で、ベンダーの「値なし」（0）は `None` になります。
時刻は取引日の日本時間の `datetime` です。数量は株数で、売買区分・フラグ・
ステータスはベンダーの値のままです。`raw=True` を指定すると、ベンダー形式
（`*_price10` は0.1円単位、`*_us` は日本時間0時からのマイクロ秒）で返します。
表形式の結果は dict のリストで、`.to_pandas()` で DataFrame に変換できます。
エラーは `briskapi.BriskError` と `briskapi.NotFoundError` です。最初の市場全体の
照会で記録のテーブルをメモリーに読み込みます（デモ全体で約2秒）。1銘柄の履歴は
その銘柄の分だけを読みます。

## コマンドライン

```sh
brisk live --web --codes 7203,6758          # 気配の更新ごとに JSON を1行出力（--raw でベンダー形式）
brisk sites | enroll | login | forget       # Chrome 経由のパスキーで証券会社の BRiSK にサインイン（試験的）
brisk live --sbi --codes 7203               # SBI証券の BRiSK。Cookie は BRISK_SBI_COOKIES（JSON）または `brisk login` が保存したもの。--trace-protocol で接続手順を表示（通信の詳細は BRISK_PROFILE）
brisk live --site e-shiten --codes 7203     # 立花証券 e支店の BRiSK Next。Cookie は BRISK_ESHITEN_COOKIES（JSON）または `brisk login` が保存したもの
brisk live --sbi --output days              # 日をまたいで動き続け、終わった日ごとに days/ に残し（例: sbi_live_20261013_085800-153512）、stderr に {"day": {...}} を1行出力
brisk live --sbi --quiet                    # 気配を出力せず、記録・アップロードと briskapi.Live() への配信だけ（--no-gateway で配信しない）
brisk record --web --output recordings/s1   # デモを記録（オプトアウトしない限りアップロード）
brisk list --date 20210927 --source historical_mock   # 自分の記録（--timing で自分のタイミングレポート、--deleted で過去30日間に削除したもの）
brisk pull PREFIX --output recordings/downloaded   # brisk list が返した prefix を使う
brisk delete PREFIX                         # 自分のアップロードを自分のアーカイブから削除（30日間は復元可能）
brisk restore PREFIX                        # 過去30日間に削除したアップロードを元に戻す（タイミングレポートのキーも可）
brisk prune recordings/s1                   # ディスクを空ける: アーカイブにある記録のローカルのコピーを削除
brisk export recordings/s1 --nautilus catalog      # NautilusTrader v2 のカタログ（または --jsonl FILE）
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13   # クラウド API：CSV を標準出力へ（--encoding json|parquet、--output FILE、--end、--limit）
brisk consent [--accept | --revoke]         # 自動アップロードの設定の表示・変更（--no-upload: 今回だけ）
brisk upload recordings/s1                  # 記録のアップロードを再試行
brisk identity [--recovery-code | --recover CODE]  # 自分の owner ID、復元コード、別のコンピューターで使う
```

各コマンドの詳細は `--help` で確認できます。`pull` はすべて検証してから
書き込み、既存のフォルダーを上書きすることはありません。

`brisk live --sbi` と `--site e-shiten` は、止めるまで動き続け、取引日ごとに1つの記録を作ります。
オプトアウトしない限り、終わった日ごとの市場データの記録とタイミングの要約を確認なしで
アップロードします。`--no-upload-market-data` で記録だけを、`--no-upload` で両方をローカルに
残せます。`brisk list --source sbi_live`（または `eshiten_live`）で自分の証券会社の記録を
一覧できます。どの `brisk live` も、`--no-gateway` を指定しない限り、このコンピューターの
`briskapi.Live()` にフィードを配信します（[ライブのレコード](#ライブのレコードdatabento-風)）。

## 自分専用のアーカイブ

アップロードすると、最後まで正常に終わったすべてのセッションのコピーを、お使いの
コンピューターの外に無料で持てます（到着時に検証し、デモの完全な再生は基準と一致する
必要があります）。アーカイブに入った記録は、`brisk prune DIR` でローカルのコピーを
削除でき（先に SHA-256 をアーカイブと照合します）、`brisk pull` でこのコンピューターにも
別のコンピューターにも戻せます。アップロードすると、[クラウド API](#クラウドでアーカイブを照会する)と、
クラウドにしかない OHLCV バーも使えます。アップロードは `archive/OWNER/YYYYMMDD/FROM-TO-SHA256`
（`brisk list` が返す prefix）の下に保存されます。FROM と TO は記録の最初と最後のソース時刻
（日本時間、HHMMSS）なので、その日の記録は時刻順に並びます。

アップロードしたものは非公開で、一覧・ダウンロード・削除ができるのはご自身だけです。
アカウントはありません。必要になったとき、ツールがあなたの **ID**（ランダムな秘密の値）を
作り、アーカイブはその一方向ハッシュである owner ID の下にアップロードを保存します。
秘密の値は、システムの資格情報ストア（macOS のキーチェーンまたは Windows の資格情報
マネージャー）と、本人だけが読めるファイル `~/.config/brisk/identity.json` の2か所に
保存され、一方が失われてももう一方から戻ります。作成時には **復元コード**（英数字28文字）が
表示されるので、書き留めてください。`brisk identity --recovery-code` で再表示でき、
`brisk identity --recover CODE` で別のコンピューターでも使えます（`BRISK_IDENTITY` でも
指定可）。秘密の値を持つ人はアーカイブにとってあなた本人なので、他人に渡さないでください。
これも保存したコピーもなくすと、アップロードにはアクセスできなくなります。ほかの
ホスティングサービスと同様に、アーカイブの運営者は保存されたデータにアクセスできます。
詳しくは [PRIVACY.md](PRIVACY.md)（英語）をご覧ください。

アップロードは既定でオンです。コマンドラインで初めて記録またはライブセッションを始めると、
アップロードされる内容が表示され、一度だけ確認されます（Enter で同意、`n` で拒否）。答えは
保存され、それ以降は最後まで正常に終わったセッション（ライブフィードでは終わった日）が、改めて
確認することなく自動的にアップロードされます。確認できない場面（Python API や端末のないコマンド実行）では、
`brisk consent --accept` または `--revoke`（`briskapi.consent(accept=True)` または
`revoke=True`）で決めるまで、セッションは既定でアップロードされ、プロセスごとに一度その旨を
表示します（Python では警告）。

- **デモのセッションでアップロードされる内容:** ローカルの計測値（お使いの
  コンピューターの時計を含み、記録した日時がわかります）。デモの再生の市場データは誰にとっても
  同じなので、アーカイブは1つだけ保持します。デモのアップロードで送るのは自分のタイミングの
  テーブルだけ（約41 MB の代わりに約0.4 MB）で、アーカイブはそれを、検証済みのデモの市場データの
  テーブルと結び付け、`brisk pull` はこれまでどおり検証したうえで完全な記録を返します
  （そのアーカイブへの最初のデモのアップロードだけは、記録全体を送ります）。IP アドレスは
  リクエスト回数などの制限にのみ、鍵付きハッシュとして使います。
- **証券会社（SBI、e支店）のセッションでアップロードされる内容:** タイミングの要約（デコード
  時間、受信時のデータの遅れ、フレーム間隔のパーセンタイル、停滞回数、フレーム数、取引日、
  最初と最後の分。`briskapi.Archive().timing()` で一覧できます）と、取引日が終わった時点で
  その日のデコードした市場データの記録（銘柄マスター、価格、数量、板、約定、銘柄コード、
  ローカルの計測値）。Cookie、トークン、接続の診断情報はアップロードしません。その日の約定からは、
  クラウドで自分の記録だけから OHLCV バーが作られます（[下記](#クラウドでアーカイブを照会する)）。
- **アップロードをやめる:** 1回だけなら `--no-upload`（Python では `upload=False`）。
  `--no-upload-market-data`（`upload_market_data=False`）は証券会社の記録の市場データだけを
  ローカルに残します。その環境のすべての実行では `BRISK_UPLOAD=0`、恒久的には
  `brisk consent --revoke`。
- **削除と復元:** `brisk delete PREFIX` でアップロードを自分のアーカイブから削除し、一覧にも
  ダウンロードにも出なくなります。30日間は元に戻せます。`brisk list --deleted` で削除したもの
  （`deleted_at` と `restore_until` 付き）を一覧でき、`brisk restore PREFIX`（タイミングレポートの
  キーも可）で復元します。30日を過ぎると復元できなくなり、削除されます。
- 途中までの再生（`--limit-frames`）や、その日が終わる前に閉じた記録はアップロードされません。
  途中で止めた証券会社のフィードは、タイミングの要約だけをアップロードします。

## クラウドでアーカイブを照会する

アップロードすると、自分の記録を対象にしたクラウド API も使えます。Databento の履歴データ API の
形（名前もパラメーターも同じ）に倣っています。API キーは自分のアーカイブの ID なので、サインアップも
キーの作成も要らず、設定もありません。`briskapi.Historical()` はアップロードのときと同じ方法で ID を
見つけます（なければ新しく作ります）。

```python
import briskapi

client = briskapi.Historical()
trades = client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", end="2026-10-17",
                                     symbols=["7203"], schema="trades")
trades.to_df()      # ts_recv をインデックスにした pandas の DataFrame。to_ndarray()、to_csv()、to_json()、to_parquet()、replay() も使えます
client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", symbols=["7203"],
                            schema="ohlcv-1m").to_df()     # 1分のバー（下記）
client.timeseries.get_range("XTKS.BRISKDEMO", start="2021-09-27T08:59", end="2021-09-27T09:03",
                            symbols=["7203"], schema="imbalance")    # デモ。どの ID でも照会できます
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # 各日付と、記録した時間帯
```

`get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None,
path=None)` はストアを返します。`path` を指定すると Parquet ファイルにも書き出します。
`client.metadata` には `list_datasets()`、`list_schemas(dataset)`、`list_fields(schema)`、
`list_publishers()`、`get_dataset_range(dataset)`、`get_dataset_condition(dataset, start_date, end_date)`
（各日付が available か missing か、自分の記録がカバーする時間帯を日本時間で返します）、
`get_record_count(...)`、`get_cost(...)` があります。`get_cost` は常に 0 です。クラウド API は無料で、
1つの ID あたり1分に60リクエスト、1日に 2 GB まで、1つの IP アドレスあたり1分に120リクエスト、
1日に 4 GB までの上限があります。

- **データセット**は Databento の `VENUE.FEED` という命名に従います（XTKS は東京証券取引所）。
  `XTKS.BRISK` は自分のライブ記録で、どの証券会社の BRiSK 経由でも同じです。`XTKS.BRISKDEMO` は
  公開デモ（2021-09-27）で、どの ID からでも照会できます。
- **スキーマ:** `trades`、`mbp-10`（各側10本。BRiSK は注文イベントではなく板を返すので、`action` と
  `side` は `N`）、`definition`、`imbalance`（寄付きと引けの板寄せの状態：予想約定価格・数量、
  成行注文の売買差）、`brisk-quotes`（BRiSK の気配のすべての項目）、そして OHLCV バーの
  `ohlcv-1s`、`ohlcv-1m`、`ohlcv-1h`、`ohlcv-1d`。`list_fields(schema)` でスキーマの列を確認できます。
- **時刻:** 日付はその日の日本時間の取引日、タイムゾーンのない時刻は日本時間、整数は UTC のナノ秒を
  表します。`end` は含まず、省略すると start の取引日の終わりまでです。1回のリクエストで扱えるのは
  最大31日、2,000銘柄までです。
- **銘柄**は JPX の銘柄コードです。`instrument_id` はコードごとに固定です。数字だけのコードはその数値
  （7203）、それ以外は 100,000 にコードの36進数の値を足したものです（`167A` は 154694）。

**`Historical` の OHLCV バーはクラウドにしかなく、** 手元のファイルから作ることはありません（`Live` は
ストリームから作ります）。アーカイブは取引日ごとに、自分の現在のライブ記録から
バーを作ります（各約定は1回だけ数えます）。記録をアップロード、削除、復元するたびに作り直すので、
照会では作成済みのバーを読むだけです。含まれるのは、記録がその銘柄と、取引時間（前場 09:00〜11:30 と
後場 12:30〜15:30、日本時間）内のそのバーの全期間をカバーしているバーだけです。たとえば 08:55 から
11:31 までの記録なら、午前の1分と1時間のバーは受け取れますが、1日のバーは受け取れません。つまり、
バーを得るにはアップロードが必要です。他の人のアップロードが自分のバーに入ることはなく、自分の
アップロードが他の人のバーに入ることもありません（`XTKS.BRISKDEMO` のバーは、デモ自身の約定から
作ります）。詳しくは [PRIVACY.md](PRIVACY.md)（英語、ポリシーバージョン7）をご覧ください。`Ticker.candles()` は別のもので、
証券会社の BRiSK 自身のバーです（証券会社へのログインが必要）。

コマンドラインでは、`brisk query` は CSV を標準出力に書きます。`--encoding json` または `parquet` で
変更でき（Parquet は `--output FILE` が必要）、`--output FILE` でファイルに書き、`--end` と `--limit` で
リクエストの範囲を絞ります。

```sh
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13 > toyota-1m.csv
```

その裏にある HTTP API は [docs/cloud-api.md](docs/cloud-api.md)（英語）に説明があります。同じ
レコードは、手元のフィードから `briskapi.Live` でライブでも受け取れます（[上記](#ライブのレコードdatabento-風)）。

## その他のドキュメント（英語）

- [docs/data-format.md](docs/data-format.md): 記録とクラウド API のレコードの全列、その読み方
- [ARCHITECTURE.md](ARCHITECTURE.md): 仕組み、データ形式、アーカイブの改ざん対策と制限
- [PRIVACY.md](PRIVACY.md): プライバシーポリシー
- [CONTRIBUTING.md](CONTRIBUTING.md): 開発、テスト、リリース
- [tools/brisk_mock/README.md](tools/brisk_mock/README.md): フィールド定義、タイミングとレイテンシー、SBI のマスター配置の確認、任意の Rust コレクター
- [infra/README.md](infra/README.md): アーカイブのデプロイ
- [THIRD_PARTY.md](THIRD_PARTY.md): デコーダー、データ、pybrisk の権利表示

ソフトウェアは MIT ライセンスです。
