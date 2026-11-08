BGBI SUPER PERFECT — V3.5.1 STOP-GEOMETRY PATCH (DEMO CANDIDATE)

BGBI-SUPER-PERFECT — V3.5 KRONOS-MINI DEMO CANDIDATE
======================================================

ACTIVE INSTRUMENT UNIVERSE (10)
--------------------------------
Crypto MT5 CFDs:
  BTCUSD, ETHUSD, BNBUSD, SOLUSD, XRPUSD

Forex:
  EURUSD, GBPUSD, USDCAD, USDJPY

Index:
  DE40

The active universe is 5 crypto + 4 FX + 1 index = 10 instruments.
The XAUUSD preset supplied separately by the user is preserved under
Optional-Not-In-Target/ and is NOT included in the default Kronos worker list
or the active Sets/ folder.

CRYPTO EXCHANGE-FEED WORKERS (5 ONLY)
-------------------------------------
  BTCUSDT -> http://127.0.0.1:8787
  SOLUSDT -> http://127.0.0.1:8788
  XRPUSDT -> http://127.0.0.1:8790
  ETHUSDT -> http://127.0.0.1:8795
  BNBUSDT -> http://127.0.0.1:8796

These Binance USDT symbols are the exchange-feed equivalents for the five
MT5 BTCUSD/ETHUSD/BNBUSD/SOLUSD/XRPUSD charts. The manager does not launch
LTCUSDT, XLMUSDT, ADAUSDT, XTZUSDT or DOGEUSDT.

BNBUSD:
  Binance is primary. Coinbase USD spot failover is intentionally disabled
  because no product mapping is configured in this bridge. The worker no
  longer aborts when the Coinbase product is empty.

PYTHON BRIDGES
--------------
  Crypto flow: BGBI-Crypto-Live-Flow-Bridge.py (5 workers)
  USD hybrid:  BGBI-USD-Live-Hybrid-Bridge-V1.1.py (read-only context)
  Kronos Mini: BGBI-Kronos-Mini-Bridge.py (10 active MT5 symbols)
  Kronos URL:  http://127.0.0.1:8798

The launch scripts use Bridges/.kronos-venv/Scripts/python.exe. Do not use
plain `python` if the packages were installed only into this virtual
environment.

FIRST-TIME SETUP
----------------
1. Install Python 3.12 64-bit and the Microsoft Visual C++ x64 Redistributable.
2. Open MT5 and allow the terminal to finish loading the target Market Watch
   symbols and their historical candles.
3. Run Bridges/setup_kronos_bridge.bat once. It installs the Kronos and
   websocket-client dependencies into the package virtual environment.
4. Run Bridges/start_all_bridges.bat and keep all three windows open.
5. Check:
     http://127.0.0.1:8798/health
     http://127.0.0.1:8798/snapshot?symbol=btcusd
   Confirm fresh forecasts on M1, M5 and M15 for the configured symbols.

PRESET CONVERSION NOTE
----------------------
The 10 active V3.5 presets were built from the 10 corresponding user-uploaded
V3.2.2 presets. Values for the 435 shared inputs were retained. The 11 inputs
added in V3.5 (Kronos configuration) were populated from the matching V3.5
preset template. Two V3.2.2-only V32LossGuardTick* keys are not present in the
V3.5 EA and are therefore not copied into V3.5 presets. The original XAUUSD
configuration is preserved separately as optional.

VALIDATION LIMITS
-----------------
- ZIP integrity, Python syntax, crypto worker list, preset key counts and
  symbol lists were checked during packaging.
- The EA has not been compiled in MetaEditor in this environment.
- The complete live bridge + EA integration has not been run on the user's MT5.
- Strategy Tester HTTP forecast replay is not implemented by this package;
  live forecast behaviour must not be mistaken for a historical backtest.
- Use on a demo account first. Keep V3.4/V3.2.2 originals untouched as backups.


V3.5.1 NOTE: The EA source in EA/ is V3.5.1 and corrects stop-distance contamination from SlippagePoints. All presets are retained. Compile the V3.5.1 EA in MetaEditor before demo use. Existing positions keep their existing SL/TP. Read Docs/V3.5.1-STOP-GEOMETRY-AND-TRADE-REVIEW.txt.
