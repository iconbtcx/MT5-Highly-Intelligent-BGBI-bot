# BGBI V3.5 + Kronos-Mini — screenshot and integration review

## Trade evidence visible in the supplied screenshots

This is a manual transcription of the visible rows only, not a complete MT5 report.

- Visible closed trades: 21
- Positive results: 4
- Negative results: 17
- Gross visible profit: approximately $4.39
- Gross visible loss: approximately $79.25
- Net visible result: approximately -$74.86
- Visible-sample profit factor: approximately 0.055 (gross profit / gross loss)

The visible sample is weak and cannot establish full-history expectancy. It is enough to justify pausing further live deployment while the actual EA build, set files and complete Strategy Tester report are reconciled.

The open-trade screenshot shows a displayed balance of $9,150.89, equity of $9,145.39, floating P/L of -$5.50, about five open positions and nine pending orders at capture time. Those figures are a snapshot and may change; they are not a complete account-risk report.

The Experts screenshot contains the diagnostic label `V3.2.4`, repeated CHOP waits despite expansion evidence, and stale crypto-bridge stream warnings on charts where flow is disabled. This means the screenshot was not running the same source build used as the base for this V3.5 package. The package is based on the uploaded V3.4 HTF-closed-confluence source. Do not load its sets into a V3.2.4 EA and assume the later logic is active.

## V3.5 implementation contents

- Shared Python process for Kronos-Mini and its 2k tokenizer; the model is loaded once for all configured symbols.
- Forecasts from completed MT5 broker candles for M1, M5 and M15, with source-bar age validation.
- Forecast-only endpoint on `127.0.0.1:8798`; no order placement endpoint.
- Bounded advisory shift to the existing FastTrend score; Kronos does not set technical direction or increase lot size.
- Optional confirmation mode and constrained CHOP expansion continuation that requires M1/M5 Kronos agreement, M5 technical alignment, no direct M15 opposition, existing expansion evidence and all existing EA risk gates.
- Forex/gold/index health initialisation no longer marks intentionally disabled crypto streams as stale.
- 11 preset files with all 446 EA input keys represented; no missing or unknown preset keys in the static check.

## Validation status

Passed checks:

- ZIP integrity check.
- Python syntax parsing for all Python files in the package.
- Balanced MQL lexical delimiters and no duplicated EA input declarations in the static source check.
- All 11 presets contain 446 keys and align with the source input names according to the static checker.
- Kronos bridge uses a pandas Series for future timestamps (required by the uploaded Kronos timestamp feature builder), checks quote freshness and fails closed on stale data or a forecast error.

Not yet verified:

- The EA has not been compiled by MetaEditor in this environment.
- The package has not been run in MT5 Strategy Tester or demo trading.
- The normal EA's HTTP calls are disabled in Strategy Tester; historical Kronos scoring therefore needs an offline timestamped replay feed before Strategy Tester can measure the AI integration. Do not interpret a V3.4 tester run as a V3.5+Kronos test.
- No profitability, PF or win-rate improvement is claimed.

## Recommended trial sequence

1. Do not replace the EA on the account that currently has positions or pending orders. Keep V3.4 and its presets as a separate baseline.
2. Use a separate demo terminal/account and first verify that the actual symbol names match the launch script. The supplied default symbols are `btcusd,ethusd,bnbusd,solusd,xrpusd,eurusd,gbpusd,usdcad,usdjpy,xauusd,de40`; change them to the exact Market Watch names if broker suffixes exist.
3. Compile the V3.5 EA in MetaEditor; resolve all compile errors before attaching it.
4. Set the allowed WebRequest URL in MT5 to `http://127.0.0.1:8798`; preserve the existing bridge allowlist.
5. Run `setup_kronos_bridge.bat`, then `start_kronos_bridge.bat`. First launch downloads model weights/tokenizer from Hugging Face. Verify `/health` says both model and MT5 are connected, and verify `/snapshot?symbol=YOUR_EXACT_SYMBOL` has fresh valid timeframes.
6. Start with advisory mode on demo and collect logs. Compare the same dates and costs against the existing bot without Kronos. Treat the AI CHOP escape as experimental and do not infer profitability from a few trades.
7. Separately A/B test the ratchet/exit settings after the AI comparison. Keep one major change per experiment so the cause of any improvement or degradation remains identifiable.

## Important

This is a demo-validation candidate, not a production-certified build. Kronos is a probabilistic forecast model; forecasts are neither win probabilities nor guaranteed trading signals. Existing stop-loss, lot-size and account risk limits remain mandatory.
