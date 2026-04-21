#!/usr/bin/env bash
# Refresh layer/python/mfd from ../backtest after you change the pipeline or sketch.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
BT="$ROOT/../backtest"
cp "$BT/sketch_quantile.py" "$ROOT/layer/python/mfd/sketch_quantile.py"
cp "$BT/polymarket_smart_money_backtest.py" "$ROOT/layer/python/mfd/pipeline.py"
cp "$BT/incremental_match.py" "$ROOT/layer/python/mfd/incremental_match.py"
PIPE="$ROOT/layer/python/mfd/pipeline.py"
INC="$ROOT/layer/python/mfd/incremental_match.py"
python3 << PY
from pathlib import Path
p = Path("$PIPE")
text = p.read_text(encoding="utf-8")
block = """_BACKTEST_DIR = os.path.dirname(os.path.abspath(__file__))
if _BACKTEST_DIR not in sys.path:
    sys.path.insert(0, _BACKTEST_DIR)
from sketch_quantile import build_log_spaced_edges, histogram_large_cut_fixed
"""
repl = "from mfd.sketch_quantile import build_log_spaced_edges, histogram_large_cut_fixed\n"
if block in text:
    text = text.replace(block, repl)
else:
    text = text.replace(
        "from sketch_quantile import build_log_spaced_edges, histogram_large_cut_fixed",
        "from mfd.sketch_quantile import build_log_spaced_edges, histogram_large_cut_fixed",
    )
p.write_text(text, encoding="utf-8")
PY
python3 << PY
from pathlib import Path
p = Path("$INC")
text = p.read_text(encoding="utf-8")
text = text.replace("from sketch_quantile import", "from mfd.sketch_quantile import")
text = text.replace("from polymarket_smart_money_backtest import", "from mfd.pipeline import")
p.write_text(text, encoding="utf-8")
PY
echo "Synced layer/python/mfd from backtest/"
