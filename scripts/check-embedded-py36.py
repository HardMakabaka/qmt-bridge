import ast
from pathlib import Path


root = Path(__file__).resolve().parents[1]
embedded_files = sorted((root / "src" / "bigqmt_signal_trader").rglob("*.py"))
embedded_files.extend(
    [
        root / "src" / "bigqmt_signal_trader_redis_rpc_runtime.py",
        root / "src" / "bigqmt_signal_trader_strategy.py",
    ]
)

for source_path in embedded_files:
    ast.parse(
        source_path.read_text(encoding="utf-8-sig"),
        filename=str(source_path.relative_to(root)),
        feature_version=(3, 6),
    )

print(f"validated {len(embedded_files)} embedded files against Python 3.6 grammar")
