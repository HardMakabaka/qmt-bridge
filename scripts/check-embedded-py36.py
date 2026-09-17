import ast
from pathlib import Path


root = Path(__file__).resolve().parents[1]
embedded_files = sorted((root / "src" / "bigqmt_signal_trader").rglob("*.py"))
embedded_files.extend(
    [
        root / "src" / "bigqmt_signal_trader_redis_rpc_runtime.py",
        root / "src" / "bigqmt_signal_trader_strategy.py",
        root / "src" / "BIGQMT_REDIS_DRYRUN.py",
        root / "src" / "MECOSTOCK_BIGQMT_ZMQ.py",
        root / "src" / "mecostock_bigqmt_mode_overlay.py",
    ]
)

for source_path in embedded_files:
    source = source_path.read_text(encoding="utf-8-sig")
    tree = ast.parse(
        source,
        filename=str(source_path.relative_to(root)),
        feature_version=(3, 6),
    )
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            if any(alias.name == "annotations" for alias in node.names):
                raise SyntaxError(
                    f"{source_path.relative_to(root)} imports unsupported "
                    "__future__.annotations for Python 3.6"
                )

print(f"validated {len(embedded_files)} embedded files against Python 3.6 grammar")
