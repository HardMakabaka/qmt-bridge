from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path

SectorConfigSignature = tuple[tuple[str, int, int], ...]


def _sector_root(local_dat_root: Path) -> Path:
    if local_dat_root.name.casefold() == "sector":
        return local_dat_root
    return local_dat_root / "Sector"


def _sector_config_signature(local_dat_root: Path) -> SectorConfigSignature:
    root = _sector_root(local_dat_root)
    try:
        config_paths = sorted(root.glob("*/sectorConfig.xml"))
    except OSError:
        return ()
    signature: list[tuple[str, int, int]] = []
    for path in config_paths:
        try:
            stat = path.stat()
        except OSError:
            continue
        signature.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


@lru_cache(maxsize=16)
def _sector_paths(signature: SectorConfigSignature) -> Mapping[str, Path]:
    sector_paths: dict[str, Path] = {}
    for path_text, _modified_ns, _size in signature:
        config_path = Path(path_text)
        try:
            tree = ET.parse(config_path)
        except (ET.ParseError, OSError):
            continue
        for item in tree.getroot().iter("Item"):
            if item.get("type") != "2":
                continue
            name = str(item.get("name") or "").strip()
            if not name or name in sector_paths:
                continue
            sector_paths[name] = config_path.parent / name
    return sector_paths


def _current_sector_paths(local_dat_root: Path) -> Mapping[str, Path]:
    return _sector_paths(_sector_config_signature(local_dat_root))


def read_qmt_sector_names(local_dat_root: Path) -> list[str]:
    return list(_current_sector_paths(local_dat_root))


def read_qmt_sector_stocks(
    local_dat_root: Path,
    sector_name: str,
) -> list[str] | None:
    target = str(sector_name or "").strip()
    if not target:
        return None
    membership_path = _current_sector_paths(local_dat_root).get(target)
    if membership_path is None:
        return None
    try:
        raw = membership_path.read_text(encoding="utf-8-sig")
    except OSError:
        return []
    stocks: list[str] = []
    seen: set[str] = set()
    for value in re.split(r"[,\s]+", raw):
        stock = value.strip().upper()
        if not stock or stock in seen:
            continue
        seen.add(stock)
        stocks.append(stock)
    return stocks


__all__ = ["read_qmt_sector_names", "read_qmt_sector_stocks"]
