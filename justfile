# QMT Bridge — 项目快捷命令
# 使用: just <命令>  |  just --list 查看所有命令

# Windows 下使用 PowerShell 作为默认 shell
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
python := ".\\.venv\\Scripts\\python.exe"

# 默认命令：列出所有可用命令
default:
    @just --list

# ─────────────────────────── 安装 ───────────────────────────

# 安装项目（仅客户端，零依赖）
install:
    {{python}} -m pip install -e .

# 安装服务端全部依赖
install-server:
    {{python}} -m pip install -e ".[full]"

# 安装文档依赖
install-docs:
    {{python}} -m pip install -e ".[docs]"

# 安装仪表盘依赖
install-dashboard:
    {{python}} -m pip install -e ".[dashboard]"

# 安装全部依赖（服务端 + 文档 + 仪表盘）
install-all:
    {{python}} -m pip install -e ".[full,docs,dashboard]"

# ─────────────────────────── 服务 ───────────────────────────

# 启动 API 服务（前台，Ctrl+C 停止）
serve *ARGS:
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/manage-bridge.ps1 start -Foreground {{ARGS}}

# 启动 API 服务（指定端口）
serve-port port="13543":
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/manage-bridge.ps1 start -Foreground --port {{port}}

# 启动 API 服务（调试模式）
serve-debug:
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/manage-bridge.ps1 start -Foreground --log-level debug

# 停止由 manage-bridge 启动且身份匹配的 API 服务
serve-stop:
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/manage-bridge.ps1 stop

serve-status:
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/manage-bridge.ps1 status

# ─────────────────────────── 仪表盘 ─────────────────────────

# 启动可视化仪表盘（http://localhost:8501）
dashboard:
    streamlit run dashboard/app.py

# ─────────────────────────── 文档 ───────────────────────────

# 本地预览 MkDocs 文档站点（http://127.0.0.1:8001）
docs:
    mkdocs serve -a 127.0.0.1:8001

# 构建 MkDocs 静态站点到 site/
docs-build:
    mkdocs build -d site/

# pdoc 本地预览客户端 API（http://localhost:8002）
docs-pdoc:
    pdoc src/qmt_bridge/client/ -p 8002

# 一键构建 MkDocs + pdoc
docs-all:
    @echo "==> 构建 MkDocs 文档..."
    mkdocs build -d site/
    @echo "==> 构建 pdoc API 参考..."
    pdoc --html --output-dir site/pdoc src/qmt_bridge/client/
    @echo "==> 完成！"
    @echo "    MkDocs: site/index.html"
    @echo "    pdoc:   site/pdoc/qmt_bridge/client/index.html"

# 清理文档构建产物
docs-clean:
    rm -rf site/

# ─────────────────────────── 测试 ───────────────────────────

# 运行测试
test *ARGS:
    {{python}} -m pytest tests/ {{ARGS}}

# 运行测试（verbose）
test-v:
    {{python}} -m pytest tests/ -v

# ─────────────────────────── 代码质量 ───────────────────────

# 类型检查（需要 mypy）
typecheck:
    {{python}} -m basedpyright

# 格式化代码（需要 ruff）
fmt:
    {{python}} -m ruff format src/ tests/

# 代码检查（需要 ruff）
lint:
    {{python}} -m ruff check src/ tests/

# 检查不改写源码；格式化必须显式执行 fmt
check: lint typecheck

# ─────────────────────────── 构建 ───────────────────────────

# 构建 wheel 和 sdist
build:
    {{python}} -m build

# 发布到 TestPyPI（首次验证用）
publish-test: build
    {{python}} -m twine upload --repository testpypi dist/*

# 发布到 PyPI
publish: build
    {{python}} -m twine upload dist/*

# 清理构建产物
clean:
    rm -rf dist/ build/ site/ *.egg-info src/*.egg-info
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ─────────────────────────── 信息 ───────────────────────────

# 显示项目版本
version:
    @{{python}} -c "from qmt_bridge._version import __version__; print(__version__)"
