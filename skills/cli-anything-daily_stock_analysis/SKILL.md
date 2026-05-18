---
name: cli-anything-daily_stock_analysis
description: Stateful CLI harness for daily_stock_analysis — stock discovery, backtesting, and analysis automation
version: 1.0.0
type: cli
commands:
  - group: project
    subcommands: [new, open, save, info, list]
    description: Project/session file management (.dsa.json)
  - group: analyze
    subcommands: [run, status]
    description: Run stock analysis pipeline
  - group: discover
    subcommands: [run]
    description: Stock discovery scanning (intraday/postmarket)
  - group: backtest
    subcommands: [run]
    description: Factor backtesting and optimization
  - group: serve
    subcommands: [start]
    description: FastAPI server management
  - group: report
    subcommands: [list, view, export]
    description: Report management and export
  - group: session
    subcommands: [undo, redo, status, save]
    description: Session state with undo/redo
  - group: config
    subcommands: [env, get]
    description: Configuration and environment management
global_options:
  - --json: Machine-readable JSON output
  - -p/--project: Path to project file (.dsa.json)
entry_point: cli-anything-daily_stock_analysis
---

# cli-anything-daily_stock_analysis

Stateful CLI harness for the daily_stock_analysis stock analysis system. Provides command-line access to stock discovery, factor backtesting, analysis pipelines, and report management.

## Installation

```bash
cd agent-harness
pip install -e .
```

Verify:

```bash
cli-anything-daily_stock_analysis --help
```

## Quick Start

```bash
# Create a project and run analysis
cli-anything-daily_stock_analysis project new --name "my-scan" --stocks "600519,000858"
cli-anything-daily_stock_analysis analyze run

# Run discovery
cli-anything-daily_stock_analysis discover run --mode postmarket

# Start interactive REPL
cli-anything-daily_stock_analysis

# Machine-readable output
cli-anything-daily_stock_analysis --json analyze status
```

## Global Options

| Option | Description |
|--------|-------------|
| `--json` | Output machine-readable JSON instead of human-readable text |
| `-p, --project PATH` | Open a project file (.dsa.json) |

## Command Groups

### project — Project Management

```bash
cli-anything-daily_stock_analysis project new --name "scan-2024" --stocks "600519,000858" --mode postmarket
cli-anything-daily_stock_analysis project open ./my-scan.dsa.json
cli-anything-daily_stock_analysis project info
cli-anything-daily_stock_analysis project list --directory ./scans/
cli-anything-daily_stock_analysis project save --output ./output.dsa.json
```

### analyze — Stock Analysis

```bash
cli-anything-daily_stock_analysis analyze run --stocks "600519,AAPL" --debug
cli-anything-daily_stock_analysis analyze run --dry-run
cli-anything-daily_stock_analysis analyze run --market-review
cli-anything-daily_stock_analysis --json analyze status
```

### discover — Stock Discovery

```bash
cli-anything-daily_stock_analysis discover run --mode postmarket
cli-anything-daily_stock_analysis discover run --mode intraday --full
```

### backtest — Factor Backtesting

```bash
cli-anything-daily_stock_analysis backtest run --code 600519
cli-anything-daily_stock_analysis backtest run --days 60 --force
```

### serve — Server Management

```bash
cli-anything-daily_stock_analysis serve start --port 8000
cli-anything-daily_stock_analysis serve start --webui --dev
```

### report — Report Management

```bash
cli-anything-daily_stock_analysis report list
cli-anything-daily_stock_analysis report view ./reports/analysis_20260518.md
cli-anything-daily_stock_analysis report export --preset json --output results.json
cli-anything-daily_stock_analysis report export --preset csv --output results.csv
cli-anything-daily_stock_analysis report export --preset md --output report.md
```

### session — Session State

```bash
cli-anything-daily_stock_analysis session status
cli-anything-daily_stock_analysis session undo
cli-anything-daily_stock_analysis session redo
cli-anything-daily_stock_analysis session save --path ./session.json
```

### config — Configuration

```bash
cli-anything-daily_stock_analysis config env
cli-anything-daily_stock_analysis config get TUSHARE_TOKEN
cli-anything-daily_stock_analysis config get LLM_PROVIDER
```

## REPL Mode

```bash
cli-anything-daily_stock_analysis
```

Type `help` for commands, `exit` to quit.

## Programmatic Usage

```bash
$ cli-anything-daily_stock_analysis --json project new --name "test" --stocks "000001"
{"status": "created", "name": "test", "stocks": 1, "mode": "postmarket"}

$ cli-anything-daily_stock_analysis --json analyze status
{"project_root": "/path/to/project", "status": "ok", ...}
```

## Testing

```bash
python -m pytest cli_anything/daily_stock_analysis/tests/ -v
```

35 tests: 21 unit (core modules) + 14 E2E (subprocess integration).
