# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FL_Checker_PY is a PyQt5 desktop application that validates SAP Functional Location (FL) hierarchies against technology-specific guidelines and uploads results to SAP. It is a Python port of an original AutoHotkey program.

## Commands

```bash
# Run the application
python main.py

# Build standalone executable (PyInstaller)
pyinstaller main.spec
```

There is no test suite or linter configured.

## Architecture

### Data Flow

Clipboard input → Generic mask validation → DataFrame creation (parse FL levels) → Uniqueness/duplicate checks → Country & technology verification → Technology-specific mask validation → Parent-child relationship check → Guideline regex matching → Generate SAP upload CSVs → (Optional) Upload to SAP via GUI automation.

### Core Modules

- **main.py** — `MainWindow` (PyQt5 GUI). Orchestrates the full validation pipeline: `extract_data()` → `validate_clipboard_data()` → `create_dataframe()` → `check_linee_guida()` → `upload_data()`. Contains `InverterSelectionDialog` for solar inverter type selection.
- **DF_Tools.py** — `DF_Tools` class. DataFrame manipulation: level parsing (`add_level_lunghezza`), column generation, CSV I/O, upload DataFrame creation (`create_df_from_lists_ZPMR_CONTROL_FLn`).
- **RE_tools.py** — `RE_tools` class. Regex-based FL validation: `filter_dataframe_by_regex()` splits FLs into SubStation/Common/Others categories, `Make_DF_RE_list()` builds regex DataFrames from Rules.csv and guideline files, `verifica_fl_con_regex_per_categorie()` validates FLs per category.
- **SAP_Connection.py** — `SAPGuiConnection` class. Win32COM-based SAP GUI automation with context manager support.
- **SAP_Transactions.py** — `SAPDataUpLoader` class. Uploads CSVs to 4 SAP tables: ZPMR_CONTROL_FL2, ZPMR_CONTROL_FLn, ZPMR_CTRL_ASS, ZPMR_TECH_OBJ.
- **ConfigWindow.py** — `ConfigWindow` (PyQt5 dialog). 9 validation toggles persisted to `validation_config.json`.
- **Config/constants.py** — Central configuration: file paths, upload headers, validation flags, SAP timeout.
- **utils/decorators.py** — `@error_logger` decorator for exception tracking and execution timing.
- **array_tools.py** — Win32 clipboard read with retry logic.
- **File_tools.py** — CSV value lookup utility.

### Key Conventions

- **FL format:** `XXX-YYYY-ZZ[-AA[-BB[-CC]]]` (3–6 hyphen-separated levels)
- **DataFrame columns:** `Livello_1` through `Livello_6`, `FL_Lunghezza`, `Check`, `Check_Result`
- **Technology codes:** `S` (Solar), `W` (Wind), `E` (Battery/BESS), `K` (CAS), `H` (Hydro)
- **Solar inverter types:** Central Inverter, String Inverter, Inverter Module — each has its own guideline CSV
- **Error pattern:** Methods return `(bool_success, DataFrame|None)` tuples
- **Guideline CSVs** in `Config/` define per-technology regex rules; `Rules.csv` defines general FL validation rules
- **Upload CSVs** are written to `FileUpLoad/` with semicolon-delimited format

### Dependencies

Python 3.13, pandas 2.2.3, PyQt5 5.15.11, pywin32 (win32com, win32clipboard), pyinstaller 6.1.2.
