# VuLeak

VuLeak is a Python-based tool designed to analyze developer code from a code vault, extract data hub credentials, and interact with database tables for querying, inserting, and deleting data. It supports both AI-based automation and manual control, with a focus on security, clarity, and usability.

---

## Features

- Code Analysis — Fetches code from a raw URL and extracts potential data hub table names using AI.
- Credential Management
  - AI-driven credential extraction (URL and Key)
  - Manual credential entry with secure key masking
- Table Operations — Query, insert, or delete rows with automatic schema inference
- Security — Keys are masked in logs while URLs remain visible
- Error Handling — Provides fallback table suggestions such as `users`, `profiles`, or `posts`
- RLS Debugging Mode — Optional master key input to bypass Row-Level Security
- Interactive CLI with Color-Coded Output
  - INFO: Cyan
  - INPUT: Blue
  - SUCCESS: Green
  - WARNING: Yellow
  - ERROR: Red

---

## Prerequisites

- Python 3.8 or higher
- Required Python packages:
  - `requests`
  - Standard modules: `subprocess`, `json`, `datetime`, `sys`, `time`, `re`, `itertools`, `threading`
- `curl` must be installed and available in the system `PATH`

Install dependencies:

```bash
pip install requests
```

---

## Setup

```bash
# Clone the repository
git clone https://github.com/mrxception/VuLeak.git
cd VuLeak

# Install dependencies
pip install -r requirements.txt  # If available

# Or install manually
pip install requests
```

---

## Usage

Run the tool using:

```bash
python vulnerability_checker.py
```

### Workflow Overview

1. Enter a Code Vault raw URL  
   Example:
   ```
   https://raw.githubusercontent.com/user/repo/main/code.py
   ```

2. Choose Credential Input Method  
   - Type `ai` for AI-based credential extraction from a second raw URL
   - Type `manual` to enter credentials directly

3. If manual mode is selected, provide:
   ```
   Data Hub URL: https://example.datahub
   Data Hub Key: ****************
   ```

4. Enter the schema name (default: `public`)

5. Interact with database tables  
   - View detected or fallback tables  
   - Query (e.g., `id=eq.1` or leave blank for all rows)  
   - Insert data through guided prompts based on inferred schema  
   - Delete a specific row or clear all rows with confirmation

6. Optionally enter a master key to bypass Row-Level Security restrictions

---

## Disclaimer

This tool is intended for educational and authorized security testing purposes only.  
Use it exclusively on systems and databases for which you have explicit permission.

---

## License

MIT License (or specify accordingly)

---

## Contributing

Contributions are welcome. Submit pull requests or report issues to suggest enhancements or fixes.

---
