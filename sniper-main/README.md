# Aviasales Fetcher

Simple CLI utility for querying Aviasales via the Travelpayouts API.

## Installation

```bash
pip install requests tabulate
```

## Usage

Set your credentials:

```bash
export TP_TOKEN=your_token
export TP_MARKER=12345
```

Then run:

```bash
python aviasales_fetcher.py WAW --dest JFK --depart 2024-09-10 --return-date 2024-09-20
```

## Automating daily checks

To fetch new offers automatically, use the `daily_runner.py` helper. It reads
settings from `config.json` by default and can be scheduled via `cron`.

Run it manually like this:

```bash
python daily_runner.py --config path/to/config.json
```

Add a cron entry to execute it every morning at 08:00:

```cron
# Run flight search daily
0 8 * * * cd ~/sniper && . .venv/bin/activate && python daily_runner.py
```

Adjust the path to match your environment and configuration file location.

## Uruchomienie

```bash
# 1. Klon repo + środowisko
git clone https://github.com/fpoplawski/sniper.git
cd sniper
python -m venv .venv && source .venv/bin/activate
pip install -r sniper-main/requirements.txt

# 2. Konfiguracja
cp config.example.json config.json      # uzupełnij swoje parametry
cp .env.example .env                    # tokeny/API‑keys

# 3. Start harmonogramu
python sniper-main/tasks.py
```
