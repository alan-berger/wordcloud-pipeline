#!/usr/bin/env python3
from __future__ import annotations
"""
wordcloud_pipeline.py
---------------------
A configurable, automated pipeline that polls a remote URL for new
incrementally-numbered text files, generates a wordcloud image from each one,
optionally patches metadata into an HTML/PHP page, and sends a push
notification via ntfy when a new file is processed.

Originally built for the Security Now podcast (https://www.grc.com/securitynow.htm)
but designed to work with any regularly-published, incrementally-numbered text source.

Configuration is handled entirely through config.ini — no Python knowledge required.
See config.ini.example for a fully annotated template.

Requirements:
    Python 3.9+
    wordcloud  (pip install wordcloud)

Usage:
    python wordcloud_pipeline.py [--config /path/to/config.ini]

Cron example (runs twice daily):
    0 6,18 * * * /path/to/venv/bin/python /path/to/wordcloud_pipeline.py >> /path/to/pipeline.log 2>&1
"""

import argparse
import configparser
import html
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime

# ---------------------------------------------------------------------------
# Defaults — used when config.ini omits a value
# ---------------------------------------------------------------------------

DEFAULTS = {
    "pipeline": {
        "url_template":     "",
        "seed_number":      "0",
        "output_dir":       "./output",
        "latest_filename":  "wordcloud-latest.png",
        "archive_filename": "wordcloud-{n}.png",
    },
    "wordcloud": {
        "width":      "800",
        "height":     "800",
        "background": "white",
        "mask":       "",
        "stopwords":  "",
        "cli_path":   "",
    },
    "metadata": {
        "title_pattern": r"^TITLE:\s+(?P<value>.+)$",
        "date_pattern":  r"^DATE:\s+(?P<value>.+)$",
        "date_format":   "%B %d, %Y",
    },
    "html_patch": {
        "enabled":      "false",
        "file":         "",
        "backup_count": "3",
    },
    "ntfy": {
        "enabled": "false",
        "server":  "https://ntfy.sh",
        "topic":   "",
    },
}

# ---------------------------------------------------------------------------
# Logging — lines are printed to stdout (captured by cron >> pipeline.log)
# and also accumulated in _log_buffer so they can be sent via ntfy.
# ---------------------------------------------------------------------------

_log_buffer: list[str] = []


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    _log_buffer.append(line)
    print(line, flush=True)


def flush_log_buffer() -> str:
    """Return all buffered log lines as a single string and clear the buffer."""
    text = "\n".join(_log_buffer)
    _log_buffer.clear()
    return text

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def resolve_path(raw: str, base_dir: str) -> str:
    """Resolve a path from config: absolute paths are used as-is,
    relative paths are resolved relative to the script's directory."""
    if not raw:
        return ""
    if os.path.isabs(raw):
        return raw
    return os.path.normpath(os.path.join(base_dir, raw))


def load_config(config_path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()

    # Apply defaults
    for section, values in DEFAULTS.items():
        cfg[section] = values

    if not os.path.exists(config_path):
        log(f"ERROR: config file not found at {config_path}")
        sys.exit(1)

    cfg.read(config_path)
    return cfg


def get_wordcloud_cli(cfg: configparser.ConfigParser, base_dir: str) -> str:
    """Return the path to wordcloud_cli, auto-detecting if not specified."""
    cli = cfg.get("wordcloud", "cli_path", fallback="").strip()
    if cli:
        return resolve_path(cli, base_dir)
    return (
        shutil.which("wordcloud_cli")
        or os.path.expanduser("~/.local/bin/wordcloud_cli")
        or ""
    )

# ---------------------------------------------------------------------------
# State — tracks the last successfully processed number
# ---------------------------------------------------------------------------

def get_state_path(base_dir: str) -> str:
    return os.path.join(base_dir, "state.txt")


def get_last_number(base_dir: str, seed: int) -> int:
    state_file = get_state_path(base_dir)
    if os.path.exists(state_file):
        try:
            return int(open(state_file).read().strip())
        except ValueError:
            pass
    return seed


def set_last_number(base_dir: str, n: int) -> None:
    with open(get_state_path(base_dir), "w") as f:
        f.write(str(n))

# ---------------------------------------------------------------------------
# Remote text fetching
# ---------------------------------------------------------------------------

def fetch_text(url_template: str, n: int) -> str | None:
    """
    Attempt to download the text file for number n.
    Returns the content on success, None if not yet available (404) or on error.
    """
    url = url_template.format(n=n)
    log(f"Fetching: {url}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None  # not published yet — normal
        log(f"HTTP {e.code} fetching {url}: {e}")
    except Exception as e:
        log(f"Error fetching {url}: {e}")
    return None

# ---------------------------------------------------------------------------
# Metadata parsing
# ---------------------------------------------------------------------------

def parse_metadata(text: str, n: int, cfg: configparser.ConfigParser) -> dict:
    """
    Optionally extract a title and date from the text file using the
    regex patterns defined in config.ini [metadata].

    Both patterns must use a named capture group called 'value', e.g.:
        title_pattern = ^TITLE:\\s+(?P<value>.+)$
        date_pattern  = ^DATE:\\s+(?P<value>.+)$

    If a pattern is blank or does not match, sensible fallback values are used.

    Returns a dict with:
        n               int    1069
        title           str    "You can't hide from LLMs"
        title_html      str    "You can&#39;t hide from LLMs"
        date_display    str    "10 Mar 2026"   (empty string if unavailable)
        date_version    str    "20260310"      (empty string if unavailable)
    """
    meta = {
        "n":            n,
        "title":        f"#{n}",
        "title_html":   f"#{n}",
        "date_display": "",
        "date_version": "",
    }

    title_pattern = cfg.get("metadata", "title_pattern", fallback="").strip()
    date_pattern  = cfg.get("metadata", "date_pattern",  fallback="").strip()
    date_format   = cfg.get("metadata", "date_format",   fallback="%B %d, %Y").strip()

    if title_pattern:
        m = re.search(title_pattern, text, re.MULTILINE)
        if m:
            try:
                title = m.group("value").strip()
                meta["title"] = title
                escaped = html.escape(title, quote=False)
                meta["title_html"] = escaped.replace("&#x27;", "&#39;")
            except IndexError:
                log("Warning: title_pattern matched but has no 'value' group — check your regex")

    if date_pattern:
        m = re.search(date_pattern, text, re.MULTILINE)
        if m:
            try:
                raw_date = m.group("value").strip()
                try:
                    dt = datetime.strptime(raw_date, date_format)
                    meta["date_display"] = dt.strftime("%-d %b %Y")
                    meta["date_version"] = dt.strftime("%Y%m%d")
                except ValueError:
                    log(f"Warning: could not parse date '{raw_date}' with format '{date_format}'")
                    meta["date_display"] = raw_date
            except IndexError:
                log("Warning: date_pattern matched but has no 'value' group — check your regex")

    return meta

# ---------------------------------------------------------------------------
# Wordcloud generation
# ---------------------------------------------------------------------------

def generate_wordcloud(text: str, n: int, cfg: configparser.ConfigParser,
                       base_dir: str) -> str | None:
    """
    Run wordcloud_cli to produce the per-number archive image.
    Returns the archive path on success, None on failure.
    """
    cli = get_wordcloud_cli(cfg, base_dir)
    if not cli or not os.path.isfile(cli):
        log(f"ERROR: wordcloud_cli not found (looked at '{cli}'). "
            "Install it with 'pip install wordcloud' or set cli_path in config.ini.")
        return None

    output_dir       = resolve_path(cfg.get("pipeline", "output_dir"), base_dir)
    archive_filename = cfg.get("pipeline", "archive_filename").replace("{n}", str(n))
    archive_path     = os.path.join(output_dir, archive_filename)

    os.makedirs(output_dir, exist_ok=True)

    mask      = resolve_path(cfg.get("wordcloud", "mask"),      base_dir)
    stopwords = resolve_path(cfg.get("wordcloud", "stopwords"), base_dir)
    width     = cfg.get("wordcloud", "width")
    height    = cfg.get("wordcloud", "height")
    bg        = cfg.get("wordcloud", "background")

    # wordcloud_cli requires a file path, not stdin
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt",
                                     delete=False, encoding="utf-8") as tmp:
        tmp.write(text)
        tmp_path = tmp.name

    cmd = [
        cli,
        "--width",      width,
        "--height",     height,
        "--background", bg,
        "--text",       tmp_path,
        "--imagefile",  archive_path,
    ]

    if mask and os.path.isfile(mask):
        cmd += ["--mask", mask]
    elif mask:
        log(f"Warning: mask file not found at '{mask}' — generating without mask")

    if stopwords and os.path.isfile(stopwords):
        cmd += ["--stopwords", stopwords]
    elif stopwords:
        log(f"Warning: stopwords file not found at '{stopwords}' — generating without stopwords")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    finally:
        os.unlink(tmp_path)

    if result.returncode != 0:
        log(f"wordcloud_cli failed (exit {result.returncode}):\n{result.stderr.strip()}")
        return None

    log(f"Saved archive : {archive_path}")
    return archive_path

# ---------------------------------------------------------------------------
# HTML/PHP file patching (optional — adapt regex to your own HTML structure)
# ---------------------------------------------------------------------------

def rotate_backups(path: str, count: int) -> None:
    """
    Rotate rolling backups before overwriting a file.
    With count=3: oldest (.bak3) is dropped, then .bak2->.bak3, .bak1->.bak2,
    and the current live file is copied to .bak1.
    """
    oldest = f"{path}.bak{count}"
    if os.path.exists(oldest):
        os.unlink(oldest)
        log(f"Dropped oldest backup: {oldest}")

    for i in range(count - 1, 0, -1):
        src = f"{path}.bak{i}"
        dst = f"{path}.bak{i + 1}"
        if os.path.exists(src):
            shutil.copy2(src, dst)
            log(f"Rotated backup: {src} -> {dst}")

    if os.path.exists(path):
        bak1 = f"{path}.bak1"
        shutil.copy2(path, bak1)
        log(f"Created backup : {bak1}")


def patch_html_file(meta: dict, cfg: configparser.ConfigParser) -> bool:
    """
    Patch dynamic episode metadata into an HTML/PHP file.

    This function is intentionally specific — the regex below is written for
    alanberger.me.uk's index.php structure. If you want to use this feature
    for your own site, adapt the pattern and replacer() to match your own HTML.

    The block being replaced looks like:

        generated from <a href="https://www.grc.com/sn/sn-NNNN.txt" ...>episode NNNN</a>
        DD Mon YYYY<br><br> <u><b>EPISODE TITLE</b></u>
        <br><br>
        <a href="https://twit.cachefly.net/audio/sn/snNNNN/snNNNN.mp3" ...>
        <img src="/images/securitynow-wordcloud.png?v=YYYYMMDD" ...>
    """
    target_file = cfg.get("html_patch", "file", fallback="").strip()
    backup_count = cfg.getint("html_patch", "backup_count", fallback=3)

    if not target_file:
        log("ERROR: html_patch.file is not set in config.ini")
        return False

    if not os.path.exists(target_file):
        log(f"ERROR: html_patch file not found at {target_file}")
        return False

    with open(target_file, "r", encoding="utf-8") as f:
        content = f.read()

    n    = meta["n"]
    n_s  = str(n)

    # -----------------------------------------------------------------------
    # CUSTOMISE THIS REGEX if you are adapting this script for your own site.
    #
    # The pattern captures 15 groups; odd-numbered groups are static anchors
    # that are preserved unchanged, even-numbered groups are the dynamic
    # values that get replaced.
    # -----------------------------------------------------------------------
    pattern = re.compile(
        r'(generated from <a href="https://www\.grc\.com/sn/sn-)'
        r'(\d+)'                                                    # [2]  transcript URL episode number
        r'(\.txt" target="_blank">episode )'
        r'(\d+)'                                                    # [4]  episode link text
        r'(</a> )'
        r'([^<]+)'                                                  # [6]  date string
        r'(<br><br> <u><b>)'
        r'([^<]+)'                                                  # [8]  episode title
        r'(</b></u>'
        r'\s*<br><br>\s*'
        r'<a href="https://twit\.cachefly\.net/audio/sn/sn)'
        r'(\d+)'                                                    # [10] MP3 folder number
        r'(/sn)'
        r'(\d+)'                                                    # [12] MP3 filename number
        r'(\.mp3" target="_blank"><img src="/images/securitynow-wordcloud\.png\?v=)'
        r'(\d+)'                                                    # [14] cache-buster date
        r'(")',
        re.DOTALL,
    )

    def replacer(m: re.Match) -> str:
        return (
            m.group(1)  + n_s                    +  # transcript URL episode
            m.group(3)  + n_s                    +  # link text episode
            m.group(5)  + meta["date_display"]   +  # date
            m.group(7)  + meta["title_html"]     +  # title
            m.group(9)  + n_s                    +  # MP3 folder
            m.group(11) + n_s                    +  # MP3 filename
            m.group(13) + meta["date_version"]   +  # cache buster
            m.group(15)                             # closing quote
        )

    new_content, count = re.subn(pattern, replacer, content)

    if count == 0:
        log(
            "ERROR: Could not find the expected block in the html_patch file.\n"
            "       The file may have been manually edited in a way that broke\n"
            "       the expected pattern, or you need to adapt the regex in\n"
            "       patch_html_file() to match your own HTML structure.\n"
            "       No changes made."
        )
        return False

    if count > 1:
        log(f"WARNING: Pattern matched {count} times (expected exactly 1). "
            "Aborting to avoid corrupting the file.")
        return False

    rotate_backups(target_file, backup_count)

    with open(target_file, "w", encoding="utf-8") as f:
        f.write(new_content)

    log(f"Patched {target_file}  n={n}  date={meta['date_display']}  title={meta['title']}")
    return True

# ---------------------------------------------------------------------------
# ntfy notification
# ---------------------------------------------------------------------------

def notify(title: str, body: str, cfg: configparser.ConfigParser) -> None:
    """
    POST a push notification to ntfy.
    Failures are logged but never raise — a notification error must not
    abort the pipeline after it has already succeeded.
    """
    server = cfg.get("ntfy", "server", fallback="https://ntfy.sh").rstrip("/")
    topic  = cfg.get("ntfy", "topic",  fallback="").strip()

    if not topic:
        log("WARNING: ntfy.topic is not set in config.ini — skipping notification")
        return

    url = f"{server}/{topic}"
    try:
        req = urllib.request.Request(
            url,
            data=body.encode("utf-8"),
            method="POST",
            headers={
                "Title":        title,
                "Priority":     "default",
                "Tags":         "loudspeaker",
                "Content-Type": "text/plain; charset=utf-8",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                log(f"ntfy notification sent: {title!r}")
            else:
                log(f"ntfy returned unexpected status {resp.status}")
    except Exception as e:
        log(f"ntfy notification failed (non-fatal): {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Automated wordcloud pipeline")
    parser.add_argument(
        "--config",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"),
        help="Path to config.ini (default: config.ini in the same directory as this script)",
    )
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cfg      = load_config(args.config)

    url_template = cfg.get("pipeline", "url_template", fallback="").strip()
    if not url_template:
        log("ERROR: pipeline.url_template is not set in config.ini")
        sys.exit(1)

    seed        = cfg.getint("pipeline", "seed_number", fallback=0)
    html_patch  = cfg.getboolean("html_patch", "enabled", fallback=False)
    ntfy_on     = cfg.getboolean("ntfy",       "enabled", fallback=False)
    output_dir  = resolve_path(cfg.get("pipeline", "output_dir"), base_dir)
    latest_name = cfg.get("pipeline", "latest_filename")

    log("=== wordcloud pipeline starting ===")
    log(f"Config  : {args.config}")
    log(f"URL     : {url_template}")

    last = get_last_number(base_dir, seed)
    log(f"Last processed number: {last}")

    processed = 0
    n = last + 1

    while True:
        log(f"Checking #{n}...")
        text = fetch_text(url_template, n)

        if text is None:
            log(f"#{n} not yet available — stopping.")
            break

        log(f"Text available ({len(text):,} chars)")

        # Parse metadata
        meta = parse_metadata(text, n, cfg)
        log(f"  Title: {meta['title']}")
        if meta["date_display"]:
            log(f"  Date : {meta['date_display']}")

        # Generate wordcloud archive
        log("Generating wordcloud...")
        archive_path = generate_wordcloud(text, n, cfg, base_dir)
        if archive_path is None:
            log("Wordcloud generation failed — stopping.")
            sys.exit(1)

        # Optionally patch HTML file
        if html_patch:
            log("Patching HTML file...")
            if not patch_html_file(meta, cfg):
                log("HTML patch failed — stopping.")
                sys.exit(1)

        # Both steps succeeded — safe to update the canonical latest image
        latest_path = os.path.join(output_dir, latest_name)
        shutil.copy2(archive_path, latest_path)
        log(f"Updated latest: {latest_path}")

        set_last_number(base_dir, n)
        processed += 1

        # Optionally send ntfy notification
        if ntfy_on:
            notif_title = f"Wordcloud #{n}"
            if meta["title"] != f"#{n}":
                notif_title = f"#{n} \u2013 {meta['title']}"
            notify(notif_title, flush_log_buffer(), cfg)

        n += 1

    if processed == 0:
        log("Nothing to do.")
    else:
        log(f"Done — processed {processed} item(s).")

    log("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
