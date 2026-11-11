#!/bin/bash
# Run daily at 6:30 AM by a launchd agent (see README), which appends output to logfile.log.
# After the scrape, the result is reported to the Trackstar admin Jobs page (if ~/.trackstar-jobs-token exists).
#   PYTHON  the Python to run the scraper with (default: .venv/bin/python here, else python3)
#   REPORT  the Trackstar report script (default: ~/Development/trackstar/scripts/report-job-run.sh)

DIR="$(cd "$(dirname "$0")" && pwd)"
REPORT="${REPORT:-$HOME/Development/trackstar/scripts/report-job-run.sh}"
if [ -z "$PYTHON" ]; then
  if [ -x "$DIR/.venv/bin/python" ]; then PYTHON="$DIR/.venv/bin/python"; else PYTHON=python3; fi
fi
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_LOG="$(mktemp -t movie-scraper)"
export ADDED_ITEMS_FILE="$(mktemp -t movie-added)"   # scraper.py writes one JSON line per added screening

"$PYTHON" "$DIR/scraper.py" 2>&1 | tee "$RUN_LOG"
CODE=${PIPESTATUS[0]}

ADDED=$(grep -c "Added movie:" "$RUN_LOG")
DUPES=$(grep -c "Duplicate movie:" "$RUN_LOG")
ERRORS=$(grep -c "^ERROR:" "$RUN_LOG")
if [ "$CODE" -eq 0 ]; then
  STATUS=success
  SUMMARY="Added $ADDED screenings ($DUPES already on the calendar)"
else
  STATUS=failed
  SUMMARY="Scraper exited with code $CODE: $(grep -v '^\(INFO\|WARNING\):' "$RUN_LOG" | tail -1 | cut -c1-200)"
fi
if [ "$ERRORS" -eq 1 ]; then SUMMARY="$SUMMARY; 1 event skipped with an error"
elif [ "$ERRORS" -gt 1 ]; then SUMMARY="$SUMMARY; $ERRORS events skipped with errors"; fi

if [ -x "$REPORT" ]; then
  ITEMS_JSON="$(mktemp -t movie-items)"
  "$PYTHON" -c 'import json,sys; print(json.dumps([json.loads(l) for l in open(sys.argv[1]) if l.strip()]))' "$ADDED_ITEMS_FILE" > "$ITEMS_JSON"
  ITEMS_FILE="$ITEMS_JSON" "$REPORT" screen-boston-movie-calendar "$STATUS" "$STARTED" "$SUMMARY" \
    "{\"added\": $ADDED, \"duplicates\": $DUPES, \"errors\": $ERRORS}"
fi
rm -f "$RUN_LOG" "$ADDED_ITEMS_FILE" "${ITEMS_JSON:-}"
exit "$CODE"
