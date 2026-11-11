# Screen Boston Movie Calendar
This python web app scrapes the Screen Boston website daily, creates a calendar, and uploads it to a public Google Calendar. This should allow for others to import the boston movie calendar across several theaters into their own calendars. The script runs every morning at 6:30am from a macOS LaunchAgent on an always-on Mac (`~/Library/LaunchAgents/com.shanefuller.screen-boston-movie-calendar.plist`, which runs `run_scraper.sh` and appends to `logfile.log`), and updates the public calendar with any new changes. Each run is reported to the Trackstar admin Jobs page.

Setup on a new Mac: `python3 -m venv .venv && .venv/bin/pip install requests beautifulsoup4 pytz google-api-python-client google-auth`, put the service-account key in `credentials.json` next to the script, and `run_scraper.sh` finds `.venv` on its own.

The code on how I got it working is here. If you want to try this out yourself, you will need to use your own set of credentials. I included a sample file for my credentials so that you can get an idea of what it is supposed to look like.

Here is the link to add the Google Calendar to your view! Enjoy, and happy movie watching!

https://calendar.google.com/calendar/u/0?cid=ZTVjMWRhMzBmYjRmNzEwN2QyZDgyNzQwYWE3MGNkYTM4NTc0ZDAzZjdjMGQyYTY0YmM1YzhmZDU1ZTZkMDQ2NUBncm91cC5jYWxlbmRhci5nb29nbGUuY29t
