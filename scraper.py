import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
import logging
import argparse
import time
from googleapiclient.errors import HttpError

# Setup logging
logging.basicConfig(level=logging.INFO)

# Constants
URL = 'https://www.screenboston.com'
TIMEZONE = 'America/New_York'
SCOPES = ['https://www.googleapis.com/auth/calendar']
CALENDAR_ID = 'e5c1da30fb4f7107d2d82740aa70cda38574d03f7c0d2a64bc5c8fd55e6d0465@group.calendar.google.com'
DATE_FORMAT = "%A, %B %d %Y %I:%M %p"

# Argument parser setup
def parse_arguments():
    parser = argparse.ArgumentParser(description='Movie Calendar Scraper')
    parser.add_argument('-reset', action='store_true', help='Reset by deleting all future calendar entries and then adding new events.')
    parser.add_argument('-see-existing', action='store_true', help='Log all existing calendar entries.')
    parser.add_argument('-delete-all', action='store_true', help='Delete all events in the calendar.')
    parser.add_argument('-delete-future', action='store_true', help='Delete future events in the calendar.')
    return parser.parse_args()

# Scraping function
def scrap_movies() -> list:
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, 'html.parser')
    date_containers = soup.find_all('div', class_='max-w-screen')
    movie_data = []

    for date_container in date_containers:
        date_id = date_container.get('id', '')
        date_element = date_container.find('p', class_='small')
        date_text = date_element.get_text(strip=True) if date_element else 'Unknown Date'
        year = date_id.split('-')[0]
        complete_date_str = f"{date_text} {year}"

        if date_text == 'Unknown Date':
            continue

        movies = date_container.find_all('button', class_='w-full h-auto max-w-full text-left')

        for movie in movies:
            try:
                title = movie.find('p', class_='big').get_text(strip=True)
                format_tag = movie.find('p', class_='text-[14px] text-primary')
                movie_format = format_tag.get_text(strip=True) if format_tag else None

                details = movie.find_all('p', class_='')
                director = details[0].get_text(strip=True) if details else 'Unknown Director'
                year_genre_runtime = details[1].get_text(strip=True) if len(details) > 1 else 'Unknown Details'
                year_genre_runtime_array = year_genre_runtime.split(',')

                theater = details[2].get_text(strip=True) if len(details) > 2 else 'Unknown Theater'
                showtime_elements = movie.find_all('p', string=lambda text: 'PM' in text or 'AM' in text)
                showtimes = [time.get_text(strip=True) for time in showtime_elements]

                movie_data.append({
                    'date': complete_date_str,
                    'title': title,
                    'format': movie_format,
                    'director': director,
                    'year': year_genre_runtime_array[0],
                    'genre': year_genre_runtime_array[1],
                    'runtime': year_genre_runtime_array[2] if len(year_genre_runtime_array) > 2 else '0h 0m',
                    'theater': theater,
                    'showtimes': showtimes if showtimes else ['Unknown Showtimes']
                })

            except (AttributeError, IndexError) as e:
                logging.error(f"Error processing movie: {e}")

    return movie_data

# Google Calendar Service creation
def create_google_calendar_service():
    creds = service_account.Credentials.from_service_account_file(
        os.path.join(os.getcwd(), 'credentials.json'), scopes=SCOPES
    )
    return build('calendar', 'v3', credentials=creds)

# Event Deletion Functions
def delete_events(service, time_min=None):
    page_token = None
    try:
        while True:
            events_result = service.events().list(
                calendarId=CALENDAR_ID,
                timeMin=time_min,
                singleEvents=True,
                orderBy='startTime',
                pageToken=page_token
            ).execute()

            events = events_result.get('items', [])
            if not events:
                logging.info('No events found to delete.')
                break

            for event in events:
                retry_count = 0
                while retry_count < 3:
                    try:
                        service.events().delete(calendarId=CALENDAR_ID, eventId=event['id']).execute()
                        logging.info(f"Deleted event: {event['summary']}")
                        break
                    except HttpError as error:
                        retry_count += 1
                        logging.warning(f"Retry {retry_count}: Failed to delete event: {error}")
                        time.sleep(2 ** retry_count)

            page_token = events_result.get('nextPageToken')
            if not page_token:
                break

    except HttpError as error:
        logging.error(f"An error occurred: {error}")

def delete_all_events(service):
    delete_events(service)

def delete_future_events(service):
    now = datetime.utcnow().isoformat() + 'Z'
    delete_events(service, time_min=now)

def fetch_all_existing_events(service):
    existing_events_dict = {}
    page_token = None

    while True:
        events_result = service.events().list(
            calendarId=CALENDAR_ID,
            singleEvents=True,
            orderBy='startTime',
            pageToken=page_token
        ).execute()

        events = events_result.get('items', [])

        if not events:
            break

        for event in events:
            event_title = event.get('summary', '').strip()
            event_start = event['start'].get('dateTime', '').strip()

            if event_title and event_start:
                # Convert event start time to UTC for consistency
                utc_event_start = datetime.strptime(event_start, '%Y-%m-%dT%H:%M:%S%z').astimezone(pytz.utc)
                utc_event_start_str = utc_event_start.strftime('%Y-%m-%dT%H:%M:%SZ')

                # Update the event key in the dictionary to match the format used for the new events
                event_key = (event_title, utc_event_start_str)
                existing_events_dict[event_key] = event['id']

        page_token = events_result.get('nextPageToken')
        if not page_token:
            break

    return existing_events_dict


# Validating showtime format
def validate_showtime(showtime: str) -> bool:
    try:
        datetime.strptime(showtime, "%I:%M %p")
        return True
    except ValueError:
        return False

# Update calendar with movies
def update_google_calendar(service, movies: list):
    existing_events_dict = fetch_all_existing_events(service)
    new_event_ids = set()

    for movie in movies:
        date_str = movie['date']
        for showtime in movie['showtimes']:
            if not validate_showtime(showtime):
                logging.warning(f"Invalid showtime '{showtime}', skipping...")
                continue

            event_datetime_str = f"{date_str} {showtime}"
            try:
                event_datetime = datetime.strptime(event_datetime_str, DATE_FORMAT)
                hours, minutes = map(int, (movie['runtime'].replace('h', '').replace('m', '').strip().split()))
                duration = timedelta(hours=hours, minutes=minutes)
                end_datetime = event_datetime + duration

                local_tz = pytz.timezone(TIMEZONE)
                utc_start_datetime = local_tz.localize(event_datetime).astimezone(pytz.utc)
                utc_end_datetime = local_tz.localize(end_datetime).astimezone(pytz.utc)

                event_key = (movie['title'].strip(), utc_start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ'))

                if event_key in existing_events_dict:
                    new_event_ids.add(existing_events_dict[event_key])
                    logging.info(f"Duplicate movie: {movie['title']} at {showtime} on {date_str}.")
                else:
                    event = {
                        'summary': movie['title'],
                        'location': movie['theater'],
                        'description': (
                            f"{movie['format']}\nDirector: {movie['director']}\n"
                            f"{movie['year']}, {movie['genre']}, {movie['runtime']}\nTheater: {movie['theater']}"
                        ),
                        'start': {
                            'dateTime': utc_start_datetime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'timeZone': TIMEZONE,
                        },
                        'end': {
                            'dateTime': utc_end_datetime.strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'timeZone': TIMEZONE,
                        },
                    }
                    added_event = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
                    new_event_ids.add(added_event['id'])
                    logging.info(f"Added movie: {movie['title']} at {showtime} on {date_str}.")

            except ValueError as e:
                logging.error(f"Skipping event due to error: {e}")

# Main function
def run():
    parser = argparse.ArgumentParser(description="Movie Scraper and Calendar Updater")
    parser.add_argument("-reset", action="store_true", help="Reset by deleting all future events and adding new ones.")
    parser.add_argument("-see-existing", action="store_true", help="Log all existing calendar events.")
    parser.add_argument("-delete-all", action="store_true", help="Delete all events in the calendar.")
    parser.add_argument("-delete-future", action="store_true", help="Delete future events in the calendar.")
    args = parser.parse_args()

    service = create_google_calendar_service()

    if args.see_existing:
        existing_events = fetch_all_existing_events(service)
        logging.info("Existing events fetched:")
        for event_key in existing_events.keys():
            logging.info(event_key)  # Log only if the flag is set
        return
    
    if args.delete_future:
        delete_future_events(service)
        return
    
    if args.delete_all:
        delete_all_events(service)
        return  # We return here because we only want to delete events and stop further actions

    if args.reset:
        delete_future_events(service)

    movies = scrap_movies()
    logging.info("Fetched movie data:")
    for movie in movies:
        logging.info(movie)

    logging.info("Updating Google Calendar")
    update_google_calendar(service, movies)
    print("Done")

if __name__ == "__main__":
    run()
