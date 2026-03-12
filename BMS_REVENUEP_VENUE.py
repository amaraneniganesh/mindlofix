# scraper.py
import json
import os
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

API_URL = "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"

WORKER_THREADS = 8
INPUT_FILE = "Venues.json"
file_lock = threading.Lock()


def get_target_dates():
    today = datetime.now()
    return [today.strftime('%Y%m%d')]


def load_venues(filepath=INPUT_FILE):
    if not os.path.exists(filepath):
        print(f"Error: '{filepath}' not found.")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            return list(data.values()) if isinstance(data, dict) else data
        except json.JSONDecodeError:
            print("Error: Invalid JSON.")
            return []


def generate_poster_url(image_code):
    if not image_code:
        return None
    return f"https://assets-in.bmscdn.com/iedb/movies/images/mobile/thumbnail/xlarge/{image_code}.jpg"


def process_single_venue(venue_item, dates):
    if isinstance(venue_item, dict):
        venue_code = venue_item.get("VenueCode")
        venue_name = venue_item.get("VenueName", venue_code)
        city  = venue_item.get("City", "Unknown City").strip()
        state = venue_item.get("State", "Unknown State").strip()
    else:
        venue_code = str(venue_item).strip()
        venue_name = venue_code
        city  = "Unknown City"
        state = "Unknown State"

    if not venue_code:
        return None

    session = requests.Session(impersonate="chrome120")
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        "Referer": "https://in.bookmyshow.com/",
        "Origin": "https://in.bookmyshow.com",
        "appVersion": "14.5.1",
        "os": "Android",
        "osVersion": "13",
        "X-Requested-With": "XMLHttpRequest",
    })

    MAX_RETRIES = 6
    shard_data = {
        "state": state,
        "city": city,
        "venue_code": venue_code,
        "venue_name": venue_name,
        "movies": {},
        "failed_dates": []
    }

    for date_code in dates:
        # Throttle before each request to avoid burst 429s
        time.sleep(random.uniform(1.0, 2.0))

        data = None
        for attempt in range(MAX_RETRIES):
            try:
                response = session.get(
                    API_URL,
                    params={"venueCode": venue_code, "dateCode": date_code},
                    timeout=15
                )

                if response.status_code == 200:
                    data = response.json()
                    break

                elif response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(2, 4)
                    print(f"  ⏳ 429 on {venue_code}, waiting {wait_time:.1f}s (attempt {attempt+1})")
                    time.sleep(wait_time)
                    continue

                elif response.status_code in [403, 404, 500, 502, 503]:
                    time.sleep(3)
                    continue

            except Exception as e:
                time.sleep(2)

        if not data:
            shard_data["failed_dates"].append(date_code)
            continue

        show_details = data.get("ShowDetails", [])
        if not show_details:
            continue

        for detail in show_details:
            for event in detail.get("Event", []):
                for child in event.get("ChildEvents", []):
                    title = child.get("EventName", event.get("EventTitle", "Unknown Title"))

                    if title not in shard_data["movies"]:
                        shard_data["movies"][title] = {
                            "Poster": generate_poster_url(child.get("EventImageCode")),
                            "TotalRevenue": 0.0,
                            "TotalSeats": 0,
                            "TotalBookedSeats": 0,
                            "TotalShows": 0,
                            "SoldOutShows": 0,
                            "FastFillingShows": 0
                        }

                    for show in child.get("ShowTimes", []):
                        show_max_seats = 0
                        show_booked_seats = 0
                        show_revenue = 0.0

                        for category in show.get("Categories", []):
                            max_seats    = int(category.get("MaxSeats", 0))
                            avail_seats  = int(category.get("SeatsAvail", 0))
                            price        = float(category.get("CurPrice", 0.0))
                            booked_seats = max(0, max_seats - avail_seats)

                            show_max_seats    += max_seats
                            show_booked_seats += booked_seats
                            show_revenue      += booked_seats * price

                        if show_max_seats > 0:
                            m = shard_data["movies"][title]
                            m["TotalShows"]       += 1
                            m["TotalSeats"]       += show_max_seats
                            m["TotalBookedSeats"] += show_booked_seats
                            m["TotalRevenue"]     += show_revenue

                            occ = show_booked_seats / show_max_seats
                            if occ == 1.0:
                                m["SoldOutShows"] += 1
                            elif occ >= 0.75:
                                m["FastFillingShows"] += 1

    session.close()
    return shard_data


def aggregate(overall_data, shard_result, output_file):
    state     = shard_result["state"]
    city      = shard_result["city"]
    venue_key = f"{shard_result['venue_code']} - {shard_result['venue_name']}"

    def add_metrics(node, prefix, m):
        node[f"{prefix}TotalRevenue"]     += m["TotalRevenue"]
        node[f"{prefix}TotalSeats"]       += m["TotalSeats"]
        node[f"{prefix}TotalBookedSeats"] += m["TotalBookedSeats"]
        node[f"{prefix}TotalShows"]       += m["TotalShows"]
        node[f"{prefix}SoldOutShows"]     += m["SoldOutShows"]
        node[f"{prefix}FastFillingShows"] += m["FastFillingShows"]
        if node[f"{prefix}TotalSeats"] > 0:
            node[f"{prefix}OccupancyPercentage"] = round(
                node[f"{prefix}TotalBookedSeats"] / node[f"{prefix}TotalSeats"] * 100, 2)

    for title, metrics in shard_result["movies"].items():
        if title not in overall_data:
            overall_data[title] = {
                "Poster": metrics["Poster"],
                "Overall_TotalRevenue": 0.0,
                "Overall_TotalSeats": 0,
                "Overall_TotalBookedSeats": 0,
                "Overall_TotalShows": 0,
                "Overall_SoldOutShows": 0,
                "Overall_FastFillingShows": 0,
                "Overall_OccupancyPercentage": 0.0,
                "Locations": {}
            }

        mn = overall_data[title]
        add_metrics(mn, "Overall_", metrics)
        locs = mn["Locations"]

        if state not in locs:
            locs[state] = {
                "State_TotalRevenue": 0.0,
                "State_TotalSeats": 0,
                "State_TotalBookedSeats": 0,
                "State_TotalShows": 0,
                "State_SoldOutShows": 0,
                "State_FastFillingShows": 0,
                "State_OccupancyPercentage": 0.0,
                "Cities": {}
            }
        add_metrics(locs[state], "State_", metrics)

        cities = locs[state]["Cities"]
        if city not in cities:
            cities[city] = {
                "City_TotalRevenue": 0.0,
                "City_TotalSeats": 0,
                "City_TotalBookedSeats": 0,
                "City_TotalShows": 0,
                "City_SoldOutShows": 0,
                "City_FastFillingShows": 0,
                "City_OccupancyPercentage": 0.0,
                "Venues": {}
            }
        add_metrics(cities[city], "City_", metrics)

        vn = cities[city]["Venues"]
        if venue_key not in vn:
            vn[venue_key] = {
                "Venue_TotalRevenue": 0.0,
                "Venue_TotalSeats": 0,
                "Venue_TotalBookedSeats": 0,
                "Venue_TotalShows": 0,
                "Venue_SoldOutShows": 0,
                "Venue_FastFillingShows": 0,
                "Venue_OccupancyPercentage": 0.0
            }
        add_metrics(vn[venue_key], "Venue_", metrics)

    # Auto-save after every venue
    with open(output_file, "w", encoding='utf-8') as f:
        json.dump(overall_data, f, indent=4, ensure_ascii=False)


def fetch_and_aggregate():
    dates       = get_target_dates()
    output_file = f"{dates[0]}_data.json"
    venues      = load_venues()

    if not venues:
        print("No venues to process.")
        return

    overall_data   = {}
    total_venues   = len(venues)
    completed      = 0
    total_failures = 0
    start_time     = time.time()

    print(f"\n🚀 Starting extraction for {total_venues} venues | {WORKER_THREADS} threads")
    print(f"📂 Output: {output_file}")
    print("=" * 60)

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        future_to_venue = {
            executor.submit(process_single_venue, v, dates): v
            for v in venues
        }

        for future in as_completed(future_to_venue):
            venue_item = future_to_venue[future]
            venue_name_log = (
                venue_item.get("VenueName", venue_item.get("VenueCode", "Unknown"))
                if isinstance(venue_item, dict) else str(venue_item).strip()
            )
            completed += 1

            try:
                shard_result = future.result()

                if shard_result:
                    failed_count = len(shard_result["failed_dates"])
                    total_failures += failed_count
                    status = "✅" if failed_count == 0 else f"⚠️ ({failed_count} failed)"

                    with file_lock:
                        aggregate(overall_data, shard_result, output_file)
                else:
                    status = "⏭️ skipped"

            except Exception as e:
                status = f"❌ {e}"
                total_failures += len(dates)

            elapsed  = time.time() - start_time
            avg      = elapsed / completed
            eta      = str(timedelta(seconds=int(avg * (total_venues - completed))))
            movies   = len(shard_result["movies"]) if shard_result else 0
            print(f"[{completed}/{total_venues}] {status} {venue_name_log} | 🎬 {movies} movies | ⏱️ ETA: {eta}")

    print("\n" + "=" * 60)
    print(f"🎯 DONE IN {str(timedelta(seconds=int(time.time() - start_time)))}")
    print(f"Total Venues : {total_venues}")
    print(f"Total Failures: {total_failures}")
    print("=" * 60)


if __name__ == "__main__":
    print(f"\n--- Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        fetch_and_aggregate()
        print("✅ Extraction complete.")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        exit(1)