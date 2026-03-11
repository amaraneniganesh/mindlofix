import json
import os
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

API_URL = "https://districtagent.agentokkadu.workers.dev/"

# --- SHARDING CONFIGURATION ---
WORKER_THREADS = 25
INPUT_FILE = "./Venues.json"

# Thread lock to prevent file corruption when auto-saving
file_lock = threading.Lock()


def get_target_dates():
    """Automatically fetch only the current day's date code."""
    today = datetime.now()
    return [today.strftime('%Y%m%d')]


def load_venues(filepath=INPUT_FILE):
    """Loads venues and intelligently handles dictionary-based JSON structures."""
    if not os.path.exists(filepath):
        print(f"Error: '{filepath}' not found.")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                return list(data.values())
            return data
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON.")
            return []


def generate_poster_url(image_code):
    if not image_code:
        return None
    return f"https://assets-in.bmscdn.com/iedb/movies/images/mobile/thumbnail/xlarge/{image_code}.jpg"


def process_single_venue(venue_item, dates):
    # Safely extract City and State from your Venues.json structure
    if isinstance(venue_item, dict):
        venue_code = venue_item.get("VenueCode")
        venue_name = venue_item.get("VenueName", venue_code)
        city = venue_item.get("City", "Unknown City").strip()
        state = venue_item.get("State", "Unknown State").strip()
    else:
        venue_code = str(venue_item).strip()
        venue_name = venue_code
        city = "Unknown City"
        state = "Unknown State"

    if not venue_code:
        return None

    session = requests.Session(impersonate="chrome110")
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
                    wait_time = (1.5 ** attempt) + random.uniform(0.5, 1.5)
                    time.sleep(wait_time)
                    continue

                elif response.status_code in [403, 404, 500, 502, 503]:
                    time.sleep(2)
                    continue

            except Exception:
                time.sleep(2)

        if not data:
            shard_data["failed_dates"].append(date_code)
            continue

        show_details = data.get("ShowDetails", [])
        if not show_details:
            continue

        for detail in show_details:
            events = detail.get("Event", [])
            for event in events:
                child_events = event.get("ChildEvents", [])
                for child in child_events:
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
                            max_seats = int(category.get("MaxSeats", 0))
                            avail_seats = int(category.get("SeatsAvail", 0))
                            price = float(category.get("CurPrice", 0.0))

                            booked_seats = max(0, max_seats - avail_seats)

                            show_max_seats += max_seats
                            show_booked_seats += booked_seats
                            show_revenue += (booked_seats * price)

                        if show_max_seats > 0:
                            shard_data["movies"][title]["TotalShows"] += 1
                            shard_data["movies"][title]["TotalSeats"] += show_max_seats
                            shard_data["movies"][title]["TotalBookedSeats"] += show_booked_seats
                            shard_data["movies"][title]["TotalRevenue"] += show_revenue

                            show_occupancy = show_booked_seats / show_max_seats

                            if show_occupancy == 1.0:
                                shard_data["movies"][title]["SoldOutShows"] += 1
                            elif show_occupancy >= 0.75:
                                shard_data["movies"][title]["FastFillingShows"] += 1

        time.sleep(random.uniform(0.2, 0.5))

    session.close()
    return shard_data


def fetch_and_aggregate():
    dates = get_target_dates()
    output_file = f"{dates[0]}_data.json"  # e.g., 20260311_data.json
    venues = load_venues()

    if not venues:
        print("No venues to process. Skipping this hour.")
        return

    overall_data = {}
    total_venues = len(venues)
    completed_count = 0
    total_failures = 0
    start_time = time.time()

    print(f"\n🚀 Starting SHARDED extraction for {total_venues} venues using {WORKER_THREADS} parallel threads...")
    print(f"📂 Output will be saved to: {output_file}")

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        future_to_venue = {executor.submit(process_single_venue, venue_item, dates): venue_item for venue_item in
                           venues}

        for future in as_completed(future_to_venue):
            venue_item = future_to_venue[future]

            if isinstance(venue_item, dict):
                venue_name_log = venue_item.get("VenueName", venue_item.get("VenueCode", "Unknown"))
            else:
                venue_name_log = str(venue_item).strip()

            completed_count += 1

            try:
                shard_result = future.result()

                if shard_result:
                    state = shard_result["state"]
                    city = shard_result["city"]
                    v_code = shard_result["venue_code"]
                    v_name = shard_result["venue_name"]
                    venue_key = f"{v_code} - {v_name}"

                    with file_lock:
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

                            movie_node = overall_data[title]
                            movie_node["Overall_TotalRevenue"] += metrics["TotalRevenue"]
                            movie_node["Overall_TotalSeats"] += metrics["TotalSeats"]
                            movie_node["Overall_TotalBookedSeats"] += metrics["TotalBookedSeats"]
                            movie_node["Overall_TotalShows"] += metrics["TotalShows"]
                            movie_node["Overall_SoldOutShows"] += metrics["SoldOutShows"]
                            movie_node["Overall_FastFillingShows"] += metrics["FastFillingShows"]

                            if movie_node["Overall_TotalSeats"] > 0:
                                movie_node["Overall_OccupancyPercentage"] = round(
                                    (movie_node["Overall_TotalBookedSeats"] / movie_node["Overall_TotalSeats"]) * 100,
                                    2)

                            locs = movie_node["Locations"]
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

                            state_node = locs[state]
                            state_node["State_TotalRevenue"] += metrics["TotalRevenue"]
                            state_node["State_TotalSeats"] += metrics["TotalSeats"]
                            state_node["State_TotalBookedSeats"] += metrics["TotalBookedSeats"]
                            state_node["State_TotalShows"] += metrics["TotalShows"]
                            state_node["State_SoldOutShows"] += metrics["SoldOutShows"]
                            state_node["State_FastFillingShows"] += metrics["FastFillingShows"]

                            if state_node["State_TotalSeats"] > 0:
                                state_node["State_OccupancyPercentage"] = round(
                                    (state_node["State_TotalBookedSeats"] / state_node["State_TotalSeats"]) * 100, 2)

                            cities = state_node["Cities"]
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

                            city_node = cities[city]
                            city_node["City_TotalRevenue"] += metrics["TotalRevenue"]
                            city_node["City_TotalSeats"] += metrics["TotalSeats"]
                            city_node["City_TotalBookedSeats"] += metrics["TotalBookedSeats"]
                            city_node["City_TotalShows"] += metrics["TotalShows"]
                            city_node["City_SoldOutShows"] += metrics["SoldOutShows"]
                            city_node["City_FastFillingShows"] += metrics["FastFillingShows"]

                            if city_node["City_TotalSeats"] > 0:
                                city_node["City_OccupancyPercentage"] = round(
                                    (city_node["City_TotalBookedSeats"] / city_node["City_TotalSeats"]) * 100, 2)

                            venues_node = city_node["Venues"]
                            if venue_key not in venues_node:
                                venues_node[venue_key] = {
                                    "Venue_TotalRevenue": 0.0,
                                    "Venue_TotalSeats": 0,
                                    "Venue_TotalBookedSeats": 0,
                                    "Venue_TotalShows": 0,
                                    "Venue_SoldOutShows": 0,
                                    "Venue_FastFillingShows": 0,
                                    "Venue_OccupancyPercentage": 0.0
                                }

                            venue_node = venues_node[venue_key]
                            venue_node["Venue_TotalRevenue"] += metrics["TotalRevenue"]
                            venue_node["Venue_TotalSeats"] += metrics["TotalSeats"]
                            venue_node["Venue_TotalBookedSeats"] += metrics["TotalBookedSeats"]
                            venue_node["Venue_TotalShows"] += metrics["TotalShows"]
                            venue_node["Venue_SoldOutShows"] += metrics["SoldOutShows"]
                            venue_node["Venue_FastFillingShows"] += metrics["FastFillingShows"]

                            if venue_node["Venue_TotalSeats"] > 0:
                                venue_node["Venue_OccupancyPercentage"] = round(
                                    (venue_node["Venue_TotalBookedSeats"] / venue_node["Venue_TotalSeats"]) * 100, 2)

                        # Auto-save after completing all movies for this venue to the daily file
                        with open(output_file, "w", encoding='utf-8') as outfile:
                            json.dump(overall_data, outfile, indent=4, ensure_ascii=False)

                    failed_count = len(shard_result["failed_dates"])
                    total_failures += failed_count
                    status = "✅" if failed_count == 0 else f"⚠️ ({failed_count} dates failed)"

            except Exception as e:
                status = f"❌ Crashed: {e}"
                total_failures += len(dates)

            elapsed_time = time.time() - start_time
            avg_time_per_venue = elapsed_time / completed_count
            remaining_venues = total_venues - completed_count
            eta_seconds = int(avg_time_per_venue * remaining_venues)
            eta_formatted = str(timedelta(seconds=eta_seconds))

            print(f"[{completed_count}/{total_venues}] {status} {venue_name_log} | ⏱️ ETA: {eta_formatted}")

    print("\n" + "=" * 40)
    print(f"🎯 SHARDED EXTRACTION COMPLETE IN {str(timedelta(seconds=int(time.time() - start_time)))}!")
    print("=" * 40)
    print(f"Total Venues Processed: {total_venues}")
    print(f"Total Failed Requests:  {total_failures}")
    print("=" * 40)


if __name__ == "__main__":
    print(f"\n--- Starting GitHub Actions extraction cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        fetch_and_aggregate()
        print("✅ Extraction complete. Exiting script so GitHub can commit the data.")
    except Exception as e:
        print(f"❌ Critical error during extraction: {e}")
        exit(1) # This safely tells GitHub the script failed if it crashes