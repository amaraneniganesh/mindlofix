import json
import os
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

API_URL = "https://globetrotter2.agentokkadu.workers.dev/"

# --- MAXIMUM SPEED CONFIGURATION ---
WORKER_THREADS = 2  # Low threads per machine, but 20 machines running at once!
INPUT_FILE = "./Venues.json"

file_lock = threading.Lock()


def get_target_dates():
    return [datetime.now().strftime('%Y%m%d')]


def load_venues(filepath=INPUT_FILE):
    if not os.path.exists(filepath):
        print(f"Error: '{filepath}' not found.")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return list(data.values()) if isinstance(data, dict) else data


def process_single_venue(venue_item, dates):
    venue_code = venue_item.get("VenueCode", str(venue_item).strip()) if isinstance(venue_item, dict) else str(
        venue_item).strip()
    venue_name = venue_item.get("VenueName", venue_code) if isinstance(venue_item, dict) else venue_code
    city = venue_item.get("City", "Unknown City").strip() if isinstance(venue_item, dict) else "Unknown City"
    state = venue_item.get("State", "Unknown State").strip() if isinstance(venue_item, dict) else "Unknown State"

    if not venue_code: return None

    session = requests.Session(impersonate="chrome120")
    shard_data = {"state": state, "city": city, "venue_code": venue_code, "venue_name": venue_name, "movies": {},
                  "failed_dates": []}

    for date_code in dates:
        data = None
        for attempt in range(4):  # 4 retries max
            try:
                # ZERO DELAY HERE. Full speed ahead.
                response = session.get(API_URL, params={"venueCode": venue_code, "dateCode": date_code}, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    break
                elif response.status_code == 429:
                    time.sleep(2)  # Only pause briefly if we actually hit the wall
            except Exception:
                pass

        if not data:
            shard_data["failed_dates"].append(date_code)
            continue

        for detail in data.get("ShowDetails", []):
            for event in detail.get("Event", []):
                for child in event.get("ChildEvents", []):
                    title = child.get("EventName", event.get("EventTitle", "Unknown Title"))

                    if title not in shard_data["movies"]:
                        img = child.get("EventImageCode")
                        shard_data["movies"][title] = {
                            "Poster": f"https://assets-in.bmscdn.com/iedb/movies/images/mobile/thumbnail/xlarge/{img}.jpg" if img else None,
                            "TotalRevenue": 0.0, "TotalSeats": 0, "TotalBookedSeats": 0, "TotalShows": 0,
                            "SoldOutShows": 0, "FastFillingShows": 0
                        }

                    for show in child.get("ShowTimes", []):
                        s_max, s_booked, s_rev = 0, 0, 0.0
                        for cat in show.get("Categories", []):
                            m_seats, a_seats, price = int(cat.get("MaxSeats", 0)), int(cat.get("SeatsAvail", 0)), float(
                                cat.get("CurPrice", 0.0))
                            b_seats = max(0, m_seats - a_seats)
                            s_max += m_seats
                            s_booked += b_seats
                            s_rev += (b_seats * price)

                        if s_max > 0:
                            m_node = shard_data["movies"][title]
                            m_node["TotalShows"] += 1
                            m_node["TotalSeats"] += s_max
                            m_node["TotalBookedSeats"] += s_booked
                            m_node["TotalRevenue"] += s_rev

                            occ = s_booked / s_max
                            if occ == 1.0:
                                m_node["SoldOutShows"] += 1
                            elif occ >= 0.75:
                                m_node["FastFillingShows"] += 1

    session.close()
    return shard_data


def fetch_and_aggregate():
    dates = get_target_dates()
    venues = load_venues()
    if not venues: return

    total_shards = int(os.environ.get("TOTAL_SHARDS", 1))
    shard_index = int(os.environ.get("SHARD_INDEX", 0))

    chunk_size = len(venues) // total_shards
    start_idx = shard_index * chunk_size
    end_idx = len(venues) if shard_index == total_shards - 1 else start_idx + chunk_size
    my_venues = venues[start_idx:end_idx]

    output_file = f"{dates[0]}_data_shard_{shard_index}.json"
    overall_data = {}

    print(f"🚀 SHARD {shard_index} | Processing {len(my_venues)} venues...")

    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
        future_to_venue = {executor.submit(process_single_venue, v, dates): v for v in my_venues}
        for future in as_completed(future_to_venue):
            shard_result = future.result()
            if shard_result:
                state, city, v_code, v_name = shard_result["state"], shard_result["city"], shard_result["venue_code"], \
                shard_result["venue_name"]
                with file_lock:
                    for title, metrics in shard_result["movies"].items():
                        if title not in overall_data:
                            overall_data[title] = {
                                "Poster": metrics["Poster"], "Overall_TotalRevenue": 0.0, "Overall_TotalSeats": 0,
                                "Overall_TotalBookedSeats": 0,
                                "Overall_TotalShows": 0, "Overall_SoldOutShows": 0, "Overall_FastFillingShows": 0,
                                "Locations": {}
                            }

                        # Add stats (Omitted occupancy calculation here to save time, merger will do it)
                        m_node = overall_data[title]
                        m_node["Overall_TotalRevenue"] += metrics["TotalRevenue"]
                        m_node["Overall_TotalSeats"] += metrics["TotalSeats"]
                        m_node["Overall_TotalBookedSeats"] += metrics["TotalBookedSeats"]

                        locs = m_node["Locations"]
                        if state not in locs: locs[state] = {"State_TotalRevenue": 0.0, "State_TotalSeats": 0,
                                                             "State_TotalBookedSeats": 0, "Cities": {}}
                        s_node = locs[state]
                        s_node["State_TotalRevenue"] += metrics["TotalRevenue"]
                        s_node["State_TotalSeats"] += metrics["TotalSeats"]
                        s_node["State_TotalBookedSeats"] += metrics["TotalBookedSeats"]

                        cities = s_node["Cities"]
                        if city not in cities: cities[city] = {"City_TotalRevenue": 0.0, "City_TotalSeats": 0,
                                                               "City_TotalBookedSeats": 0, "Venues": {}}
                        c_node = cities[city]
                        c_node["City_TotalRevenue"] += metrics["TotalRevenue"]
                        c_node["City_TotalSeats"] += metrics["TotalSeats"]
                        c_node["City_TotalBookedSeats"] += metrics["TotalBookedSeats"]

                        venues_node = c_node["Venues"]
                        vk = f"{v_code} - {v_name}"
                        if vk not in venues_node: venues_node[vk] = {"Venue_TotalRevenue": 0.0, "Venue_TotalSeats": 0,
                                                                     "Venue_TotalBookedSeats": 0}
                        v_node = venues_node[vk]
                        v_node["Venue_TotalRevenue"] += metrics["TotalRevenue"]
                        v_node["Venue_TotalSeats"] += metrics["TotalSeats"]
                        v_node["Venue_TotalBookedSeats"] += metrics["TotalBookedSeats"]

                    with open(output_file, "w", encoding='utf-8') as outfile:
                        json.dump(overall_data, outfile, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    fetch_and_aggregate()