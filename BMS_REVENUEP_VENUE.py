# BMS_REVENUEP_VENUE.py
import json
import os
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

API_URL = "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
WORKER_THREADS = 15
INPUT_FILE = "Venues.json"
file_lock = threading.Lock()
thread_local = threading.local()


def get_target_dates():
    return [datetime.now().strftime('%Y%m%d')]


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


def get_thread_context():
    """Each thread gets its own browser context — isolated, no sharing."""
    if not hasattr(thread_local, "context"):
        thread_local.playwright = sync_playwright().start()
        thread_local.browser = thread_local.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=390,844",
            ]
        )
        thread_local.context = thread_local.browser.new_context(
            user_agent="Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            viewport={"width": 390, "height": 844},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            extra_http_headers={
                "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
                "appVersion": "14.5.1",
                "os": "Android",
                "osVersion": "13",
            }
        )
        thread_local.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-IN', 'en'] });
            window.chrome = { runtime: {} };
        """)

        # Visit BMS once per thread to establish session/cookies
        page = thread_local.context.new_page()
        try:
            page.goto("https://in.bookmyshow.com/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)
        except:
            pass
        finally:
            page.close()

        print(f"  🌐 Thread browser ready (Thread ID: {threading.get_ident()})")

    return thread_local.context


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

    shard_data = {
        "state": state, "city": city,
        "venue_code": venue_code, "venue_name": venue_name,
        "movies": {}, "failed_dates": []
    }

    MAX_RETRIES = 5

    for date_code in dates:
        data = None

        for attempt in range(MAX_RETRIES):
            page = None
            try:
                context = get_thread_context()
                page = context.new_page()
                captured = []

                def handle_response(response):
                    if "showtimes/byvenue" in response.url:
                        try:
                            captured.append(response.json())
                        except:
                            pass

                page.on("response", handle_response)
                page.goto(
                    f"{API_URL}?venueCode={venue_code}&dateCode={date_code}",
                    wait_until="domcontentloaded",
                    timeout=20000
                )

                if captured:
                    data = captured[0]
                    break
                else:
                    try:
                        content = page.inner_text("body")
                        if content and "{" in content:
                            data = json.loads(content)
                            if data.get("status") == 429:
                                wait_time = (2 ** attempt) + random.uniform(3, 6)
                                print(f"  ⏳ 429 on {venue_code}, waiting {wait_time:.1f}s")
                                page.close()
                                page = None
                                time.sleep(wait_time)
                                continue
                            if "ShowDetails" in data:
                                break
                    except:
                        pass

            except Exception:
                time.sleep(2)
            finally:
                if page and not page.is_closed():
                    try:
                        page.close()
                    except:
                        pass

            time.sleep(random.uniform(0.2, 0.5))

        if not data or "ShowDetails" not in data:
            shard_data["failed_dates"].append(date_code)
            continue

        for detail in data.get("ShowDetails", []):
            for event in detail.get("Event", []):
                for child in event.get("ChildEvents", []):
                    title = child.get("EventName", event.get("EventTitle", "Unknown Title"))

                    if title not in shard_data["movies"]:
                        shard_data["movies"][title] = {
                            "Poster": generate_poster_url(child.get("EventImageCode")),
                            "TotalRevenue": 0.0, "TotalSeats": 0,
                            "TotalBookedSeats": 0, "TotalShows": 0,
                            "SoldOutShows": 0, "FastFillingShows": 0
                        }

                    for show in child.get("ShowTimes", []):
                        mx, bk, rev = 0, 0, 0.0
                        for cat in show.get("Categories", []):
                            ms = int(cat.get("MaxSeats", 0))
                            av = int(cat.get("SeatsAvail", 0))
                            pr = float(cat.get("CurPrice", 0.0))
                            booked = max(0, ms - av)
                            mx += ms; bk += booked; rev += booked * pr

                        if mx > 0:
                            m = shard_data["movies"][title]
                            m["TotalShows"] += 1
                            m["TotalSeats"] += mx
                            m["TotalBookedSeats"] += bk
                            m["TotalRevenue"] += rev
                            occ = bk / mx
                            if occ == 1.0: m["SoldOutShows"] += 1
                            elif occ >= 0.75: m["FastFillingShows"] += 1

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
                "Overall_TotalRevenue": 0.0, "Overall_TotalSeats": 0,
                "Overall_TotalBookedSeats": 0, "Overall_TotalShows": 0,
                "Overall_SoldOutShows": 0, "Overall_FastFillingShows": 0,
                "Overall_OccupancyPercentage": 0.0, "Locations": {}
            }
        mn = overall_data[title]
        add_metrics(mn, "Overall_", metrics)
        locs = mn["Locations"]

        if state not in locs:
            locs[state] = {
                "State_TotalRevenue": 0.0, "State_TotalSeats": 0,
                "State_TotalBookedSeats": 0, "State_TotalShows": 0,
                "State_SoldOutShows": 0, "State_FastFillingShows": 0,
                "State_OccupancyPercentage": 0.0, "Cities": {}
            }
        add_metrics(locs[state], "State_", metrics)

        cities = locs[state]["Cities"]
        if city not in cities:
            cities[city] = {
                "City_TotalRevenue": 0.0, "City_TotalSeats": 0,
                "City_TotalBookedSeats": 0, "City_TotalShows": 0,
                "City_SoldOutShows": 0, "City_FastFillingShows": 0,
                "City_OccupancyPercentage": 0.0, "Venues": {}
            }
        add_metrics(cities[city], "City_", metrics)

        vn = cities[city]["Venues"]
        if venue_key not in vn:
            vn[venue_key] = {
                "Venue_TotalRevenue": 0.0, "Venue_TotalSeats": 0,
                "Venue_TotalBookedSeats": 0, "Venue_TotalShows": 0,
                "Venue_SoldOutShows": 0, "Venue_FastFillingShows": 0,
                "Venue_OccupancyPercentage": 0.0
            }
        add_metrics(vn[venue_key], "Venue_", metrics)

    with open(output_file, "w", encoding='utf-8') as f:
        json.dump(overall_data, f, indent=4, ensure_ascii=False)


def run_batch(venues, dates, overall_data, output_file, start_time, label=""):
    """Run a batch of venues and return list of failed ones."""
    failed_venues = []
    completed = 0

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
                    status = "✅" if failed_count == 0 else f"⚠️ ({failed_count} failed)"

                    if failed_count > 0:
                        failed_venues.append(venue_item)

                    with file_lock:
                        aggregate(overall_data, shard_result, output_file)
                else:
                    status = "⏭️ skipped"
                    shard_result = {"movies": {}}

            except Exception as e:
                status = f"❌ {e}"
                failed_venues.append(venue_item)
                shard_result = {"movies": {}}

            elapsed = time.time() - start_time
            avg     = elapsed / max(completed, 1)
            eta     = str(timedelta(seconds=int(avg * (len(venues) - completed))))
            print(f"{label}[{completed}/{len(venues)}] {status} {venue_name_log} | 🎬 {len(shard_result['movies'])} movies | ⏱️ ETA: {eta}")

    return failed_venues


def fetch_and_aggregate():
    dates       = get_target_dates()
    os.makedirs("data", exist_ok=True)
    output_file = f"data/{dates[0]}_data.json"
    venues      = load_venues()

    if not venues:
        print("No venues to process.")
        return

    overall_data = {}
    total_venues = len(venues)
    start_time   = time.time()

    print(f"\n🚀 Starting: {total_venues} venues | {WORKER_THREADS} threads")
    print(f"📂 Output: {output_file}")
    print("=" * 60)

    # ── Pass 1: All venues ───────────────────────────────────
    failed_venues = run_batch(venues, dates, overall_data, output_file, start_time)

    # ── Pass 2, 3, 4: Retry failed venues ───────────────────
    MAX_RETRY_PASSES = 3
    for retry_num in range(1, MAX_RETRY_PASSES + 1):
        if not failed_venues:
            break

        print(f"\n{'='*60}")
        print(f"🔄 RETRY PASS {retry_num}: {len(failed_venues)} venues — retrying...")
        print(f"{'='*60}")
        time.sleep(5)

        failed_venues = run_batch(
            failed_venues, dates, overall_data,
            output_file, start_time,
            label=f"[RETRY {retry_num}] "
        )

    # ── Final summary ────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"🎯 DONE IN {str(timedelta(seconds=int(time.time() - start_time)))}")
    print(f"Total Venues:       {total_venues}")
    print(f"Remaining Failures: {len(failed_venues)}")
    if failed_venues:
        print("\n⚠️  Still failed after all retries:")
        for v in failed_venues:
            name = v.get("VenueName", v.get("VenueCode", "?")) if isinstance(v, dict) else str(v)
            print(f"   - {name}")
    else:
        print("✅ All venues fetched successfully!")
    print("=" * 60)


if __name__ == "__main__":
    print(f"\n--- Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        fetch_and_aggregate()
        print("✅ Extraction complete.")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        exit(1)