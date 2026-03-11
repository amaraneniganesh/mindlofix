import json
import glob
import os
from datetime import datetime


def calculate_occupancy(booked, total):
    return round((booked / total) * 100, 2) if total > 0 else 0.0


def merge_all_shards():
    today = datetime.now().strftime('%Y%m%d')
    shard_files = glob.glob(f"{today}_data_shard_*.json")

    if not shard_files:
        print("No shard files found to merge.")
        return

    master_data = {}

    for file in shard_files:
        with open(file, 'r', encoding='utf-8') as f:
            try:
                shard_data = json.load(f)
            except:
                continue

            for movie, m_data in shard_data.items():
                if movie not in master_data:
                    master_data[movie] = m_data
                    continue

                # Merge Movies
                master = master_data[movie]
                master["Overall_TotalRevenue"] += m_data.get("Overall_TotalRevenue", 0)
                master["Overall_TotalSeats"] += m_data.get("Overall_TotalSeats", 0)
                master["Overall_TotalBookedSeats"] += m_data.get("Overall_TotalBookedSeats", 0)

                # Merge States
                for state, s_data in m_data.get("Locations", {}).items():
                    if state not in master["Locations"]:
                        master["Locations"][state] = s_data
                        continue

                    m_state = master["Locations"][state]
                    m_state["State_TotalRevenue"] += s_data.get("State_TotalRevenue", 0)
                    m_state["State_TotalSeats"] += s_data.get("State_TotalSeats", 0)
                    m_state["State_TotalBookedSeats"] += s_data.get("State_TotalBookedSeats", 0)

                    # Merge Cities
                    for city, c_data in s_data.get("Cities", {}).items():
                        if city not in m_state["Cities"]:
                            m_state["Cities"][city] = c_data
                            continue

                        m_city = m_state["Cities"][city]
                        m_city["City_TotalRevenue"] += c_data.get("City_TotalRevenue", 0)
                        m_city["City_TotalSeats"] += c_data.get("City_TotalSeats", 0)
                        m_city["City_TotalBookedSeats"] += c_data.get("City_TotalBookedSeats", 0)

                        # Merge Venues
                        for venue, v_data in c_data.get("Venues", {}).items():
                            if venue not in m_city["Venues"]:
                                m_city["Venues"][venue] = v_data
                                continue

                            m_venue = m_city["Venues"][venue]
                            m_venue["Venue_TotalRevenue"] += v_data.get("Venue_TotalRevenue", 0)
                            m_venue["Venue_TotalSeats"] += v_data.get("Venue_TotalSeats", 0)
                            m_venue["Venue_TotalBookedSeats"] += v_data.get("Venue_TotalBookedSeats", 0)

    # Final Pass: Calculate Occupancies
    for movie, m_data in master_data.items():
        m_data["Overall_OccupancyPercentage"] = calculate_occupancy(m_data["Overall_TotalBookedSeats"],
                                                                    m_data["Overall_TotalSeats"])
        for state, s_data in m_data.get("Locations", {}).items():
            s_data["State_OccupancyPercentage"] = calculate_occupancy(s_data["State_TotalBookedSeats"],
                                                                      s_data["State_TotalSeats"])
            for city, c_data in s_data.get("Cities", {}).items():
                c_data["City_OccupancyPercentage"] = calculate_occupancy(c_data["City_TotalBookedSeats"],
                                                                         c_data["City_TotalSeats"])
                for venue, v_data in c_data.get("Venues", {}).items():
                    v_data["Venue_OccupancyPercentage"] = calculate_occupancy(v_data["Venue_TotalBookedSeats"],
                                                                              v_data["Venue_TotalSeats"])

    final_filename = f"{today}_data.json"
    with open(final_filename, 'w', encoding='utf-8') as f:
        json.dump(master_data, f, indent=4, ensure_ascii=False)

    print(f"✅ Successfully merged {len(shard_files)} shards into {final_filename}")


if __name__ == "__main__":
    merge_all_shards()