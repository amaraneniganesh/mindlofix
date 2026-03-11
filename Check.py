import json

with open("overall.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Loop through the movies to find Dhurandhar
for movie_title, metrics in data.items():
    if "Dhurandhar The Revenge - Hindi" in movie_title:
        print(f"\nFound Movie: {movie_title}")
        # This will print the exact State, City, and Venue Code!
        print(json.dumps(metrics["Locations"], indent=4))