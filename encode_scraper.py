"""
Run this locally ONCE to generate run.py
Place this in: D:\WEB PROJECTS\Python\DAILY SALES\
Run: python encode_scraper.py
"""
import base64
import zlib
import os

script_dir  = os.path.dirname(os.path.abspath(__file__))
source_file = os.path.join(script_dir, "BMS_REVENUEP_VENUE.py")
venues_file = os.path.join(script_dir, "Venues.json")
output_file = os.path.join(script_dir, "../DAILY SALES/run.py")

if not os.path.exists(source_file):
    print(f"❌ BMS_REVENUEP_VENUE.py not found in {script_dir}")
    exit(1)

if not os.path.exists(venues_file):
    print(f"❌ Venues.json not found in {script_dir}")
    exit(1)

with open(source_file, "r", encoding="utf-8") as f:
    source_code = f.read()

with open(venues_file, "r", encoding="utf-8") as f:
    venues_data = f.read()

encoded_code   = base64.b64encode(zlib.compress(source_code.encode("utf-8"), 9)).decode("utf-8")
encoded_venues = base64.b64encode(zlib.compress(venues_data.encode("utf-8"), 9)).decode("utf-8")

launcher = f"""import base64,zlib,os
os.makedirs("data",exist_ok=True)
with open("Venues.json","w",encoding="utf-8") as _f:
    _f.write(zlib.decompress(base64.b64decode("{encoded_venues}")).decode("utf-8"))
exec(zlib.decompress(base64.b64decode("{encoded_code}")).decode("utf-8"))
"""

with open(output_file, "w", encoding="utf-8") as f:
    f.write(launcher)

size_kb = os.path.getsize(output_file) / 1024
print(f"✅ run.py generated ({size_kb:.1f} KB)")
print(f"   → BMS_REVENUEP_VENUE.py encoded ✅")
print(f"   → Venues.json encoded ✅")
print(f"\n📦 Push ONLY these to GitHub:")
print(f"   ✅ run.py")
print(f"   ✅ .github/workflows/hourly_scraper.yml")
print(f"\n❌ Never push:")
print(f"   ❌ BMS_REVENUEP_VENUE.py")
print(f"   ❌ Venues.json")
print(f"   ❌ encode_scraper.py")
print(f"   ❌ Check.py")