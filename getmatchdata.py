import json
import os.path
import requests
import time

leagues = [
    16169, # BetBoom Dacha Dubai 2024
    16201, # DreamLeague Season 22
    16483, # Elite League Season 1
    16518, # ESL One Birmingham 2024
    16669, # PGL Wallachia Season 1
    16632, # DreamLeague Season 23
    16881, # Riyadh Master 2024
    16901, # Clavision: Snow Ruyi
    16935, # TI 2024 
    17126, # BB Dacha Belgrade 2024
    17272, # DreamLeague Season 24
    17414, # BLAST Slam I
    17509, # ESL One Bangkok 2024
    17588, # FISSURE PLAYGROUND 1
    17417, # BLAST Slam II
    17765, # DreamLeague Season 25
    17891, # PGL Wallachia Season 3
    17907, # FISSURE Universe: Episode 4
    17795, # ESL One Raleigh 25
    18058  # PGL Wallachia Season 4
]

if not os.path.isfile("matches.json"):
    leaguematches = []
    for league in leagues:
        url = f"https://api.opendota.com/api/leagues/{league}/matches"
        try:
            response = requests.get(url)
            response.raise_for_status()

            jsondata = response.json()
            for val in jsondata:
                leaguematches.append(val)

        except requests.exceptions.RequestException as e:
            print(e)

    with open("matches.json", "w") as file:
        file.write(json.dumps(leaguematches))

with open("matches.json", "r") as file:
    matches = json.load(file)

if not os.path.isdir("matches"):
    os.mkdir("matches")

counter = 0;
counter_start = time.time()
counter_end = time.time()
for match in matches:
    if counter == 60:
        counter_end = time.time()
        elapsed = counter_end - counter_start
        if elapsed > 0 and elapsed < 60:
            time.sleep(elapsed)
        counter_start = time.time()
        counter = 0

    matchid = match["match_id"]
    if os.path.isfile(f"matches/{matchid}.json"):
        continue

    url = f"https://api.opendota.com/api/matches/{matchid}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(response.headers)

        with open(f"matches/{matchid}.json", 'w') as file:
            json.dump(response.json(), file)
    except requests.exceptions.RequestException as e:
        print(e)
    counter = counter + 1