from datetime import datetime, timedelta
from BAScraper.BAScraper_async import PullPushAsync
import asyncio

reddits = [
    "belgium",
    "Belgium2",
    "Belgium4",
    "belgie",
    "belgique",
    "nederlands",
    "Nederland",
    "Benelux",
    "Telegraaf",
    "Dutchnews",
    "PVV",
    "fvd",
    "PVDA_PTB",
    "VVD",
    "GroenLinks",
    "partijvoordedieren",
    "D66",
    "VoltNederland",
    "JA21",
    "Vooruit",
    "openvld",
    "VlaamsBelang"
]

keywords = [
 "asielzoeker",
 "asylum seeker",
 "buitenlander",
 "demandeur d'asile",
 "diaspora",
 "dispersed population",
 "displaced people",
 "emigrant",
 "expat",
 "expatriate",
 "expatrié"
 "fleeing population",
 "herplaatste bevolking",
 "immigrant",
 "immigratie",
 "immigration",
 "migratie",
 "migration",
 "ontheemde",
 "oorlogsvluchteling",
 "personne relocalisée",
 "personnes déplacées",
 "population dispersée",
 "population fuyante",
 "refugee",
 "relocated population",
 "réfugié",
 "réfugiés de guerre",
 "verspreide bevolking",
 "vluchteling",
 "vluchtende bevolking",
 "war refugees",
 "émigrant",
 "étranger",
 "migrant"
]

def fetch(reddit: str, keyword: str):
    # `log_stream_level` can be one of DEBUG, INFO, WARNING, ERROR
    ppa = PullPushAsync(log_stream_level="INFO", sleep_sec=4, task_num=1)

    # basic fetching with comments
    asyncio.run(ppa.get_submissions(subreddit=reddit,
                                    after=datetime.timestamp(datetime(2022, 2, 1)),
                                    before=datetime.timestamp(datetime(2024, 11, 30)),
                                    file_name=f"reddit-{reddit}-{keyword}",
                                    q=keyword,
                                    get_comments=True,
                                    ))


if __name__ == "__main__":
    for reddit in reddits:
        for keyword in keywords:
            print(f"Fetching reddit {reddit}, keyword {keyword}")
            fetch(reddit, keyword)