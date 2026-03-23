"""
Danish optical company configuration.
Top 5 optical companies in Denmark with their social media handles.
Platforms monitored: Facebook, Instagram.
"""

COMPANIES = [
    {
        "id": "synoptik",
        "name": "Synoptik",
        "website": "https://www.synoptik.dk",
        "socials": {
            "facebook": {"page_id": "synoptik.dk", "url": "https://www.facebook.com/synoptik.dk"},
            "instagram": {"username": "synoptikdk", "url": "https://www.instagram.com/synoptikdk/"},
        },
    },
    {
        "id": "louis_nielsen",
        "name": "Louis Nielsen",
        "website": "https://www.louisnielsen.dk",
        "socials": {
            "facebook": {"page_id": "louisnielsen", "url": "https://www.facebook.com/louisnielsen"},
            "instagram": {"username": "louis.nielsen", "url": "https://www.instagram.com/louis.nielsen/"},
        },
    },
    {
        "id": "profil_optik",
        "name": "Profil Optik",
        "website": "https://www.profiloptik.dk",
        "socials": {
            "facebook": {"page_id": "profiloptik", "url": "https://www.facebook.com/profiloptik"},
            "instagram": {"username": "profiloptik", "url": "https://www.instagram.com/profiloptik/"},
        },
    },
    {
        "id": "thiele",
        "name": "Thiele",
        "website": "https://www.thiele.dk",
        "socials": {
            "facebook": {"page_id": "thieleoptik", "url": "https://www.facebook.com/thieleoptik"},
            "instagram": {"username": "thiele.dk", "url": "https://www.instagram.com/thiele.dk/"},
        },
    },
    {
        "id": "nyt_syn",
        "name": "Nyt Syn",
        "website": "https://www.nytsyn.dk",
        "socials": {
            "facebook": {"page_id": "nytsyn", "url": "https://www.facebook.com/nytsyn"},
            "instagram": {"username": "nytsyndanmark", "url": "https://www.instagram.com/nytsyndanmark/"},
        },
    },
]

# Time filter presets (in days)
TIME_FILTERS = {
    "1_day": 1,
    "1_week": 7,
    "1_month": 30,
}
