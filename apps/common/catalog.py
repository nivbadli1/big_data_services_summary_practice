"""
QuickBite catalog - the "master data" shared by the seed, the simulators and the chaos script.

QuickBite is a FICTIONAL Israeli food-delivery platform (think "an app like Wolt").
Everything here is made up: restaurants, couriers, customers, streets.
"""

# ----- Restaurants ---------------------------------------------------------------
# restaurant_id, name, canonical city, cuisine, commission_pct (what QuickBite charges the restaurant)
RESTAURANTS = [
    ("R-01", "Hummus Abu Dani",      "Tel Aviv",    "hummus",  18.0),
    ("R-02", "Sabich Corner",        "Tel Aviv",    "street",  15.0),
    ("R-03", "Pizza Yafo",           "Tel Aviv",    "pizza",   22.0),
    ("R-04", "Sushi Ha'Namal",       "Tel Aviv",    "sushi",   25.0),
    ("R-05", "Burger Rothschild",    "Tel Aviv",    "burger",  20.0),
    ("R-06", "Green Bowl",           "Tel Aviv",    "salad",   20.0),
    ("R-07", "Shawarma King",        "Tel Aviv",    "street",  15.0),
    ("R-08", "Noodle Bar Florentin", "Tel Aviv",    "asian",   22.0),
    ("R-09", "Bialik Bakery",        "Ramat Gan",   "bakery",  15.0),
    ("R-10", "Diamond Grill",        "Ramat Gan",   "grill",   20.0),
    ("R-11", "Pasta Bursa",          "Ramat Gan",   "italian", 22.0),
    ("R-12", "Katzenelson Falafel",  "Givatayim",   "street",  15.0),
    ("R-13", "Holon Home Food",      "Holon",       "home",    18.0),
    ("R-14", "Taco Holon",           "Holon",       "mexican", 20.0),
    ("R-15", "Marina Fish",          "Herzliya",    "fish",    25.0),
    ("R-16", "Pituah Poke",          "Herzliya",    "poke",    22.0),
    ("R-17", "Petah Tikva Pizza",    "Petah Tikva", "pizza",   20.0),
    ("R-18", "Hadar Schnitzel",      "Petah Tikva", "home",    18.0),
    ("R-19", "Negev Grill",          "Be'er Sheva", "grill",   18.0),
    ("R-20", "Carmel Hummus",        "Haifa",       "hummus",  18.0),
]

# ----- Menu: dish name, price in ILS, per cuisine --------------------------------
MENU = {
    "hummus":  [("Hummus Masabacha", 42.0), ("Hummus Ful", 39.0), ("Pita Basket", 12.0)],
    "street":  [("Sabich", 34.0), ("Falafel Pita", 26.0), ("Shawarma Laffa", 49.0), ("Fries", 18.0)],
    "pizza":   [("Margherita", 58.0), ("Pizza Funghi", 64.0), ("Garlic Bread", 22.0)],
    "sushi":   [("Salmon Roll", 46.0), ("Tuna Sashimi", 62.0), ("Edamame", 19.0)],
    "burger":  [("Classic Burger", 62.0), ("Cheeseburger", 68.0), ("Sweet Potato Fries", 24.0)],
    "salad":   [("Quinoa Bowl", 54.0), ("Greek Salad", 48.0), ("Lemonade", 16.0)],
    "asian":   [("Pad Thai", 56.0), ("Ramen", 59.0), ("Gyoza", 32.0)],
    "bakery":  [("Croissant", 14.0), ("Shakshuka Brunch", 52.0), ("Cappuccino", 14.0)],
    "grill":   [("Mixed Grill", 89.0), ("Kebab Plate", 66.0), ("Chips", 18.0)],
    "italian": [("Penne Arrabbiata", 54.0), ("Lasagna", 62.0), ("Tiramisu", 32.0)],
    "home":    [("Schnitzel Plate", 58.0), ("Meatballs & Rice", 55.0), ("Majadra", 38.0)],
    "mexican": [("Tacos x3", 52.0), ("Burrito", 56.0), ("Nachos", 34.0)],
    "fish":    [("Grilled Sea Bream", 98.0), ("Fish & Chips", 72.0), ("Ceviche", 54.0)],
    "poke":    [("Salmon Poke", 64.0), ("Tofu Poke", 54.0), ("Miso Soup", 18.0)],
}

# ----- City spellings: canonical name -> how people / old app versions typed it ---
# The first spelling is the "correct" one. Repeated entries make it more frequent.
CITY_SPELLINGS = {
    "Tel Aviv":    ["Tel Aviv", "Tel Aviv", "Tel Aviv", "Tel Aviv", "tel aviv", "Tel-Aviv",
                    "TLV", "Tel Aviv-Yafo", "Tel Aviv "],
    "Ramat Gan":   ["Ramat Gan", "Ramat Gan", "Ramat Gan", "ramat gan", "Ramat-Gan"],
    "Givatayim":   ["Givatayim", "Givatayim", "Giv'atayim"],
    "Holon":       ["Holon", "Holon", "holon"],
    "Herzliya":    ["Herzliya", "Herzliya", "Herzlia"],
    "Petah Tikva": ["Petah Tikva", "Petah Tikva", "Petach Tikva", "Petah Tiqva"],
    "Be'er Sheva": ["Be'er Sheva", "Beer Sheva", "Beersheba"],
    "Haifa":       ["Haifa", "Haifa", "haifa"],
}

STREETS = ["Dizengoff", "Ibn Gabirol", "Allenby", "Herzl", "Jabotinsky", "Ben Yehuda",
           "Rothschild", "Weizmann", "Sokolov", "HaNasi", "Bialik", "Arlozorov"]

FIRST_NAMES = ["Noa", "Yonatan", "Maya", "Itai", "Tamar", "Omer", "Shira", "Daniel",
               "Yael", "Amit", "Lior", "Roni", "Eden", "Guy", "Michal", "Ariel"]
LAST_NAMES = ["Cohen", "Levi", "Mizrahi", "Peretz", "Biton", "Friedman", "Azoulay",
              "Katz", "Shapiro", "Ben-David", "Golan", "Avraham"]

# ----- Couriers ------------------------------------------------------------------
N_COURIERS = 30
COURIERS = [f"C-{i:02d}" for i in range(1, N_COURIERS + 1)]

# Gush Dan bounding box for the courier map (kept east of the beach line).
GUSH_DAN = {"lat_min": 32.010, "lat_max": 32.170, "lon_min": 34.775, "lon_max": 34.900}

# Order lifecycle (lower-case = the official values)
STATUSES = ["created", "accepted", "picked_up", "delivered", "cancelled"]


def restaurant_by_id():
    """{restaurant_id: {...}} - handy lookup used by the simulators."""
    return {r[0]: {"restaurant_id": r[0], "name": r[1], "city": r[2],
                   "cuisine": r[3], "commission_pct": r[4]} for r in RESTAURANTS}
