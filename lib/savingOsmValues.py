import yaml

livingBuild = list(
    [
        "yes",
        "house",
        "apartment",
        "apartments",
        "residential",
        "civic",
        "дом",
        "ДА",
        "semidetached_house",
    ]
)

industrialObj = list(
    [
        "industrial",
        "garages",
        "garage",
        "service",
        "construction",
        "factory",
        "manufacture",
        "hangar",
        "warehouse",
        "storage_tank",
        "constructions",
        "container",
        "recycling",
        "waste_transfer_station",
        "quarry",
    ]
)

retailObj = list(
    [
        "retail",
        "pavilion",
        "store",
        "shop",
        "supermarket",
        "marketplace",
        "pharmacy",
        "kiosk",
    ]
)

adminBuild = list(
    [
        "accountant",
        "advertising_agency",
        "architect",
        "charity",
        "company",
        "consulting",
        "coworking",
        "financial",
        "financial_advisor",
        "forestery",
        "foundation",
        "government",
        "graphic_design",
        "guide",
        "insurance",
        "it",
        "lawyer",
        "logistics",
        "newspaper",
        "notarity",
        "political_party",
        "tax_advisor",
        "telecommunication",
        "yes",
        "education_insitution",
        "commercial",
        "office",
        "offices",
        "bank",
        "courthouse",
        "townhall",
        "government",
        "college",
        "language_school",
        "library",
        "music_school",
        "school",
        "university",
        "dormitory",
        "college",
        "education",
        "research",
        "kindergarten",
        "childcare",
        "public",
        "civic",
        "social_facility",
        "public_building",
        "post_office",
        "police",
        "fire_station",
        "public_service",
        "bureau_de_change",
        "mortuary",
        "crematorium",
        "rescue_station",
        "studio",
        "embassy",
        "ranger_station",
        "internet_cafe",
        "money_transfer",
        "commercial",
        "common",
        "social_club",
    ]
)

publicTransportObj = list(
    [
        "train_station",
        "railway_station",
        "station",
        "platform",
        "stop_position",
        "stop_area",
        "tram_stop",
        "subway_entrance",
        "subway",
        "stop",
        "bus_stop",
        "public_transport",
        "stop_area_group",
        "yes",
        "stop",
        "transportation",
        "bus_station",
        "taxi",
        "ferry_terminal",
    ]
)

histObj = list(
    [
        "aircraft",
        "aqueduct",
        "archaeological_site",
        "battlefield",
        "building",
        "cannon",
        "castle",
        "castle_wall",
        "charcoal_pile",
        "city_gate",
        "citywalls",
        "fort",
        "locomotive",
        "manor",
        "memorial",
        "mine",
        "mine_shaft",
        "milestone",
        "monument",
        "ruins",
        "rune_stone",
        "ship",
        "stone",
        "tank",
        "tomb",
        "vehicle",
        "wreck",
        "yes",
        "wayside_shrine",
        "clock",
        "fountain",
        "attraction",
        "heritage=6",
        "heritage=4",
        "heritage=2",
    ]
)

foodObj = list(
    ["bar", "cafe", "fast_food", "food_court", "pub", "restaurant", "ice_cream"]
)

medsObj = list(
    [
        "veterinary",
        "clinic",
        "dantist",
        "doctors",
        "hospital",
        "nursing_house",
        "sanatorium",
        "ail",
        "policlinic",
        "veterinary",
    ]
)

entertainObj = list(
    [
        "art_centre",
        "casino",
        "cinema",
        "community_centre",
        "planetarim",
        "theatre",
        "riding_hall",
        "aquarium",
        "artwork",
        "gallery",
        "museum",
        "adult_gaming_centre",
        "amusement_arcade",
        "pub",
        "hookah_lounge",
        "club",
        "nightclub",
        "stripclub",
        "cemetery",
        "grave_yard",
        "information",
        "gambling",
        "dance",
        "toilets",
        "drinking_water",
        "outdoor_seating",
        "playground",
        "vending_machine",
        "payment_terminal",
        "atm",
        "training",
        "smoking_area",
        "charging_station",
        "device_charging_station",
    ]
)

campObj = list(
    [
        "attraction",
        "viewpoint",
        "yes",
        "hunting_stand",
        "camp_pitch",
        "camp_site",
        "caravan_site",
        "bbq",
        "picnic_site",
        "alpine_hut",
        "wilderness_hut",
        "guest_house",
        "bungalow",
        "detached",
        "farm",
        "apartment",
        "chalet",
        "cabin",
        "turbaza",
        "туристический комплекс",
        "hunting_lodge",
        "water_point",
        "shower",
        "summer_camp",
        "firepit",
    ]
)

greenObj = list(
    [
        "nature_reserve",
        "fishning",
        "picnic_table",
        "farmland",
        "farmyard",
        "barn",
        "shed",
        "stable",
        "stables",
        "horse_riding",
        "cowshed",
        "household",
        "farm",
        "farm_auxiliary",
        "allotment_house",
        "vineyard",
        "greenhouse_horticulture",
    ]
)

parkObj = list(["national_park", "garden", "park", "water_park", "theme_park", "zoo"])

hotelObj = list(
    [
        "hotel",
        "resort",
        "hostel",
        "motel",
        "guest_house",
        "healthcare",
        "spa",
        "spa_resort",
    ]
)

beachObj = list(
    [
        "beach_resort",
        "marina",
        "swimming_area",
        "yacht_club",
        "boat_rental",
        "river_rafting",
        "lighthouse",
        "boat_storage",
    ]
)

sportObj = list(
    [
        "public_bath",
        "pavilion",
        "riding_hall",
        "sports_hall",
        "stadium",
        "fitness_centre",
        "fitness_station",
        "golf_course",
        "ice_rink",
        "pitch",
        "sports_centre",
        "stadium",
        "swimming_pool",
        "track",
        "bleachers",
        "ski_rental",
        "winter_sports",
        "grandstand",
        "driving_school",
        "gym",
        "recreation_ground",
        "sport",
        "ski_resort",
        "skatepark",
        "trampoline_park",
        "table_tennis_table",
        "racetrack",
    ]
)

religiousObj = list(
    [
        "monastery",
        "place_of_worship",
        "shrine",
        "cathedral",
        "chapel",
        "church",
        "mosque",
        "religious",
        "synagogue",
        "religion",
        "temple",
        "wayside_shrine",
        "churchyard",
    ]
)

prisonObj = list(["prison", "concentration_camp", "labor_camp"])

militaryObj = list(["military"])

parkingObj = list(
    [
        "parking",
        "fuel",
        "car_wash",
        "car_repair",
        "carport",
        "vehicle_ramp",
        "car_rental",
        "vehicle_inspection",
        "driver_training",
        "motorcycle_parking",
        "car_sharing",
        "bicycle_parking",
        "bicycle_rental",
        "parking_space",
        "parking_entrance",
        "sally_port",
        "port",
    ]
)

roadObj = list(
    [
        "rail",
        "monorail",
        "tram",
        "service",
        "residential",
        "primary",
        "trunk",
        "living_street",
        "road",
        "highway",
        "motorway",
        "secondary",
        "tertiary",
    ]
)

allBuild = livingBuild + adminBuild 

allOsmObj = {
    "allBuild": allBuild,
    "livingBuild": livingBuild,
    "industrialObj": industrialObj,
    "retailObj": retailObj,
    "adminBuild": adminBuild,
    "publicTransportObj": publicTransportObj,
    "histObj": histObj,
    "foodObj": foodObj,
    "medsObj": medsObj,
    "entertainObj": entertainObj,
    "campObj": campObj,
    "greenObj": greenObj,
    "parkObj": parkObj,
    "hotelObj": hotelObj,
    "beachObj": beachObj,
    "sportObj": sportObj,
    "religiousObj": religiousObj,
    "prisonObj": prisonObj,
    "militaryObj": militaryObj,
    "parkingObj": parkingObj,
    "roadObj": roadObj,
}

with open("osmValues.yml", "w") as outfile:
    yaml.dump(allOsmObj, outfile)