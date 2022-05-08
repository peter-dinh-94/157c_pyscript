import pymongo
from pymongo import MongoClient

menu_option = {1: 'Find Business by Name', 2: 'Find Nearby Businesses By Location', 3: 'Find Businesses by Category.',
               4: 'Find High Rated Businesses', 5: 'Find Popular Businesses', 6: 'Find popular businesses that allow pets',
               7: 'Find popular restaurants that do not require reservations', 8: 'Find populat businesses that are wheel chair accessible',
               9: 'Find popular restaurants that are good for kids', 10: 'Find popular restaurants that have outdoor seating',
               11: 'Find popular businesses that are good for groups', 12: 'Find users join from a specific date',
               13: 'Find user by name', 14: 'Find elite users in a specific year',
               15: 'Find influenced user', 16: 'Exit', 0: 'Select another postal code'}

user_projection = {"_id": 0, "friends": 0, "compliment_hot": 0, "compliment_more": 0, "compliment_profile": 0,
                    "compliment_cute": 0, "compliment_list": 0, "compliment_note": 0, "compliment_plain": 0,
                    "compliment_cool": 0, "compliment_funny": 0, "compliment_writer": 0, "compliment_photos": 0} 
def print_menu(menu_option):
    print("\n-----------Welcome to our Yelp Cloning Application--------------")
    for key, value in menu_option.items():
        print("|" + str(key) + ". " + str(value))
    print("----------------------------------------------------------------")


def main():
    mongoClient = None
    try:
        mongoClient = MongoClient("localhost:27017")
        print("Connected successfully!!!")
    except:
        print("Could not connect to MongoDB")

    db = mongoClient["projectDatabase"]
    businessCollection = db["business"]
    userCollection = db["user"]

    get_pc = True
    while (True):
        print_menu(menu_option)
        if get_pc:
            postal_code = input('**NOTE**: Due to the limit of Yelp dataset\n \
            Please use some of these postal code for best experience\n \
            Postal code: 19103, 89511, 19064, 19372\n \
            Please enter your postal code: ')
            get_pc = False
        option = int(input('Please enter your choice: '))

        if option == 16:
            mongoClient.close()
            break
        elif option == 1:
            find_business_by_name(businessCollection, postal_code)
        elif option == 2:
            find_nearby_businesses_by_loc(businessCollection, postal_code)
        elif option == 3:
            find_businesses_by_category(businessCollection, postal_code)
        elif option == 4:
            fetch_high_rated_business(businessCollection, postal_code)
        elif option == 5:
            fetch_popular_business(businessCollection, postal_code)
        elif option == 6:
            fetch_business_allowing_pets(businessCollection, postal_code)
        elif option == 7:
            fetch_restaurants_without_reservations(
                businessCollection, postal_code)
        elif option == 8:
            fetch_wheelchair_accesible_businesses(
                businessCollection, postal_code)
        elif option == 9:
            fetch_goodforkids_restaurants(businessCollection, postal_code)
        elif option == 10:
            fetch_outdoor_seating_restaurants(businessCollection, postal_code)
        elif option == 11:
            fetch_good_for_groups(businessCollection, postal_code)
        elif option == 12:
            fetch_user_on_date(userCollection)
        elif option == 13:
            fetch_user_by_name(userCollection)
        elif option == 14:
            fetch_elite_user(userCollection)
        elif option == 15:
            fetch_influenced_user(userCollection)
        elif option == 0:
            get_pc = True


def find_business_by_name(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    name = input("Please Enter Business Name: ")
    output = collection.find({"$and": [shard_query, {"name": {"$regex": name}}]},
                             {"name": 1, "address": 1, "city": 1, "state": 1, "postal_code": 1,
                              "stars": 1, "_id": 0}).limit(5)
    for doc in output:
        printDoc(doc)


# function 2
def find_nearby_businesses_by_loc(collection, postal_code):
    shard_query = {"postal_code": "70130"}
    info = input("For testing, please enter -90.065639019,29.950570141,5 \n \
    Default postal code is 70130 \n \
    Please enter your <longitude>,<latitude>,<distance(in mile)>: ")
    info = info.split(',')
    radius = float(info[2]) / 3963.2
    location_query = {"location": {"$geoWithin": {
        "$centerSphere": [[float(info[0]), float(info[1])], radius]}}}

    cursor = collection.find({"$and": [shard_query, location_query]},
                             {"name": 1, "address": 1, "postal_code": 1, "city": 1, "location": 1, "_id": 0}).limit(5)
    for record in cursor:
        printDoc(record)


# function 3
def find_businesses_by_category(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    category = input(
        "Please enter a category!\nFor example: Fast Food, Sushi, Italian, etc: ")
    category = category.split(',')
    regexQueries = [shard_query]
    for el in category:
        regexQueries.append({"categories": {"$regex": el}})
    output = collection.find({"$and": regexQueries},
                             {"name": 1, "address": 1, "city": 1, "state": 1, "postal_code": 1,
                              "stars": 1, "categories": 1, "_id": 0}).limit(5)
    for doc in output:
        printDoc(doc)

# function 4
def fetch_high_rated_business(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"stars": {"$gte": 4}}
    projection = {"name": 1, "_id": 0, "stars": 1}
    cursor = collection.find(
        {"$and": [shard_query, query]}, projection).limit(5)
    for record in cursor:
        printDoc(record)


# function 5
def fetch_popular_business(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"is_open": 1}
    projection = {"name": 1, "_id": 0, "review_count": 1}
    sort_query = {"review_count": -1}
    cursor = collection.find({"$and": [shard_query, query]}, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 6 find businesses that allow pets
def fetch_business_allowing_pets(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.DogsAllowed": "true"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.DogsAllowed": 1}
    cursor = collection.find({"$and": [shard_query, query]}, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 7 Find restaurants that do not require reservations
def fetch_restaurants_without_reservations(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.RestaurantsReservations": "False"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.RestaurantsReservations": 1}
    cursor = collection.find({"$and": [shard_query, query]}, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 8 Find businesses that are wheelchair accessible
def fetch_wheelchair_accesible_businesses(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.WheelchairAccessible": "True"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.WheelchairAccessible": 1}
    cursor = collection.find({"$and": [shard_query, query]}, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 9 Find restaurants that are good for kids
def fetch_goodforkids_restaurants(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.GoodForKids": "True"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.GoodForKids": 1}
    cursor = collection.find(query, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 10 Find restaurants that have outdoor seating
def fetch_outdoor_seating_restaurants(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.OutdoorSeating": "True"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.OutdoorSeating": 1}
    cursor = collection.find(query, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 11 Find businesses that are good for groups
def fetch_good_for_groups(collection, postal_code):
    shard_query = {"postal_code": postal_code}
    query = {"attributes.RestaurantsGoodForGroups": "True"}
    projection = {"name": 1, "_id": 0, "city": 1,
                  "postal_code": 1, "attributes.RestaurantsGoodForGroups": 1}
    cursor = collection.find(query, projection).sort(
        "review_count", -1).limit(5)
    for record in cursor:
        printDoc(record)

# function 12 Find users join on a specific date
def fetch_user_on_date(collection):
    date = input("Please enter a date in format YYYY-MM-DD: ")
    year = int(date[0:4])
    output = collection.find({"$and": [{"joining_year": year},
        {"yelping_since": {"$regex": date}}]},
                             user_projection).limit(5)
    for doc in output:
        printDoc(doc)

# function 13 Find user by user name
def fetch_user_by_name(collection):
    name = input("Please Enter User Name: ")
    output = collection.find({"name": {"$regex": name}},
                             {"_id": 0, "friends": 0}).limit(5)
    for doc in output:
        printDoc(doc)

# function 14  Find elite user
def fetch_elite_user(collection):
    year = input("Please enter a year: ")
    output = collection.find({"elite": year},
                             {"_id": 0, "friends": 0}).limit(5)
    for doc in output:
        printDoc(doc)

# function 15   Find influenced user
def fetch_influenced_user(collection):
    output = collection.find({"fans": {"$gte": 1400}}, {
                             "_id": 0, "friends": 0}).limit(5)
    for doc in output:
        printDoc(doc)


def printDoc(doc):
    print("{")
    for key in doc:
        print("    " + key + ": " + str(doc[key]))
    print("}")


if __name__ == "__main__":
    main()
