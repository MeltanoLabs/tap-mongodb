# /// script
# dependencies = [
#   "faker",
#   "pymongo",
# ]
# ///

import argparse

import pymongo
from faker import Faker


class Params(argparse.Namespace):
    host: str
    database: str
    username: str
    password: str
    port: int


def seed():
    """Seed the database with fake data.
    
    - users: 100
    - posts: 1000
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost", help="MongoDB host")
    parser.add_argument("--database", required=True, help="MongoDB database")
    parser.add_argument("--username", default=None, help="MongoDB username")
    parser.add_argument("--password", default=None, help="MongoDB password")
    parser.add_argument("--port", default=27017, help="MongoDB port")
    args = parser.parse_args(namespace=Params())

    fake = Faker()
    uri = f"mongodb://{args.username}:{args.password}@{args.host}:{args.port}"
    client = pymongo.MongoClient(uri)
    db = client[args.database]

    users = db["users"]
    users.delete_many({})
    users_oids = set()

    for _ in range(100):
        result = users.insert_one({
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "joined_at": fake.date_time_this_decade(),
        })
        users_oids.add(result.inserted_id)

    posts = db["posts"]
    posts.delete_many({})
    for _ in range(1000):
        created_at = fake.date_time_this_decade()
        updated_at = fake.date_between_dates(created_at)
        posts.insert_one({
            "title": fake.sentence(),
            "content": fake.text(),
            "user_id": fake.random_element(users_oids),
            # updated_at is always greater than created_at
            "created_at": created_at,
            "updated_at": updated_at,
        })


if __name__ == "__main__":
    seed()
