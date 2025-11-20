#!/usr/bin/env python3
"""Seed MongoDB with test data and simulate CDC events.

This script:
1. Seeds initial data
2. Performs updates, inserts, and deletes to generate change stream events
"""

import argparse
import time
from datetime import datetime

import pymongo
from faker import Faker


class Params(argparse.Namespace):
    """Namespace for command line arguments."""

    connection_string: str
    database: str
    simulate_changes: bool


def seed_initial_data(db):
    """Seed initial data into the database."""
    fake = Faker()
    print("Seeding initial data...")

    # Clear existing data
    db["users"].delete_many({})
    db["posts"].delete_many({})

    # Create users
    users_oids = []
    for i in range(50):
        result = db["users"].insert_one(
            {
                "name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "joined_at": fake.date_time_this_decade(),
                "active": True,
            }
        )
        users_oids.append(result.inserted_id)
        if (i + 1) % 10 == 0:
            print(f"  Inserted {i + 1} users...")

    # Create posts
    for i in range(200):
        created_at = fake.date_time_this_decade()
        db["posts"].insert_one(
            {
                "title": fake.sentence(),
                "content": fake.text(),
                "user_id": fake.random_element(users_oids),
                "created_at": created_at,
                "updated_at": created_at,
                "views": fake.random_int(min=0, max=1000),
                "published": fake.boolean(),
            }
        )
        if (i + 1) % 50 == 0:
            print(f"  Inserted {i + 1} posts...")

    print(f"\n✓ Initial data seeded: {len(users_oids)} users, 200 posts")
    return users_oids


def simulate_cdc_events(db, users_oids):
    """Simulate various CDC events to test change stream capturing."""
    fake = Faker()
    print("\nSimulating CDC events...")
    print("=" * 60)

    # 1. INSERT new users
    print("\n1. Inserting new users...")
    new_users = []
    for _i in range(5):
        result = db["users"].insert_one(
            {
                "name": fake.name(),
                "email": fake.email(),
                "address": fake.address(),
                "joined_at": datetime.now(),
                "active": True,
            }
        )
        new_users.append(result.inserted_id)
        print(f"   ✓ Inserted user: {result.inserted_id}")
        time.sleep(0.5)

    # 2. UPDATE existing users
    print("\n2. Updating existing users...")
    for _i in range(10):
        user_id = fake.random_element(users_oids)
        db["users"].update_one(
            {"_id": user_id},
            {
                "$set": {
                    "address": fake.address(),
                    "updated_at": datetime.now(),
                }
            },
        )
        print(f"   ✓ Updated user: {user_id}")
        time.sleep(0.5)

    # 3. INSERT new posts
    print("\n3. Inserting new posts...")
    all_users = users_oids + new_users
    for _i in range(10):
        result = db["posts"].insert_one(
            {
                "title": fake.sentence(),
                "content": fake.text(),
                "user_id": fake.random_element(all_users),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "views": 0,
                "published": True,
            }
        )
        print(f"   ✓ Inserted post: {result.inserted_id}")
        time.sleep(0.5)

    # 4. UPDATE posts (increment views)
    print("\n4. Updating posts (simulating views)...")
    posts = list(db["posts"].find().limit(15))
    for post in posts:
        db["posts"].update_one(
            {"_id": post["_id"]},
            {
                "$inc": {"views": fake.random_int(min=1, max=50)},
                "$set": {"updated_at": datetime.now()},
            },
        )
        print(f"   ✓ Updated post views: {post['_id']}")
        time.sleep(0.5)

    # 5. REPLACE a document
    print("\n5. Replacing a user document...")
    user_to_replace = fake.random_element(users_oids)
    db["users"].replace_one(
        {"_id": user_to_replace},
        {
            "name": fake.name(),
            "email": fake.email(),
            "address": fake.address(),
            "joined_at": datetime.now(),
            "active": True,
            "replaced": True,
        },
    )
    print(f"   ✓ Replaced user: {user_to_replace}")
    time.sleep(1)

    # 6. DELETE some posts
    print("\n6. Deleting posts...")
    posts_to_delete = list(db["posts"].find().limit(5))
    for post in posts_to_delete:
        db["posts"].delete_one({"_id": post["_id"]})
        print(f"   ✓ Deleted post: {post['_id']}")
        time.sleep(0.5)

    # 7. DELETE a user
    print("\n7. Deleting a user...")
    user_to_delete = new_users[0] if new_users else fake.random_element(users_oids)
    db["users"].delete_one({"_id": user_to_delete})
    print(f"   ✓ Deleted user: {user_to_delete}")
    time.sleep(1)

    print("\n" + "=" * 60)
    print("✓ CDC event simulation complete!")
    print("\nSummary of changes:")
    print(f"  - {len(new_users)} users inserted")
    print("  - 10 users updated")
    print("  - 1 user replaced")
    print("  - 1 user deleted")
    print("  - 10 posts inserted")
    print("  - 15 posts updated")
    print("  - 5 posts deleted")
    print("\nTotal change events: ~47")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Seed MongoDB with test data for CDC testing")
    parser.add_argument(
        "--connection-string",
        default="mongodb://localhost:27017/?replicaSet=rs0&directConnection=true",
        help="MongoDB connection string",
    )
    parser.add_argument(
        "--database",
        default="test",
        help="Database name",
    )
    parser.add_argument(
        "--simulate-changes",
        action="store_true",
        help="Simulate CDC events after initial seed",
    )
    args = parser.parse_args(namespace=Params())

    print("Connecting to MongoDB...")
    client = pymongo.MongoClient(args.connection_string)
    db = client[args.database]

    # Test connection
    try:
        client.admin.command("ping")
        print("✓ Connected to MongoDB")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        return

    # Seed initial data
    users_oids = seed_initial_data(db)

    # Optionally simulate CDC events
    if args.simulate_changes:
        print("\nWaiting 3 seconds before simulating changes...")
        time.sleep(3)
        simulate_cdc_events(db, users_oids)

    print("\n" + "=" * 60)
    print("Done!")


if __name__ == "__main__":
    main()
