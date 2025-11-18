#!/bin/bash
# Script to initialize MongoDB replica set

echo "Waiting for MongoDB nodes to be ready..."
sleep 10

echo "Initializing replica set..."
mongosh --host mongodb-primary:27017 <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongodb-primary:27017", priority: 2 },
    { _id: 1, host: "mongodb-secondary1:27017", priority: 1 },
    { _id: 2, host: "mongodb-secondary2:27017", priority: 1 }
  ]
});
EOF

echo "Waiting for replica set to elect primary..."
sleep 10

echo "Checking replica set status..."
mongosh --host mongodb-primary:27017 --eval "rs.status()"

echo "Creating test database and collections..."
mongosh --host mongodb-primary:27017 <<EOF
use test;

// Create initial collections
db.createCollection("users");
db.createCollection("posts");

EOF

echo "Replica set initialization complete!"
echo ""
echo "Connection string: mongodb://localhost:27017/?replicaSet=rs0"
echo "Test database: mongodb://localhost:27017/test?replicaSet=rs0"
