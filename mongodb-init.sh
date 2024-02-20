#!/bin/bash

#echo "###### Waiting for localhost:27117 instance startup.."
#until mongosh --host localhost:27117 --username admin --password password --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 2)' &>/dev/null; do
#  printf '.'
#  sleep 1
#done
#echo "###### Working localhost:27117 instance found, initiating user setup & initializing rs setup.."

echo "###### Running replicaset initialize command"
# setup user + pass and initialize replica sets
mongosh --host localhost:27117 <<EOF
  var rootUser = 'admin';
  var rootPassword = 'password';
  var admin = db.getSiblingDB('admin');
  admin.auth(rootUser, rootPassword);

  var config = {
      "_id": "rs0",
      "version": 1,
      "members": [
          {
              "_id": 1,
              // "host": "mongodb-test-1:27117",
              "host": "host.docker.internal:27117",
              "priority": 2
          },
          {
              "_id": 2,
              // "host": "mongodb-test-2:27118",
              "host": "host.docker.internal:27118",
              "priority": 1
          },
          {
              "_id": 3,
              // "host": "mongodb-test-3:27119",
              "host": "host.docker.internal:27119",
              "priority": 1,
              "arbiterOnly": true
          }
      ]
  };
  rs.initiate(config, { force: true });
  rs.status();
EOF

echo "###### Ran mongodb command..."
