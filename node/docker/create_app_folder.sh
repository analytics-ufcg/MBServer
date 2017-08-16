#!/bin/bash

mkdir -p app

cp -r ../api ./app/
cp -r ../config ./app/
cp ../package.json ./app/
cp ../server.js ./app/

rm app/server_build.sh
touch app/server_build.sh
echo 'npm install' >> app/server_build.sh
echo 'npm run start' >> app/server_build.sh
