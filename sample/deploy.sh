#!/bin/bash
# https://docs.travis-ci.com/user/customizing-the-build#Implementing-Complex-Build-Steps
set -ev

git config --global user.email "openmaptiles@klokantech.com"
git config --global user.name "OpenMapTiles Travis"

# deploy
rm -f ./data/tile.json
cp ./sample/tile.json ./data/
rm -rf ./data/tiles
git clone https://github.com/mapbox/mbutil.git
cd data
./../mbutil/mb-util --image_format=pbf --silent tiles.mbtiles tiles
cd tiles
gzip -d -r -S .pbf *
find . -type f ! -name 'metadata.json' -exec mv '{}' '{}'.pbf \;
cd ..
git init
git add tiles.mbtiles
git add quickstart.log
git add tile.json
git add tiles/**/*
git commit --quiet -m "Deploy to Github Pages"
git push --force --quiet "https://${GITHUB_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git" master:gh-pages > /dev/null 2>&1
