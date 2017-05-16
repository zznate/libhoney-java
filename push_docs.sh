if [ "$TRAVIS_BRANCH" == "master" ]; then
# Unfortunately javadoc.io doesn't pull from GitHub
set -e

# set up git
git config --global user.email "travis@travis-ci.org"
git config --global user.name "Travis CI"

# build and commit website files
javadoc -d javadoc -sourcepath src/main/java/ io.honeycomb

# Check out orphan gh-pages branch, get it set up correctly
git checkout --orphan gh-pages
git reset
git add javadoc/
git mv javadoc/* ./
git add .gitignore
git clean -fd
git commit -m "Travis build: $TRAVIS_BUILD_NUMBER"

# Pushing via secure GH_TOKEN
git remote add origin-pages https://${GH_TOKEN}@github.com/honeycombio/libhoney-java.git > /dev/null 2>&1
git push --force --quiet --set-upstream origin-pages gh-pages
fi
