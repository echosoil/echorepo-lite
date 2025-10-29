cd ../echorepo-lite
git fetch --all
git switch main
git merge --no-ff origin/develop
git tag -a v$(date +%Y.%m.%d-%H%M) -m "Release"
git push --follow-tags
