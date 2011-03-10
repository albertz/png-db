#!/bin/zsh

DBFILE=db.kch
SRCDIR="$HOME/screenies"
[ "$1" != "" ] && SCRDIR="$1"

zmodload zsh/stat
sizesum=0
numfiles=0

./bin/db-list-dir | while read l; do
	[[ "$l" =~ "^file: /(.*), .*, .*$" ]] && {
		fn="$match"
		size="$(stat -L +size $SRCDIR/$fn)"
		sizesum=$(($sizesum + $size))
		numfiles=$(($numfiles + 1))
	}
done

dbsize="$(stat -L +size $DBFILE)"

echo "num files: $numfiles"
echo "fs size: $(($sizesum / 1024 / 1024.0)) MB"
echo "db size: $(($dbsize / 1024 / 1024.0)) MB"
