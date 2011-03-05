#!/bin/bash

cd "$(dirname $0)"
ROOTDIR="$(pwd)"
INCLUDE="$ROOTDIR"

# $1 - target
# $... - deps
function checkdeps() {
	local t="$1"
	[ ! -e "$t" ] && return -1
	while shift; do
		[ "$1" != "" ] && [ ! -e "$1" ] && return -1
		[ "$1" != "" ] && [ "$1" -nt "$t" ] && return -1
	done
	return 0
}

# $1 - dep-file
function listdeps() {
	sed -e "s/\\\\$//g" -e "s/.*\\.o://g" \
		-e "s/^ *//g" -e "s/ *$//g" \
		"$1"
		#-e "s/^/\\\"/g" -e "s/$/\\\"/g" 
}

# $1 - cpp-file
# will compile the o-file
function srccompile() {
	local f="$1"
	local o="${f/.cpp/.o}"
	[ -e "$f.deps" ] && checkdeps "$o" "$f" $(listdeps "$f.deps") && echo "uptodate: $o" && return 0
	echo "compiling $o"
	g++ -c -MMD -MF "$f.deps" -o "$o" -iquote "$INCLUDE" -g "$f" || exit -1
}

# $1 - bin-file
# will link all the $OBJS together
function srclink() {
	local b="$1"
	checkdeps "$b" $OBJS && echo "uptodate: $b" && return 0
	echo "linking $b"
	g++ $OBJS -o "$b" $2 || exit -1
}

# compile all sources
OBJS=""
for f in *.cpp; do
	srccompile "$f"
	OBJS="$OBJS $(pwd)/${f/.cpp/.o}"
done

srclink "test-png.bin" "-lz"
