#!/bin/sh
# check-proved.sh is what stands between an unproved goal and a green run, and
# it is not exercised by the proofs themselves: a report it wrongly accepts
# looks exactly like a report it rightly accepts. So feed it known reports.
set -eu

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
check="$here/check-proved.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fail=0

expect() {
	want=$1
	name=$2
	shift 2
	got=0
	"$check" "$@" >/dev/null 2>&1 || got=$?
	if [ "$got" -ne "$want" ]; then
		echo "check-proved-test: $name: expected exit $want, got $got" >&2
		fail=1
	fi
}

cat >"$work/ok.json" <<'EOF'
[{"goal":"typed_f_ensures","smoke":false,"verdict":"valid","passed":true},
 {"goal":"smoke_dead_code","smoke":true,"verdict":"unknown","passed":true}]
EOF

# A smoke test whose goal is valid means the code it guards is unreachable.
cat >"$work/unproved.json" <<'EOF'
[{"goal":"typed_f_ensures","smoke":false,"verdict":"timeout","passed":false}]
EOF
cat >"$work/smoke_fired.json" <<'EOF'
[{"goal":"smoke_dead_code","smoke":true,"verdict":"valid","passed":false}]
EOF

echo '[]' >"$work/empty.json"
cat >"$work/no_smoke.json" <<'EOF'
[{"goal":"typed_f_ensures","smoke":false,"verdict":"valid","passed":true}]
EOF
cat >"$work/lemmas.json" <<'EOF'
[{"goal":"typed_lemma_sum_frame","property":"lemma_sum_frame","smoke":false,"verdict":"valid","passed":true}]
EOF
cat >"$work/lemma_and_fn.json" <<'EOF'
[{"goal":"typed_lemma_sum_frame","property":"lemma_sum_frame","smoke":false,"verdict":"valid","passed":true},
 {"goal":"typed_f_ensures","function":"f","smoke":false,"verdict":"valid","passed":true}]
EOF
cat >"$work/lemma_named_fn.json" <<'EOF'
[{"goal":"typed_lemma_f_ensures","function":"lemma_f","smoke":false,"verdict":"valid","passed":true}]
EOF
cat >"$work/lemma_unproved.json" <<'EOF'
[{"goal":"typed_lemma_sum_frame","property":"lemma_sum_frame","smoke":false,"verdict":"timeout","passed":false}]
EOF
: >"$work/truncated.json"
echo 'Proved goals: 12 / 12' >"$work/console.log"

expect 0 "every goal proved"            "$work/ok.json"
expect 1 "goal left unproved"           "$work/unproved.json"
expect 1 "smoke test fired"             "$work/smoke_fired.json"
expect 1 "no goals at all"              "$work/empty.json"
expect 1 "smoke tests never ran"        "$work/no_smoke.json"
expect 0 "lemmas alone, all proved"     "$work/lemmas.json"
expect 1 "lemma left unproved"          "$work/lemma_unproved.json"
expect 1 "lemma and function, no smoke" "$work/lemma_and_fn.json"
expect 1 "function named lemma_*"       "$work/lemma_named_fn.json"
expect 1 "report written but empty"     "$work/truncated.json"
expect 1 "report never written"         "$work/absent.json"
expect 1 "console log instead of JSON"  "$work/console.log"

if [ "$fail" -ne 0 ]; then
	echo "check-proved-test: FAILED" >&2
	exit 1
fi

echo "check-proved-test: 12 cases passed"
