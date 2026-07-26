#!/bin/sh
# Decides whether a WP run actually proved anything.
#
# frama-c exits 0 whether or not it discharges every goal, so its exit status
# says only that the tool ran. Rules on a -wp-report-json report, which carries
# a verdict per goal rather than a count scraped out of the console summary.
set -eu

report="${1:?usage: check-proved.sh <wp-report.json>}"

case "$report" in
*.json) ;;
*)
	echo "check-proved: $report is not a JSON report; pass -wp-report-json output" >&2
	exit 1
	;;
esac

if [ ! -s "$report" ]; then
	echo "check-proved: $report is empty or missing; WP did not run" >&2
	exit 1
fi

total=$(jq 'length' "$report")
if [ "$total" -eq 0 ]; then
	echo "check-proved: no goals in $report; WP proved nothing" >&2
	exit 1
fi

# `passed` is false for an unproved goal and for a smoke test that succeeded,
# which is a reachable contradiction. Both are failures here.
failed=$(jq '[.[] | select(.passed == false)] | length' "$report")
if [ "$failed" -ne 0 ]; then
	echo "check-proved: $failed of $total goals not proved in $report" >&2
	jq -r '.[] | select(.passed == false)
	       | "  \(.goal) [\(if .smoke then "smoke" else "goal" end)] \(.verdict)"' \
		"$report" >&2
	exit 1
fi

# Without smoke tests a vacuous precondition reports green.
smoke=$(jq '[.[] | select(.smoke == true)] | length' "$report")
if [ "$smoke" -eq 0 ]; then
	echo "check-proved: no smoke tests in $report; run with -wp-smoke-tests" >&2
	exit 1
fi
