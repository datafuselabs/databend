#!/usr/bin/env bash

perform_initial_query() {
    local response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select avg(number) from numbers(1000000000)"}')
    local stats_uri=$(echo "$response" | jq -r '.stats_uri')
    local final_uri=$(echo "$response" | jq -r '.final_uri')
    echo "$stats_uri|$final_uri"
}
poll_stats_uri() {
    local uri=$1
    local state_exists=true
    while $state_exists; do
        local response=$(curl -s -u root: -XGET "http://localhost:8000$uri")
        if ! echo "$response" | jq -e '.state' > /dev/null; then
            state_exists=false
        else
            sleep 2
        fi
    done
}
get_final_state() {
    local uri=$1
    local response=$(curl -s -u root: -XGET "http://localhost:8000$uri")
    echo "$response" | jq -r '.state'
}

IFS='|' read -r stats_uri final_uri <<< $(perform_initial_query)

poll_stats_uri "$stats_uri" &
POLL_PID=$!
sleep 1
netstat_output=$(netstat -an | grep '9092')

# skip standalone mode
if [ -z "$netstat_output" ]; then
    echo "Final state: Succeeded"
    exit 0
fi

port=$(echo "$netstat_output" | awk '
    $NF == "ESTABLISHED" {
        if ($4 ~ /:9092$/) {
            split($5, a, ":")
            port = a[2]
        } else if ($5 ~ /:9092$/) {
            split($4, a, ":")
            port = a[2]
        }
    }
    END {
        print port
    }
')

# Start tcpkill in the background
sudo tcpkill -i lo host 127.0.0.1 and port $port > tcpkill_output.txt 2>&1 &
TCPKILL_PID=$!

# Wait for tcpkill to output at least 3 lines or terminate if done earlier
while [ $(wc -l < tcpkill_output.txt) -lt 3 ]; do
    if ! kill -0 $TCPKILL_PID 2> /dev/null; then
        break
    fi
    sleep 1
done

# Kill tcpkill after the desired number of lines if it's still running
if kill -0 $TCPKILL_PID 2> /dev/null; then
    kill $TCPKILL_PID
fi

wait $POLL_PID

final_state=$(get_final_state "$final_uri")
echo "Final state: $final_state"

