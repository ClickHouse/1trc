#!/bin/bash

# Record start time
start=$(date +%s)

# Always destroy infrastructure on exit (including on failure) so that a failed
# `pulumi up` cannot leave expensive instances running.
down_done=0
teardown() {
    if [ "$down_done" -eq 0 ]; then
        down_done=1
        pulumi down -f
    fi
}
trap teardown EXIT

# Run pulumi up with no confirmation. Exit if this fails.
pulumi up -f
if [ $? -ne 0 ]; then
    echo "pulumi up failed, tearing down..."
    exit 1
fi

# If pulumi up is successful, run pulumi down with no confirmation.
teardown

# Record end time
end=$(date +%s)

# Calculate total duration
duration=$((end - start))

echo "Total time: $duration seconds"
