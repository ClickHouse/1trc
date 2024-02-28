#!/bin/bash

# Record start time
start=$(date +%s)

# Run pulumi up with no confirmation. Exit if this fails.
pulumi up -f
if [ $? -ne 0 ]; then
    echo "pulumi up failed, exiting..."
    exit 1
fi

# If pulumi up is successful, run pulumi down with no confirmation.
pulumi down -f

# Record end time
end=$(date +%s)

# Calculate total duration
duration=$((end - start))

echo "Total time: $duration seconds"
