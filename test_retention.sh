#!/bin/bash

echo "=== Message Retention & Timestamp Test ==="
echo

# Setup config with message_retention=2
mkdir -p data
cat > data/queue.conf << 'EOF'
port=9294
max_producers=100
max_consumers=100
message_retention=2
EOF

echo "Config created with message_retention=2"
echo "This means each message needs to reach 2 consumers before deletion from WAL"
echo

# Start server in background
echo "Starting queue server..."
./build/protocol queue start &
SERVER_PID=$!
sleep 1

echo "Publishing test messages..."
./build/protocol producer register "first message"
./build/protocol producer register "second message"

echo
echo "=== WAL.log contents (before consumers) ==="
if [ -f WAL.log ]; then
    cat WAL.log
    echo
else
    echo "WAL.log not found"
fi

echo "Messages are stored as: timestamp|content|retention_count"
echo "Format allows reading back structured data with full metadata"
echo

# Cleanup
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "=== Test complete ==="
