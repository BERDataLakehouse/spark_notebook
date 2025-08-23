#!/bin/bash
set -x

echo "Running custom pre jupyter stack start.sh steps..."
#adduser


exec tini -g -- /usr/local/bin/start.sh "$@"