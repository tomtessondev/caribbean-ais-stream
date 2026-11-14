#!/usr/bin/env bash
# Assemble map screenshots (f0000.png, f0001.png, ...) into a small looping GIF.
# Usage: scripts/frames-to-gif.sh <frames dir> <out.gif> [keep every Nth frame] [fps] [width]
set -euo pipefail

frames=$1
out=$2
step=${3:-2}
fps=${4:-10}
width=${5:-720}

# One palette for the whole clip keeps the map colours stable from frame to frame.
filters="select='not(mod(n\,${step}))',setpts=N/${fps}/TB,scale=${width}:-1:flags=lanczos"
ffmpeg -loglevel error -y -framerate 30 -i "${frames}/f%04d.png" \
  -vf "${filters},split[a][b];[a]palettegen=max_colors=128[p];[b][p]paletteuse=dither=bayer:bayer_scale=4" \
  -r "${fps}" "${out}"
ls -lh "${out}"
