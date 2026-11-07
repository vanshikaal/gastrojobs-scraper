#!/usr/bin/env bash
# Regenerates the synthesised sound effects in public/sfx.
set -euo pipefail
cd "$(dirname "$0")/../public/sfx"
ffmpeg -y -v error -f lavfi -i "aevalsrc='0.9*sin(2*PI*(55+70*exp(-t*30))*t)*exp(-t*9)+0.35*(random(0)-0.5)*exp(-t*40)':s=48000:d=0.45" -ac 2 thump.wav
ffmpeg -y -v error -f lavfi -i "aevalsrc='0.5*sin(2*PI*(500+900*t)*t)*exp(-t*28)':s=48000:d=0.18" -ac 2 pop.wav
ffmpeg -y -v error -f lavfi -i "anoisesrc=d=0.5:c=pink:a=0.6" -af "afade=t=in:d=0.2,afade=t=out:st=0.25:d=0.25,bandpass=f=1400:w=1800" -ar 48000 -ac 2 whoosh.wav
ffmpeg -y -v error -f lavfi -i "aevalsrc='0.3*(sin(2*PI*1320*t)+0.6*sin(2*PI*1980*t)+0.4*sin(2*PI*2640*t))*exp(-t*3)':s=48000:d=1.4" -ac 2 bell.wav
ffmpeg -y -v error -f lavfi -i "aevalsrc='0.25*sin(2*PI*880*t)*exp(-t*6)+0.25*sin(2*PI*1320*(t-0.12))*exp(-(t-0.12)*6)*gt(t,0.12)':s=48000:d=0.8" -ac 2 ding.wav
ffmpeg -y -v error -f lavfi -i "aevalsrc='0.12*sin(2*PI*4200*t)*gt(sin(2*PI*28*t),0.2)*gt(sin(2*PI*3*t),0)':s=48000:d=1.2" -af "afade=t=out:st=0.9:d=0.3" -ac 2 cricket.wav
