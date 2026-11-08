# Geet Needed Life Prototyping — 44 s vertical reel

An animated Instagram reel built in [Remotion](https://remotion.dev) (React).
It runs 1080×1920 (9:16) at 30 fps: 1320 frames, 44.00 s.

Scene lengths are **generated** from the voiceover script, so the edit, the subtitles and the cue-driven animation beats always stay in sync.

## Storyboard → code

| Time | Scene file | What happens |
|---|---|---|
| 0.0–7.5 s | `src/scenes/S1Train.tsx` | A JAB WE MET film strip, "starring: Geet", a MAIN CHARACTER ENERGY meter. Geet dashes for the train, then a freeze frame and PROTOTYPE ENERGY 3%. A LIFE DESIGN LAB chalkboard and podium drop in, with an ATTENDANCE COMPULSORY stamp |
| 7.5–13.1 s | `src/scenes/S2Dream.tsx` | Thought bubble: an "elope!!" train, a mandap with "her boyfriend", bells, a house. APPROVED stamp, plus the "approved by: Geet" sticky note |
| 13.1–17.8 s | `src/scenes/S3Hypothesis.tsx` | HYPOTHESIS #01 card, then NOT VERIFIED. A punchline beat: PAUSED, *cricket noises*, Geet's horrified peek |
| 17.8–26.7 s | `src/scenes/S4Paths.tsx` | LIFE PROTOTYPING. COMMIT (one giant leap, she falls) vs SMALL STEPS: TEST → LEARN → REASSESS. Sticky note: "learn first, then leap." |
| 26.7–35.8 s | `src/scenes/S5LDL.tsx` | LDL universe. ① PROTOTYPE CONVERSATION (an honest talk; his answer surprises her). ② PROTOTYPE EXPERIENCE (a weekend together, no wedding). MY PLAN: I assumed → I learned → plan v2, stamped REASSESSED, "(no crystal ball. just better info.)" |
| 35.8–44.0 s | `src/scenes/S6Ending.tsx` | An over-the-top plot twist (DUN. DUN. DUNNN., PLOT TWIST!!), then a record scratch as LDL Geet strolls in with a tiny checklist: "small test first?" End card: TEST. LEARN. THEN DECIDE. |

Shared pieces:
- `src/components/Geet.tsx` and `Anshuman.tsx`: rigged vector characters.
- `src/components/Doodles.tsx`: arrows, stamps, bubbles and so on.
- `src/components/Defs.tsx`: paper texture and the "line boil" filter.
- `src/components/Subtitles.tsx`: burned-in captions in the mobile-safe zone.

## Voiceover, subtitles, timing

The narration lives in `scripts/script.json`. `node scripts/build-timing.mjs` estimates each line's length at a relaxed ~168 wpm with generous pauses, then writes two files:
- `src/timing.json`: scene lengths and cue frames
- `subtitles.srt`

The estimate comes from an offline speech engine whose audio is only measured, never embedded. **The render contains no narration**, only the synthesised SFX (whoosh, pop, stamp thump, bell, ding, crickets) from `public/sfx`.

To add a recorded voiceover:
1. Read each line at the start time given in `subtitles.srt`.
2. Save it as `public/vo-final.wav`.
3. Render with `--props='{"voiceover":"final","sfx":true,"subtitles":true}'`.

If your read runs longer or shorter, edit `scripts/script.json` (`rate`, `pauses`, `minDur`, `tail`) and rerun the script. Everything re-times automatically.

## Commands

```bash
npm install
npm run studio          # live preview / editing
node scripts/build-timing.mjs     # after changing the script
npx remotion render GeetReel out/geet-needed-life-prototyping.mp4 --crf=18
```

In a sandbox without Remotion's own Chrome download, add
`--browser-executable=/path/to/chrome-headless-shell --timeout=180000`.
