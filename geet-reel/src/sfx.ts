import { SCENES, cueAt, cueEnd } from "./anim";

// Synthesised SFX in public/sfx (see scripts/make-sfx.sh). Frames are absolute.
const s2 = SCENES.s2.from;
const s3 = SCENES.s3.from;
const s4 = SCENES.s4.from;
const s5 = SCENES.s5.from;
const s6 = SCENES.s6.from;

export const SFX: { at: number; file: string; vol: number }[] = [
  // 1 · dash + freeze
  { at: 0, file: "whoosh", vol: 0.6 },
  { at: 8, file: "pop", vol: 0.5 },
  { at: 40, file: "pop", vol: 0.5 },
  { at: 74, file: "thump", vol: 0.8 },
  { at: 76, file: "ding", vol: 0.35 },
  // 2 · dream + APPROVED
  { at: s2 + 4, file: "pop", vol: 0.5 },
  { at: s2 + 16, file: "bell", vol: 0.45 },
  { at: s2 + 30, file: "pop", vol: 0.4 },
  { at: s2 + 52, file: "pop", vol: 0.4 },
  { at: s2 + 70, file: "pop", vol: 0.4 },
  { at: s2 + cueAt("s2", 2) + 12, file: "whoosh", vol: 0.5 },
  { at: s2 + cueAt("s2", 2) + 22, file: "thump", vol: 0.9 },
  // 3 · hypothesis card
  { at: s3, file: "whoosh", vol: 0.5 },
  { at: s3 + cueAt("s3", 2) + 2, file: "thump", vol: 1 },
  { at: s3 + cueEnd("s3", 2) + 12, file: "cricket", vol: 0.6 },
  // 4 · paths
  { at: s4 + 60, file: "whoosh", vol: 0.5 },
  { at: s4 + cueAt("s4", 1) + 4, file: "pop", vol: 0.4 },
  { at: s4 + cueAt("s4", 1) + 16, file: "pop", vol: 0.4 },
  { at: s4 + cueAt("s4", 1) + 28, file: "pop", vol: 0.4 },
  { at: s4 + cueAt("s4", 1) + 104, file: "ding", vol: 0.4 },
  // 5 · LDL universe
  { at: s5 + 24, file: "thump", vol: 0.7 },
  { at: s5 + 46, file: "whoosh", vol: 0.6 },
  { at: s5 + cueAt("s5", 1) + 26, file: "whoosh", vol: 0.4 },
  { at: s5 + cueAt("s5", 2) - 3, file: "whoosh", vol: 0.4 },
  { at: s5 + cueAt("s5", 3) - 3, file: "whoosh", vol: 0.4 },
  { at: s5 + cueAt("s5", 3) + 63, file: "thump", vol: 0.6 },
  // 6 · ending
  { at: s6 + cueAt("s6", 2) + 30, file: "whoosh", vol: 0.5 },
  { at: s6 + cueAt("s6", 2) + 40, file: "ding", vol: 0.45 },
];
