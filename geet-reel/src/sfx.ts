import { SCENES, cueAt, cueEnd } from "./anim";
import { S2_IMPACT } from "./scenes/S2Dream";
import { S3_SLAM } from "./scenes/S3Hypothesis";
import { S4_BEATS } from "./scenes/S4Paths";
import { S5_BEATS } from "./scenes/S5LDL";
import { S6_BEATS } from "./scenes/S6Ending";

// Synthesised SFX in public/sfx (see scripts/make-sfx.sh). Frames are absolute.
const s1 = SCENES.s1.from;
const s2 = SCENES.s2.from;
const s3 = SCENES.s3.from;
const s4 = SCENES.s4.from;
const s5 = SCENES.s5.from;
const s6 = SCENES.s6.from;
const F1 = cueAt("s1", 1) + 10;

export const SFX: { at: number; file: string; vol: number }[] = [
  // 1 · Jab We Met dash → freeze → lecture
  { at: s1, file: "whoosh", vol: 0.6 },
  { at: s1 + 3, file: "pop", vol: 0.45 },
  { at: s1 + 30, file: "pop", vol: 0.5 },
  { at: s1 + F1, file: "thump", vol: 0.8 },
  { at: s1 + F1 + 2, file: "ding", vol: 0.35 },
  { at: s1 + cueAt("s1", 2), file: "pop", vol: 0.5 },
  { at: s1 + cueAt("s1", 3), file: "whoosh", vol: 0.5 },
  { at: s1 + cueAt("s1", 3) + 16, file: "thump", vol: 0.9 },
  { at: s1 + cueAt("s1", 3) + 18, file: "bell", vol: 0.3 },
  // 2 · dream + APPROVED
  { at: s2 + 2, file: "pop", vol: 0.5 },
  { at: s2 + 10, file: "bell", vol: 0.45 },
  { at: s2 + 20, file: "pop", vol: 0.4 },
  { at: s2 + 34, file: "pop", vol: 0.4 },
  { at: s2 + S2_IMPACT() - 10, file: "whoosh", vol: 0.5 },
  { at: s2 + S2_IMPACT(), file: "thump", vol: 0.9 },
  // 3 · hypothesis card
  { at: s3, file: "whoosh", vol: 0.5 },
  { at: s3 + S3_SLAM(), file: "thump", vol: 1 },
  { at: s3 + cueEnd("s3", 2) + 4, file: "cricket", vol: 0.6 },
  // 4 · leap vs stepping stones
  ...S4_BEATS().map((b: { at: number; file: string; vol: number }) => ({ at: s4 + b.at, file: b.file, vol: b.vol })),
  // 5 · LDL Geet
  ...S5_BEATS().map((b: { at: number; file: string; vol: number }) => ({ at: s5 + b.at, file: b.file, vol: b.vol })),
  // 6 · plot twist, interrupted
  ...S6_BEATS().map((b: { at: number; file: string; vol: number }) => ({ at: s6 + b.at, file: b.file, vol: b.vol })),
];
