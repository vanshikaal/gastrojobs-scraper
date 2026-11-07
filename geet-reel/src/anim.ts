import { Easing, interpolate, spring } from "remotion";
import timing from "./timing.json";
import { FPS } from "./theme";

export const clamp = { extrapolateLeft: "clamp", extrapolateRight: "clamp" } as const;

/** Bouncy 0→1 (overshoots) starting at `at`. */
export const pop = (frame: number, at: number, stiff = 180, damping = 11) =>
  spring({ frame: frame - at, fps: FPS, config: { stiffness: stiff, damping, mass: 0.6 } });

/** Smooth 0→1 between frames a and b. */
export const ease = (frame: number, a: number, b: number, from = 0, to = 1, e = Easing.inOut(Easing.cubic)) =>
  interpolate(frame, [a, b], [from, to], { ...clamp, easing: e });

export const lin = (frame: number, a: number, b: number, from = 0, to = 1) =>
  interpolate(frame, [a, b], [from, to], clamp);

/** Camera shake offset that decays after an impact at `at`. */
export const shake = (frame: number, at: number, dur = 12, amp = 22) => {
  const t = frame - at;
  if (t < 0 || t > dur) return { x: 0, y: 0 };
  const k = (1 - t / dur) * amp;
  return { x: Math.sin(t * 2.9) * k, y: Math.cos(t * 3.7) * k * 0.8 };
};

/** Squash-and-stretch pulse after an impact: returns [sx, sy]. */
export const squash = (frame: number, at: number, amt = 0.22): [number, number] => {
  const t = frame - at;
  if (t < 0) return [1, 1];
  const s = Math.exp(-t / 5) * Math.cos(t / 2.2) * amt;
  return [1 + s, 1 - s];
};

export const SCENES = {
  s1: { from: 0, dur: 120 },
  s2: { from: 120, dur: 210 },
  s3: { from: 330, dur: 180 },
  s4: { from: 510, dur: 240 },
  s5: { from: 750, dur: 330 },
  s6: { from: 1080, dur: 210 },
} as const;
export type SceneId = keyof typeof SCENES;

export type Cue = { scene: string; text: string; from: number; to: number; showUntil: number };
export const CUES = timing.cues as Cue[];

/** Local (scene-relative) start frame of the n-th VO cue of a scene. */
export const cueAt = (scene: SceneId, n: number) => {
  const list = CUES.filter((c) => c.scene === scene);
  return list[n].from - SCENES[scene].from;
};
export const cueEnd = (scene: SceneId, n: number) => {
  const list = CUES.filter((c) => c.scene === scene);
  return list[n].to - SCENES[scene].from;
};
