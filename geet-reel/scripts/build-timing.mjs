// Builds src/timing.json (scene lengths + subtitle cue timings) and subtitles.srt.
// Cue lengths are estimated by synthesising each line offline (espeak-ng / MBROLA
// at a natural conversational rate). The synthetic audio is only measured, never
// embedded. Record the real voiceover to the cue times in subtitles.srt.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const script = JSON.parse(fs.readFileSync(path.join(root, "scripts/script.json"), "utf8"));
const fps = script.fps;
const tmp = path.join(root, ".vo-tmp");
fs.mkdirSync(tmp, { recursive: true });
const say = (s) => s.replace(/\.\.\./g, ",").replace(/Geet/g, "Geeth").replace(/Anshuman/g, "Unshoo-mun");
const dur = (f) => parseFloat(execFileSync("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", f]).toString());
const measure = (text, i) => {
  const raw = path.join(tmp, `c${i}.wav`);
  const out = path.join(tmp, `c${i}.trim.wav`);
  execFileSync("espeak-ng", ["-v", "mb-us2", "-s", String(script.rate), "-g", "2", "-w", raw, say(text)]);
  execFileSync("ffmpeg", ["-y", "-v", "error", "-i", raw, "-af",
    "silenceremove=start_periods=1:start_threshold=-45dB,areverse,silenceremove=start_periods=1:start_threshold=-45dB,areverse", out]);
  return dur(out);
};

const LEAD = 0.15;
const cues = [];
const scenes = [];
let t0 = 0;
let n = 0;
for (const sc of script.scenes) {
  const pauses = sc.pauses ?? sc.cues.slice(1).map(() => 0.15);
  let t = t0 + LEAD;
  const sceneCues = sc.cues.map((text, i) => {
    const len = measure(text, n++);
    const c = { scene: sc.id, text, start: t, end: t + len };
    t += len + (pauses[i] ?? 0);
    return c;
  });
  const speechEnd = sceneCues[sceneCues.length - 1].end;
  const durS = Math.max(sc.minDur, speechEnd - t0 + sc.tail);
  const from = Math.round(t0 * fps);
  const durF = Math.round((t0 + durS) * fps) - from;
  scenes.push({ id: sc.id, from, dur: durF });
  for (const c of sceneCues) cues.push({ scene: c.scene, text: c.text, from: Math.round(c.start * fps), to: Math.round(c.end * fps) });
  console.log(`${sc.id}: ${t0.toFixed(2)}s → ${(t0 + durS).toFixed(2)}s, speech ends ${speechEnd.toFixed(2)}s`);
  t0 += durS;
}
const total = scenes.reduce((a, s) => a + s.dur, 0);
const sceneEnd = Object.fromEntries(scenes.map((s) => [s.id, s.from + s.dur]));
cues.forEach((c, i) => {
  const next = cues[i + 1];
  const limit = next && next.scene === c.scene ? next.from : sceneEnd[c.scene] - 2;
  c.showUntil = Math.min(limit, c.to + 15);
});
fs.writeFileSync(path.join(root, "src/timing.json"), JSON.stringify({ fps, durationInFrames: total, scenes, cues }, null, 2));
const ts = (f) => { const ms = Math.round((f / fps) * 1000); const p = (v, l = 2) => String(v).padStart(l, "0");
  return `${p(Math.floor(ms / 3600000))}:${p(Math.floor(ms / 60000) % 60)}:${p(Math.floor(ms / 1000) % 60)},${p(ms % 1000, 3)}`; };
fs.writeFileSync(path.join(root, "subtitles.srt"), cues.map((c, i) => `${i + 1}\n${ts(c.from)} --> ${ts(c.showUntil)}\n${c.text}\n`).join("\n"));
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`total ${total} frames = ${(total / fps).toFixed(2)} s`);
