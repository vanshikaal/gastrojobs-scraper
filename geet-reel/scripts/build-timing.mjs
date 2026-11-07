// Builds src/timing.json (subtitle cue timings) and, with --scratch-vo,
// public/vo-scratch.wav: an offline MBROLA scratch voiceover for timing review.
// Real VO: record each cue to match src/timing.json, or replace public/vo-final.wav.
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const script = JSON.parse(fs.readFileSync(path.join(root, "scripts/script.json"), "utf8"));
const fps = script.fps;
const tmp = path.join(root, ".vo-tmp");
fs.mkdirSync(tmp, { recursive: true });
const say = (s) => s.replace(/\bLDL\b/g, "L. D. L.").replace(/Geet/g, "Geeth").replace(/Anshuman/g, "Unshoo-mun");

const dur = (f) => parseFloat(execFileSync("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", f]).toString());
const synth = (text, rate, file) => {
  execFileSync("espeak-ng", ["-v", "mb-us2", "-s", String(rate), "-g", "2", "-w", file, say(text)]);
  // trim leading/trailing silence
  const out = file.replace(".wav", ".trim.wav");
  execFileSync("ffmpeg", ["-y", "-v", "error", "-i", file, "-af",
    "silenceremove=start_periods=1:start_threshold=-45dB,areverse,silenceremove=start_periods=1:start_threshold=-45dB,areverse,aresample=48000",
    out]);
  return out;
};

const cues = [];
const clips = [];
for (const sc of script.scenes) {
  const lead = 0.2, tail = 0.25;
  const pauses = sc.pauses ?? sc.cues.slice(1).map(() => 0.22);
  let rate = 165, files, lens;
  for (;;) {
    files = sc.cues.map((c, i) => synth(c, rate, path.join(tmp, `${sc.id}_${i}.wav`)));
    lens = files.map(dur);
    const total = lead + lens.reduce((a, b) => a + b, 0) + pauses.reduce((a, b) => a + b, 0) + tail;
    if (total <= sc.end - sc.start || rate > 230) break;
    rate += 8;
  }
  let t = sc.start + lead;
  sc.cues.forEach((text, i) => {
    const from = Math.round(t * fps);
    const to = Math.round((t + lens[i]) * fps);
    cues.push({ scene: sc.id, text, from, to });
    clips.push({ file: files[i], at: t });
    t += lens[i] + (pauses[i] ?? 0);
  });
  console.log(`${sc.id}: rate ${rate}, speech ends at ${t.toFixed(2)}s (scene ends ${sc.end}s)`);
}

// Subtitles stay on screen until the next cue (or scene end) so they never flash.
const sceneEnd = Object.fromEntries(script.scenes.map((s) => [s.id, s.end * fps]));
cues.forEach((c, i) => {
  const next = cues[i + 1];
  const limit = next && next.scene === c.scene ? next.from : sceneEnd[c.scene] - 2;
  c.showUntil = Math.min(limit, c.to + 18);
});
fs.writeFileSync(path.join(root, "src/timing.json"), JSON.stringify({ fps, durationInFrames: 43 * fps, cues }, null, 2));

// SRT for platforms / editors
const ts = (f) => { const ms = Math.round((f / fps) * 1000); const p = (n, l = 2) => String(n).padStart(l, "0");
  return `${p(Math.floor(ms / 3600000))}:${p(Math.floor(ms / 60000) % 60)}:${p(Math.floor(ms / 1000) % 60)},${p(ms % 1000, 3)}`; };
fs.writeFileSync(path.join(root, "subtitles.srt"), cues.map((c, i) => `${i + 1}\n${ts(c.from)} --> ${ts(c.showUntil)}\n${c.text}\n`).join("\n"));

if (process.argv.includes("--scratch-vo")) {
  const args = ["-y", "-v", "error"];
  clips.forEach((c) => args.push("-i", c.file));
  const chains = clips.map((c, i) => `[${i}]adelay=${Math.round(c.at * 1000)}|${Math.round(c.at * 1000)}[a${i}]`);
  const mix = clips.map((_, i) => `[a${i}]`).join("") + `amix=inputs=${clips.length}:normalize=0,apad=whole_dur=43,atrim=0:43,highpass=f=90,acompressor,loudnorm=I=-16[out]`;
  args.push("-filter_complex", [...chains, mix].join(";"), "-map", "[out]", "-ar", "48000", "-ac", "2", path.join(root, "public/vo-scratch.wav"));
  execFileSync("ffmpeg", args);
  console.log("wrote public/vo-scratch.wav", dur(path.join(root, "public/vo-scratch.wav")));
}
fs.rmSync(tmp, { recursive: true, force: true });
