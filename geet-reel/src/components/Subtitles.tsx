import React from "react";
import { useCurrentFrame } from "remotion";
import { CUES, pop } from "../anim";
import { C, FONT_DISPLAY } from "../theme";

// Words that get a mustard highlight in the captions.
const KEY = new Set(["prototype", "confirmed", "hypothesis,", "plan.", "prototyping", "test", "learning.", "heart.", "LDL", "massive", "smaller,", "visit", "APPROVED"]);

/** Burned-in English subtitles, synced to src/timing.json, inside the mobile-safe zone. */
export const Subtitles: React.FC = () => {
  const frame = useCurrentFrame();
  const cue = CUES.find((c) => frame >= c.from - 1 && frame < c.showUntil);
  if (!cue) return null;
  const t = frame - (cue.from - 1);
  const s = 0.86 + 0.14 * pop(frame, cue.from - 1, 260, 14);
  const out = Math.min(1, (cue.showUntil - frame) / 3);
  return (
    <div
      style={{
        position: "absolute",
        left: 0,
        right: 0,
        top: 1395,
        display: "flex",
        justifyContent: "center",
        pointerEvents: "none",
      }}
    >
      <div
        style={{
          maxWidth: 820,
          padding: "14px 30px 10px",
          background: C.white,
          border: `5px solid ${C.ink}`,
          borderRadius: 26,
          boxShadow: `7px 8px 0 ${C.ink}`,
          transform: `scale(${s}) rotate(${-0.8}deg)`,
          opacity: Math.min(1, t / 2) * out,
          fontFamily: FONT_DISPLAY,
          fontWeight: 700,
          fontSize: 50,
          lineHeight: 1.12,
          color: C.ink,
          textAlign: "center",
        }}
      >
        {cue.text.split(" ").map((w, i) => (
          <span
            key={i}
            style={{
              background: KEY.has(w) ? `linear-gradient(transparent 55%, ${C.mustard} 55%, ${C.mustard} 92%, transparent 92%)` : undefined,
            }}
          >
            {w}
            {" "}
          </span>
        ))}
      </div>
    </div>
  );
};
