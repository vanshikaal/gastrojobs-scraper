import React from "react";
import { Composition } from "remotion";
import { GeetReel, GeetReelProps } from "./GeetReel";
import { FPS, H, W } from "./theme";

export const RemotionRoot: React.FC = () => (
  <>
    <Composition
      id="GeetReel"
      component={GeetReel}
      durationInFrames={43 * FPS}
      fps={FPS}
      width={W}
      height={H}
      defaultProps={{ voiceover: "scratch", sfx: true, subtitles: true }}
    />
  </>
);
