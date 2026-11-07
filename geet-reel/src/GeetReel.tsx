import "./fonts";
import React from "react";
import { AbsoluteFill, Audio, Sequence, staticFile } from "remotion";
import { SCENES } from "./anim";
import { Subtitles } from "./components/Subtitles";
import { S1Train } from "./scenes/S1Train";
import { S2Dream } from "./scenes/S2Dream";
import { S3Hypothesis } from "./scenes/S3Hypothesis";
import { S4Paths } from "./scenes/S4Paths";
import { S5LDL } from "./scenes/S5LDL";
import { S6Ending } from "./scenes/S6Ending";
import { SFX } from "./sfx";
import { C } from "./theme";

export type GeetReelProps = Record<string, unknown> & {
  /**
   * "scratch": offline robotic guide voice (public/vo-scratch.wav) for timing review.
   * "final":   your recorded / AI voiceover, saved as public/vo-final.wav (43 s, cues per src/timing.json).
   * "none":    no narration (subtitles + SFX only).
   */
  voiceover: "scratch" | "final" | "none";
  sfx: boolean;
  subtitles: boolean;
};

export const GeetReel: React.FC<GeetReelProps> = ({ voiceover, sfx, subtitles }) => (
  <AbsoluteFill style={{ backgroundColor: C.paper }}>
    <Sequence from={SCENES.s1.from} durationInFrames={SCENES.s1.dur} name="1 · Train dash + title">
      <S1Train />
    </Sequence>
    <Sequence from={SCENES.s2.from} durationInFrames={SCENES.s2.dur} name="2 · Dream + APPROVED">
      <S2Dream />
    </Sequence>
    <Sequence from={SCENES.s3.from} durationInFrames={SCENES.s3.dur} name="3 · Hypothesis card">
      <S3Hypothesis />
    </Sequence>
    <Sequence from={SCENES.s4.from} durationInFrames={SCENES.s4.dur} name="4 · Commit vs Test path">
      <S4Paths />
    </Sequence>
    <Sequence from={SCENES.s5.from} durationInFrames={SCENES.s5.dur} name="5 · LDL Geet universe">
      <S5LDL />
    </Sequence>
    <Sequence from={SCENES.s6.from} durationInFrames={SCENES.s6.dur} name="6 · Both Geets + end card">
      <S6Ending />
    </Sequence>
    {subtitles ? <Subtitles /> : null}
    {voiceover === "scratch" ? <Audio src={staticFile("vo-scratch.wav")} /> : null}
    {voiceover === "final" ? <Audio src={staticFile("vo-final.wav")} /> : null}
    {sfx
      ? SFX.map((s, i) => (
          <Sequence key={i} from={s.at} durationInFrames={60} name={`sfx ${s.file}`}>
            <Audio src={staticFile(`sfx/${s.file}.wav`)} volume={s.vol * (voiceover === "none" ? 1 : 0.7)} />
          </Sequence>
        ))
      : null}
  </AbsoluteFill>
);
