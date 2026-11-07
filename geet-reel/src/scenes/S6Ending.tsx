import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, cueEnd, ease, lin, pop } from "../anim";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Bubble, Burst, Display, Hand, Heart, Sparkle, Underline } from "../components/Doodles";
import { Geet, POSES } from "../components/Geet";
import { C } from "../theme";
import { MiniClipboard } from "./S5LDL";

const beats = () => {
  const c1 = cueAt("s6", 1);
  const c2 = cueAt("s6", 2);
  return { c1, c2, DUN: [c1 + 4, c1 + 16, c1 + 28], INTERRUPT: c2 - 6, END: c2 + 32 };
};

/** Zig-zag lightning bolt. */
const Bolt: React.FC<{ x: number; y: number; s: number; rot?: number }> = ({ x, y, s, rot = 0 }) => (
  <path
    transform={`translate(${x} ${y}) rotate(${rot}) scale(${s})`}
    d="M0 -70 L-30 6 L-4 6 L-22 70 L32 -12 L6 -12 L22 -70 Z"
    fill={C.mustard}
    stroke={C.ink}
    strokeWidth={6}
    strokeLinejoin="round"
  />
);

export const S6Ending: React.FC = () => {
  const frame = useCurrentFrame();
  const { c1, c2, DUN, INTERRUPT, END } = beats();
  const drama = frame < INTERRUPT;
  // after the interruption the drama freezes (no more boil on it) and fades away
  const boil = boilOf(frame);
  const dramaFade = lin(frame, INTERRUPT, INTERRUPT + 8, 1, 0);

  // "DUN. DUN. DUNNN." — snap-zooms on Geet's face
  const zSteps = DUN.filter((d) => frame >= d).length;
  const zTarget = [1, 1.22, 1.45, 1.75][zSteps];
  const lastDun = zSteps ? DUN[zSteps - 1] : 0;
  const zPrev = [1, 1.22, 1.45, 1.75][Math.max(0, zSteps - 1)];
  const zoomDrama = zSteps ? interpolate(frame, [lastDun, lastDun + 3], [zPrev, zTarget], clamp) : 1;
  const zoom = drama ? zoomDrama : interpolate(frame, [INTERRUPT, INTERRUPT + 6], [1.75, 1], clamp);

  // after the interruption Geet shuffles left, LDL Geet strolls in from the right
  const gx = interpolate(ease(frame, INTERRUPT + 4, INTERRUPT + 16), [0, 1], [540, 300]);
  const lx = interpolate(ease(frame, INTERRUPT + 2, INTERRUPT + 22, 0, 1, (t) => 1 - Math.pow(1 - t, 2)), [0, 1], [1350, 780]);
  const walk = Math.sin(frame * 0.5);
  const wind = Math.sin(frame / 2.2);

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <Paper pattern="dots" />
        {/* over-the-top Bollywood drama backdrop */}
        {dramaFade > 0 ? (
          <g opacity={dramaFade}>
            <rect width={1080} height={1920} fill={C.red} opacity={0.9} />
            <g filter="url(#boil)">
              <Burst x={540} y={880} r={1100} spikes={22} fill={C.coral} rot={frame * 1.5} />
              <Burst x={540} y={880} r={620} spikes={18} fill={C.mustard} rot={-frame * 2} />
            </g>
            {[...Array(9)].map((_, i) => {
              const k = ((frame * 1.3 + i * 37) % 120) / 120;
              return <Heart key={i} x={(i * 131 + 60) % 1040 + Math.sin(k * 7 + i) * 30} y={200 + k * 1250} size={30} rotate={k * 300 + i * 40} fill={C.pink} shine={false} />;
            })}
          </g>
        ) : null}
        <rect x={-20} y={1290} width={1120} height={700} fill={C.paperDeep} />
        <path d="M-20 1290 Q540 1280 1100 1294" stroke={C.ink} strokeWidth={7} fill="none" filter="url(#boil)" />

        <g transform={`translate(540 860) scale(${zoom}) translate(-540 -860)`}>
          <g filter="url(#boil)">
            <Geet
              x={gx}
              y={1285}
              scale={0.95}
              expr={drama ? (frame >= c1 ? "shocked" : "worried") : frame < INTERRUPT + 14 ? "shocked" : "smile"}
              blink={!drama && frame >= INTERRUPT + 14 && frame < INTERRUPT + 18}
              {...(drama ? (frame >= c1 ? POSES.cheeks : { armL: [150, 165] as [number, number], armR: [150, 165] as [number, number] }) : POSES.idle)}
              flap={drama ? wind : Math.sin(frame / 8) * 0.25}
              headTilt={drama ? wind * 3 : 0}
              look={drama ? 0 : 1}
            />
            {!drama ? (
              <Geet
                x={lx}
                y={1285 + (lx > 790 ? -Math.abs(walk) * 8 : 0)}
                scale={0.95}
                glasses
                expr="smile"
                legL={lx > 790 ? [walk * 18, walk * 18] : [-4, -4]}
                legR={lx > 790 ? [-walk * 18, -walk * 18] : [4, 4]}
                armL={POSES.hips.armL}
                armR={[35, 160]}
                handR={<MiniClipboard s={0.55} rot={10} />}
                flap={Math.sin(frame / 8 + 1) * 0.2}
              />
            ) : null}
          </g>
        </g>

        {/* drama captions */}
        {drama ? (
          <g filter="url(#boil)">
            <Hand x={540} y={420} size={58} anchor="middle" color={C.ink} opacity={lin(frame, 10, 14)}>
              *dramatic violins*
            </Hand>
            <Bolt x={170} y={560} s={pop(frame, 8)} rot={-12} />
            <Bolt x={910} y={520} s={pop(frame, 12)} rot={14} />
            {DUN.map((d, i) =>
              frame >= d ? (
                <Display key={i} x={240 + i * 300} y={560 + (i % 2) * 40} size={70} color={C.white} outline={C.ink} ow={10} rotate={-10 + i * 10} scale={pop(frame, d, 260, 10)}>
                  {i < 2 ? "DUN." : "DUNNN."}
                </Display>
              ) : null,
            )}
            {frame >= c1 + 34 ? (
              <g transform={`translate(540 330) rotate(-4) scale(${pop(frame, c1 + 34)})`}>
                <Display x={0} y={0} size={104} color={C.mustard} outline={C.ink} ow={14} spacing={2}>
                  PLOT TWIST!!
                </Display>
              </g>
            ) : null}
          </g>
        ) : null}

        {/* record-scratch interruption */}
        {!drama && frame < END ? (
          <g filter="url(#boil)">
            <Hand x={540} y={420} size={56} anchor="middle" rotate={-3} opacity={lin(frame, INTERRUPT, INTERRUPT + 3) * lin(frame, END - 6, END, 1, 0)}>
              *record scratch*
            </Hand>
            {frame >= INTERRUPT + 22 ? (
              <Bubble x={560} y={600} w={420} h={110} tx={760} ty={760} s={pop(frame, INTERRUPT + 22) * lin(frame, END - 4, END, 1, 0)}>
                <Hand x={770} y={670} size={48} anchor="middle">
                  small test first?
                </Hand>
              </Bubble>
            ) : null}
          </g>
        ) : null}

        {/* end card */}
        {frame >= END ? (
          <g filter="url(#boil)">
            <g transform={`translate(540 ${interpolate(ease(frame, END, END + 10, 0, 1, (t) => 1 - Math.pow(1 - t, 3)), [0, 1], [-500, 470])}) rotate(-2)`}>
              <rect x={-410} y={-200} width={820} height={380} rx={36} fill={C.white} stroke={C.ink} strokeWidth={9} />
              <rect x={-410} y={-200} width={820} height={380} rx={36} fill="none" stroke={C.mustard} strokeWidth={6} transform="translate(10 12)" opacity={0.9} />
              <Display x={0} y={-50} size={104} color={C.ink} spacing={2}>
                TEST. LEARN.
              </Display>
              <Display x={0} y={86} size={104} color={C.coral} outline={C.ink} ow={12} spacing={2}>
                THEN DECIDE.
              </Display>
              <Underline x1={-310} x2={310} y={118} p={ease(frame, END + 12, END + 24)} color={C.mustard} w={11} />
            </g>
            <Sparkle x={130} y={300} size={30 * pop(frame, END + 12)} rotate={frame * 2} />
            <Sparkle x={950} y={640} size={26 * pop(frame, END + 15)} rotate={-frame * 2} color={C.coral} />
          </g>
        ) : null}
      </Stage>
    </AbsoluteFill>
  );
};

/** Sound cues for this scene (scene-relative frames). */
export const S6_BEATS = () => {
  const { c1, DUN, INTERRUPT, END } = beats();
  return [
    { at: 0, file: "whoosh", vol: 0.5 },
    ...DUN.map((d) => ({ at: d, file: "thump", vol: 0.75 })),
    { at: c1 + 34, file: "bell", vol: 0.4 },
    { at: INTERRUPT, file: "whoosh", vol: 0.7 },
    { at: INTERRUPT + 22, file: "pop", vol: 0.4 },
    { at: END, file: "whoosh", vol: 0.5 },
    { at: END + 8, file: "ding", vol: 0.45 },
  ];
};

export const _u = [cueEnd, clamp];
