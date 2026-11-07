import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, cueEnd, ease, lin, pop, shake } from "../anim";
import { ANSH_POSES, Anshuman } from "../components/Anshuman";
import { Paper, Stage, boilOf } from "../components/Defs";
import { DrawPath, Display, Hand, QMark, Stamp, Tape } from "../components/Doodles";
import { Geet, POSES } from "../components/Geet";
import { C, FONT_DISPLAY } from "../theme";

const Field: React.FC<{ y: number; label: string; value: string; frame: number; at: number; color?: string }> = ({ y, label, value, frame, at, color = C.ink }) => {
  if (frame < at) return null;
  const o = lin(frame, at, at + 5);
  const dx = interpolate(frame, [at, at + 7], [30, 0], clamp);
  return (
    <g opacity={o} transform={`translate(${dx} 0)`}>
      <text x={560} y={y} fontFamily={FONT_DISPLAY} fontWeight={800} fontSize={30} letterSpacing={3} fill={C.coralDeep}>
        {label}
      </text>
      <Hand x={560} y={y + 58} size={60} color={color}>
        {value}
      </Hand>
    </g>
  );
};

export const S3Hypothesis: React.FC = () => {
  const frame = useCurrentFrame();
  const c1 = cueAt("s3", 1);
  const c2 = cueAt("s3", 2);
  const SLAM = c2 + 2;
  const FREEZE = cueEnd("s3", 2) + 3;
  const frozen = frame >= FREEZE;
  const f = Math.min(frame, FREEZE);
  const boil = boilOf(f);
  const sh = shake(frame, SLAM, 14, 30);
  const cardY = interpolate(frame, [0, 12], [1100, 0], { ...clamp, easing: (t) => 1 - Math.pow(1 - t, 3) });
  const cardRot = interpolate(frame, [0, 12], [8, -1.5], clamp);
  const peek = ease(frame, FREEZE + 4, FREEZE + 12, 260, 0);

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <g transform={`translate(${sh.x} ${sh.y})`}>
          <Paper pattern="graph" />
          <g transform={`translate(0 ${cardY}) rotate(${cardRot} 540 700)`}>
            <g filter="url(#boil)">
              <rect x={118} y={298} width={860} height={820} rx={18} fill={C.ink} opacity={0.18} transform="translate(12 14)" />
              <rect x={110} y={290} width={860} height={820} rx={18} fill={C.white} stroke={C.ink} strokeWidth={7} />
              {[0, 1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
                <line key={i} x1={114} x2={966} y1={470 + i * 72} y2={470 + i * 72} stroke={C.blueLine} strokeWidth={3} />
              ))}
              <line x1={190} x2={190} y1={294} y2={1106} stroke={C.coral} strokeWidth={4} />
              <Tape x={200} y={300} rot={-14} />
              <Tape x={880} y={300} rot={12} color={C.mustard} />
              <Display x={540} y={400} size={70} spacing={4}>
                HYPOTHESIS #01
              </Display>
              {frame >= c1 ? (
                <DrawPath d="M250 330 C 120 360 160 450 540 440 C 930 432 940 340 820 320 C 700 300 420 300 300 330" p={ease(frame, c1 + 2, c1 + 16)} w={7} color={C.coral} />
              ) : null}
              <Anshuman
                x={330}
                y={1070}
                scale={0.95}
                expr={frame >= SLAM ? "sweat" : "confused"}
                {...(frame >= 18 ? ANSH_POSES.scratch : ANSH_POSES.idle)}
                headTilt={frame >= 18 ? 6 + Math.sin(f / 5) * 2 : 0}
                look={frame >= SLAM ? 1 : -0.4}
              />
              <QMark frame={f} at={24} x={190} y={600} size={96} color={C.mustard} rot={-12} />
              <QMark frame={f} at={30} x={470} y={560} size={70} color={C.coral} rot={14} />
              <Field y={512} label="SUBJECT" value="Anshuman" frame={f} at={14} />
              <Field y={656} label="CLAIM" value="my entire future" frame={f} at={24} />
              <Field y={800} label="EVIDENCE" value="1 train ride" frame={f} at={34} />
              <Field y={944} label="STATUS" value={frame >= SLAM ? "uh..." : "obviously true"} frame={f} at={44} color={frame >= SLAM ? C.red : C.ink} />
            </g>
          </g>
          <Stamp frame={frame} at={SLAM} x={545} y={760} text="NOT VERIFIED" sub="INSUFFICIENT DATA" rotate={-13} size={98} color={C.red} />
        </g>

        {/* comedic freeze: Geet pops up, horrified */}
        {frozen ? (
          <g>
            <clipPath id="peekClip">
              <rect x={0} y={0} width={1080} height={1384} />
            </clipPath>
            <g clipPath="url(#peekClip)">
              <Geet x={830} y={1600 + peek} scale={0.85} expr="shocked" {...POSES.cheeks} />
            </g>
            <path d="M560 1384 H1080" stroke={C.ink} strokeWidth={6} />
            <Hand x={140} y={1300} size={50} rotate={-4} opacity={lin(frame, FREEZE + 10, FREEZE + 14)}>
              *cricket noises*
            </Hand>
            <g opacity={lin(frame, FREEZE, FREEZE + 2)} transform="translate(130 1140)">
              <rect x={0} y={0} width={190} height={64} rx={14} fill={C.ink} />
              <rect x={22} y={16} width={10} height={32} fill={C.white} />
              <rect x={40} y={16} width={10} height={32} fill={C.white} />
              <text x={64} y={45} fontFamily={FONT_DISPLAY} fontWeight={800} fontSize={30} fill={C.white}>
                PAUSED
              </text>
            </g>
          </g>
        ) : null}
      </Stage>
    </AbsoluteFill>
  );
};
