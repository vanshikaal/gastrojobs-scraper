import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, ease, lin, pop } from "../anim";
import { Burst, Display, Hand, Puff, Sparkle, SpeedLines } from "../components/Doodles";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Geet, runPose } from "../components/Geet";
import { C } from "../theme";

export const FREEZE_S1 = 74;
const GROUND = 1290;

const Train: React.FC<{ x: number; t: number }> = ({ x, t }) => (
  <g transform={`translate(${x} 0)`}>
    {/* steam */}
    {[0, 1, 2].map((i) => {
      const k = ((t * 0.8 + i * 22) % 66) / 66;
      return <Puff key={i} x={120 + i * 40 + k * 40} y={930 - k * 220} r={40 + k * 50} opacity={1 - k} />;
    })}
    <rect x={0} y={950} width={700} height={290} rx={36} fill={C.coral} stroke={C.ink} strokeWidth={8} />
    <rect x={0} y={950} width={700} height={56} rx={28} fill={C.coralDeep} stroke={C.ink} strokeWidth={6} />
    <rect x={0} y={1150} width={700} height={34} fill={C.mustard} stroke={C.ink} strokeWidth={6} />
    {/* open door */}
    <rect x={40} y={1030} width={110} height={190} rx={10} fill={C.inkSoft} stroke={C.ink} strokeWidth={7} />
    <line x1={22} y1={1040} x2={22} y2={1210} stroke={C.ink} strokeWidth={8} strokeLinecap="round" />
    {[200, 330, 460, 590].map((wx) => (
      <g key={wx}>
        <rect x={wx} y={1040} width={100} height={84} rx={14} fill={C.white} stroke={C.ink} strokeWidth={6} />
        <path d={`M${wx + 20} ${1060} l20 -10`} stroke={C.blueLine} strokeWidth={6} strokeLinecap="round" />
      </g>
    ))}
    {[90, 260, 480, 640].map((wx) => (
      <g key={wx} transform={`translate(${wx} 1250) rotate(${t * 9})`}>
        <circle r={36} fill={C.ink} />
        <circle r={14} fill={C.silver} />
        <line x1={-30} y1={0} x2={30} y2={0} stroke={C.silver} strokeWidth={5} />
      </g>
    ))}
  </g>
);

const Meter: React.FC<{ y: number; label: string; fill: number; color: string; frame: number; at: number; low?: boolean }> = ({
  y,
  label,
  fill,
  color,
  frame,
  at,
  low,
}) => {
  const s = pop(frame, at);
  const f = ease(frame, at + 4, at + 16, 0, fill);
  const blink = low && Math.floor(frame / 5) % 2 === 0;
  return (
    <g transform={`translate(540 ${y}) scale(${s}) translate(-540 ${-y})`}>
      <Display x={120} y={y - 20} size={42} anchor="start">
        {label}
      </Display>
      <rect x={120} y={y} width={720} height={66} rx={18} fill={C.white} stroke={C.ink} strokeWidth={7} />
      <rect x={852} y={y + 18} width={22} height={30} rx={6} fill={C.ink} />
      <rect x={130} y={y + 10} width={Math.max(0, 700 * f)} height={46} rx={10} fill={blink ? C.red : color} />
      <Display x={830} y={y + 50} size={40} anchor="end" color={low ? C.red : C.ink}>
        {`${Math.round(f * 100)}%`}
      </Display>
    </g>
  );
};

export const S1Train: React.FC = () => {
  const frame = useCurrentFrame();
  const f = Math.min(frame, FREEZE_S1);
  const frozen = frame >= FREEZE_S1;
  const boil = boilOf(frame);

  // Geet's dash toward the train, then a leap that freezes mid-air.
  const run = runPose(f * 0.52);
  const gx = interpolate(f, [0, 60, FREEZE_S1], [-170, 330, 420], { ...clamp });
  const jump = ease(f, 58, FREEZE_S1, 0, 1);
  const lift = Math.sin(jump * Math.PI * 0.62) * 70;
  const leaping = f >= 60;
  const zoom = frozen ? 1 + 0.05 * ease(frame, FREEZE_S1, FREEZE_S1 + 10) : 1;
  const flash = frozen ? interpolate(frame, [FREEZE_S1, FREEZE_S1 + 7], [0.9, 0], clamp) : 0;
  const meterOut = lin(frame, 66, 74, 1, 0);

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <g transform={`translate(540 960) scale(${zoom}) translate(-540 -960)`}>
          <Paper />
          {/* platform */}
          <rect x={-20} y={GROUND} width={1120} height={700} fill={C.paperDeep} />
          <g filter="url(#boil)">
            <path d={`M-20 ${GROUND} Q300 ${GROUND - 8} 560 ${GROUND + 2} T1100 ${GROUND - 2}`} stroke={C.ink} strokeWidth={8} fill="none" />
            {[0, 1, 2, 3, 4, 5, 6, 7].map((i) => (
              <rect key={i} x={i * 150 - 20} y={GROUND + 18} width={80} height={18} rx={4} fill={C.mustard} stroke={C.ink} strokeWidth={4} />
            ))}
            <Train x={600 + f * 1.4} t={f} />
            {/* pole + station sign */}
            <g transform="translate(250 0)">
              <rect x={-8} y={760} width={16} height={530} fill={C.inkSoft} />
              <rect x={-120} y={780} width={240} height={86} rx={12} fill={C.white} stroke={C.ink} strokeWidth={6} />
              <Display x={0} y={838} size={42}>
                PLATFORM 1
              </Display>
            </g>
          </g>
          {frozen ? (
            <g filter="url(#boil)">
              <Burst x={gx + 20} y={GROUND - lift - 300} r={330} scale={pop(frame, FREEZE_S1, 220, 12)} rot={frame * 0.4} fill={C.mustard} />
              <Burst x={gx + 20} y={GROUND - lift - 300} r={250} scale={pop(frame, FREEZE_S1 + 2, 220, 12)} rot={-frame * 0.5} fill={C.coral} spikes={12} />
            </g>
          ) : null}
          <g filter="url(#boil)">
            {!frozen && f > 6 ? <SpeedLines x={gx - 70} y={GROUND - 260 - lift} frame={frame} /> : null}
            {!leaping
              ? [0, 1, 2].map((i) => {
                  const age = (f + i * 7) % 21;
                  return <Puff key={i} x={gx - 80 - age * 6} y={GROUND - 10 - age * 2} r={22 + age} opacity={1 - age / 21} fill={C.paperDeep} />;
                })
              : null}
            <Geet
              x={gx}
              y={GROUND - lift + (leaping ? 0 : run.bob)}
              expr={frozen ? "happy" : "determined"}
              legL={leaping ? [48, 20] : run.legL}
              legR={leaping ? [-40, -95] : run.legR}
              armL={leaping ? [70, 120] : run.armL}
              armR={leaping ? [130, 115] : run.armR}
              lean={leaping ? 10 : run.lean}
              flap={leaping ? 1 : run.flap}
              look={1}
            />
            {frozen ? (
              <>
                <Sparkle x={gx - 190} y={GROUND - 520} size={28 * pop(frame, FREEZE_S1 + 4)} rotate={frame * 2} />
                <Sparkle x={gx + 210} y={GROUND - 470} size={22 * pop(frame, FREEZE_S1 + 7)} rotate={-frame * 2} color={C.white} />
              </>
            ) : null}
          </g>
        </g>

        {/* energy meters (pre-freeze) */}
        {meterOut > 0 ? (
          <g opacity={meterOut} filter="url(#boil)">
            <Meter y={330} label="MAIN CHARACTER ENERGY" fill={1} color={C.coral} frame={frame} at={8} />
            <Meter y={500} label="PROTOTYPE ENERGY" fill={0.03} color={C.coral} frame={frame} at={40} low />
            {frame > 52 ? (
              <Hand x={560} y={640} size={48} rotate={-3} opacity={lin(frame, 52, 56)}>
                (uh oh.)
              </Hand>
            ) : null}
          </g>
        ) : null}

        {/* freeze-frame title */}
        {frozen ? (
          <g filter="url(#boil)">
            <Display x={540} y={400} size={112} color={C.ink} scale={pop(frame, FREEZE_S1 + 2)} rotate={-3} spacing={2}>
              GEET NEEDED
            </Display>
            <Display x={540} y={520} size={120} color={C.coral} outline={C.ink} ow={14} scale={pop(frame, FREEZE_S1 + 7)} rotate={2}>
              LIFE
            </Display>
            <g transform={`translate(540 640) scale(${pop(frame, FREEZE_S1 + 12)}) rotate(-2)`}>
              <path d="M-420 -96 Q0 -116 420 -92 L412 12 Q0 28 -414 10 Z" fill={C.mustard} stroke={C.ink} strokeWidth={7} strokeLinejoin="round" />
              <Display x={0} y={-2} size={118} color={C.ink}>
                PROTOTYPING
              </Display>
            </g>
          </g>
        ) : null}
        {frozen ? (
          <rect x={18} y={18} width={1044} height={1884} rx={30} fill="none" stroke={C.ink} strokeWidth={14} opacity={ease(frame, FREEZE_S1, FREEZE_S1 + 4)} />
        ) : null}
        <rect width={1080} height={1920} fill={C.white} opacity={flash} />
      </Stage>
    </AbsoluteFill>
  );
};
