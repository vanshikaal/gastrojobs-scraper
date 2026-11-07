import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, shake } from "../anim";
import { Burst, Display, Hand, Heart, Puff, Sparkle, SpeedLines, Stamp } from "../components/Doodles";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Geet, runPose } from "../components/Geet";
import { C } from "../theme";

const FREEZE_S1 = cueAt("s1", 1) + 10;
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

/** Doodled film strip naming the movie (plain text, no film branding). */
const FilmStrip: React.FC<{ s: number }> = ({ s }) => (
  <g transform={`translate(540 330) rotate(-2) scale(${s})`}>
    <rect x={-400} y={-62} width={800} height={124} rx={10} fill={C.ink} />
    {[...Array(13)].map((_, i) => (
      <g key={i}>
        <rect x={-380 + i * 60} y={-52} width={30} height={16} rx={4} fill={C.paper} />
        <rect x={-380 + i * 60} y={36} width={30} height={16} rx={4} fill={C.paper} />
      </g>
    ))}
    <Display x={0} y={22} size={64} color={C.white} spacing={6}>
      JAB WE MET
    </Display>
  </g>
);

const Chalkboard: React.FC<{ y: number }> = ({ y }) => (
  <g transform={`translate(540 ${y}) rotate(-1.5)`}>
    <line x1={-250} y1={-260} x2={-300} y2={-170} stroke={C.ink} strokeWidth={5} />
    <line x1={250} y1={-260} x2={300} y2={-170} stroke={C.ink} strokeWidth={5} />
    <rect x={-410} y={-170} width={820} height={330} rx={20} fill="#A9683E" stroke={C.ink} strokeWidth={8} />
    <rect x={-385} y={-148} width={770} height={286} rx={10} fill="#2F4A3E" stroke={C.ink} strokeWidth={5} />
    <Display x={0} y={-62} size={62} color={C.white} spacing={4}>
      LIFE DESIGN LAB
    </Display>
    <Hand x={0} y={20} size={58} anchor="middle" color={C.mustard}>
      Lecture 1: Life Prototyping
    </Hand>
    <path d="M-160 50 Q0 66 160 46" stroke={C.white} strokeWidth={5} fill="none" strokeLinecap="round" opacity={0.7} />
    <rect x={250} y={112} width={70} height={18} rx={4} fill={C.white} stroke={C.ink} strokeWidth={3} />
  </g>
);

const Podium: React.FC<{ x: number }> = ({ x }) => (
  <g transform={`translate(${x} ${GROUND})`}>
    <path d="M-110 0 L-90 -250 H90 L110 0 Z" fill="#C98B55" stroke={C.ink} strokeWidth={7} strokeLinejoin="round" />
    <path d="M-130 -250 H130 L115 -290 H-115 Z" fill="#A9683E" stroke={C.ink} strokeWidth={7} strokeLinejoin="round" />
    <rect x={-60} y={-190} width={120} height={56} rx={8} fill={C.mustard} stroke={C.ink} strokeWidth={5} />
    <Display x={0} y={-148} size={36}>
      LDL
    </Display>
    {/* the apple for the teacher */}
    <circle cx={60} cy={-312} r={22} fill={C.red} stroke={C.ink} strokeWidth={5} />
    <path d="M60 -334 q4 -12 14 -14" stroke={C.ink} strokeWidth={4} fill="none" />
  </g>
);

export const S1Train: React.FC = () => {
  const frame = useCurrentFrame();
  const f = Math.min(frame, FREEZE_S1);
  const frozen = frame >= FREEZE_S1;
  const boil = boilOf(frame);

  // Geet's dash toward the train, then a leap that freezes mid-air.
  const run = runPose(f * 0.52);
  const gx = interpolate(f, [0, FREEZE_S1 - 14, FREEZE_S1], [-170, 330, 420], { ...clamp });
  const jump = ease(f, FREEZE_S1 - 16, FREEZE_S1, 0, 1);
  const lift = Math.sin(jump * Math.PI * 0.62) * 45;
  const leaping = f >= FREEZE_S1 - 14;
  const zoom = frozen ? 1 + 0.05 * ease(frame, FREEZE_S1, FREEZE_S1 + 10) : 1;
  const flash = frozen ? interpolate(frame, [FREEZE_S1, FREEZE_S1 + 7], [0.9, 0], clamp) : 0;
  const c2 = cueAt("s1", 2);
  const c3 = cueAt("s1", 3);
  const STAMP = c3 + 16;
  const meterOut = lin(frame, FREEZE_S1 - 6, FREEZE_S1, 1, 0);
  const lowOut = lin(frame, c3 - 2, c3 + 4, 1, 0);
  const boardY = interpolate(frame, [c3, c3 + 9, c3 + 13], [-400, 470, 450], clamp);
  const podiumX = interpolate(frame, [c3 + 4, c3 + 12], [1300, 860], { ...clamp, easing: (t) => 1 - Math.pow(1 - t, 3) });
  const sh = shake(frame, STAMP, 12, 20);

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <g transform={`translate(${sh.x} ${sh.y})`}>
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
              expr={frame >= STAMP ? "shocked" : frozen ? "happy" : "determined"}
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

        {/* movie context + main character energy (pre-freeze) */}
        {meterOut > 0 ? (
          <g opacity={meterOut} filter="url(#boil)">
            <FilmStrip s={pop(frame, 3)} />
            {frame >= 14 ? (
              <g opacity={lin(frame, 14, 18)}>
                <Hand x={520} y={455} size={50} anchor="middle" color={C.coralDeep}>
                  starring: Geet
                </Hand>
                <Heart x={690} y={440} size={34} scale={pop(frame, 18)} />
              </g>
            ) : null}
            <Meter y={560} label="MAIN CHARACTER ENERGY" fill={1} color={C.coral} frame={frame} at={30} />
          </g>
        ) : null}

        {/* "girl desperately needed..." */}
        {frame >= c2 && lowOut > 0 ? (
          <g opacity={lowOut} filter="url(#boil)">
            <Meter y={360} label="PROTOTYPE ENERGY" fill={0.03} color={C.coral} frame={frame} at={c2} low />
            <Hand x={560} y={510} size={50} rotate={-3} opacity={lin(frame, c2 + 12, c2 + 16)}>
              (uh oh.)
            </Hand>
          </g>
        ) : null}

        {/* "...a Life Design Lab lecture." */}
        {frame >= c3 ? (
          <g filter="url(#boil)">
            <Chalkboard y={boardY} />
            <Podium x={podiumX} />
            <Stamp frame={frame} at={STAMP} x={700} y={640} text="ATTENDANCE" sub="COMPULSORY" rotate={-9} size={64} color={C.red} />
          </g>
        ) : null}
        </g>
        {frozen ? (
          <rect x={18} y={18} width={1044} height={1884} rx={30} fill="none" stroke={C.ink} strokeWidth={14} opacity={ease(frame, FREEZE_S1, FREEZE_S1 + 4)} />
        ) : null}
        <rect width={1080} height={1920} fill={C.white} opacity={flash} />
      </Stage>
    </AbsoluteFill>
  );
};
