import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, squash } from "../anim";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Check, DrawPath, Display, Hand, Heart, Sparkle, Strike } from "../components/Doodles";
import { Geet, runPose } from "../components/Geet";
import { C } from "../theme";
import { cloudPath } from "./S2Dream";

type P = [number, number];
const GS = 0.42;

/** Parabolic hop from a to b between frames t0..t1. */
const hop = (frame: number, a: P, b: P, t0: number, t1: number, h = 120): P => {
  const k = interpolate(frame, [t0, t1], [0, 1], clamp);
  return [a[0] + (b[0] - a[0]) * k, a[1] + (b[1] - a[1]) * k - Math.sin(k * Math.PI) * h];
};

const Stone: React.FC<{ x: number; y: number; label: string; s: number; color: string }> = ({ x, y, label, s, color }) => (
  <g transform={`translate(${x} ${y}) scale(${s})`}>
    <ellipse cx={0} cy={22} rx={112} ry={46} fill={C.inkSoft} stroke={C.ink} strokeWidth={6} />
    <ellipse cx={0} cy={0} rx={112} ry={44} fill={color} stroke={C.ink} strokeWidth={6} />
    <Display x={0} y={16} size={46} spacing={2}>
      {label}
    </Display>
  </g>
);

export const S4Paths: React.FC = () => {
  const frame = useCurrentFrame();
  const boil = boilOf(frame);
  const c1 = cueAt("s4", 1);
  const c2 = cueAt("s4", 2);
  const c3 = cueAt("s4", 3);

  // --- left: the giant COMMIT leap ---
  const L0: P = [175, 1180];
  const arcD = "M190 1080 Q 230 560 430 470";
  const arcP = ease(frame, 10, 36);
  const JUMP0 = 42;
  const FALL0 = 60;
  const LAND = 86;
  let lx = L0[0];
  let ly = L0[1];
  let lrot = 0;
  if (frame >= JUMP0 && frame < FALL0) {
    const k = ease(frame, JUMP0, FALL0, 0, 0.45, (t) => 1 - Math.pow(1 - t, 2));
    // point on the quadratic arc (shifted to feet)
    const x = (1 - k) * (1 - k) * 190 + 2 * (1 - k) * k * 230 + k * k * 430;
    const y = (1 - k) * (1 - k) * 1180 + 2 * (1 - k) * k * 660 + k * k * 570;
    lx = x;
    ly = y;
  } else if (frame >= FALL0) {
    const k = ease(frame, FALL0, LAND, 0, 1, (t) => t * t);
    lx = interpolate(k, [0, 1], [258, 395]);
    ly = interpolate(k, [0, 1], [799, 1420]);
    lrot = k * 200;
  }
  const leftRun = runPose(frame * 0.5);

  // --- right: stepping stones ---
  const stones: { p: P; label: string; at: number; color: string }[] = [
    { p: [660, 1170], label: "TEST", at: c1 + 4, color: C.mustard },
    { p: [860, 960], label: "LEARN", at: c1 + 16, color: C.coral },
    { p: [680, 760], label: "DECIDE", at: c1 + 28, color: C.pink },
  ];
  const start: P = [840, 1340];
  const island: P = [600, 452];
  const hops = [c1 + 34, c1 + 52, c1 + 70, c1 + 90];
  const pts: P[] = [start, ...stones.map((s) => s.p), island];
  let rp: P = start;
  let lastLand = -100;
  for (let i = 0; i < hops.length; i++) {
    if (frame >= hops[i]) {
      rp = hop(frame, pts[i], pts[i + 1], hops[i], hops[i] + 14, 110);
      if (frame >= hops[i] + 14) lastLand = hops[i] + 14;
    }
  }
  const inAir = hops.some((h) => frame >= h && frame < h + 14);
  const [rsx, rsy] = squash(frame, lastLand, 0.25);
  const arrived = frame >= hops[3] + 14;

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <Paper />
        {/* chasm + ledge */}
        <g filter="url(#boil)">
          <path d="M40 1180 H262 L280 1260 L262 1400 H40 Z" fill={C.paperDeep} stroke={C.ink} strokeWidth={7} strokeLinejoin="round" />
          <path d="M262 1182 L300 1240 L282 1300 L320 1360 L300 1400 H520 L500 1330 L530 1260 L505 1182 Z" fill={C.ink} />
          <path d="M520 1182 L505 1182 L530 1260 L500 1330 L520 1400" stroke={C.ink} strokeWidth={7} fill="none" />
          <path d="M540 610 Q550 900 536 1390" stroke={C.ink} strokeWidth={5} strokeDasharray="18 16" strokeLinecap="round" fill="none" />
          {/* future island */}
          <g transform={`translate(540 470) scale(${pop(frame, 2)}) translate(-540 -470)`}>
            <path d={cloudPath(540, 470, 210, 70, 12)} fill={C.white} stroke={C.ink} strokeWidth={7} />
            <rect x={436} y={392} width={70} height={58} fill={C.coral} stroke={C.ink} strokeWidth={5} />
            <path d="M424 396 L471 352 L518 396 Z" fill={C.mustard} stroke={C.ink} strokeWidth={5} strokeLinejoin="round" />
            <Heart x={471} y={420} size={22} shine={false} />
            <line x1={690} y1={452} x2={690} y2={340} stroke={C.ink} strokeWidth={6} />
            <path d="M692 342 L760 360 L692 380 Z" fill={C.red} stroke={C.ink} strokeWidth={4} strokeLinejoin="round" />
            <Hand x={540} y={575} size={46} anchor="middle">
              the future
            </Hand>
          </g>
        </g>

        {/* giant leap arc */}
        <g filter="url(#boil)" opacity={frame > c2 - 6 ? 0.35 : 1}>
          <mask id="arcMask">
            <path d={arcD} pathLength={1} fill="none" stroke="#fff" strokeWidth={30} strokeDasharray="1 1" strokeDashoffset={1 - arcP} />
          </mask>
          <path d={arcD} fill="none" stroke={C.coralDeep} strokeWidth={9} strokeLinecap="round" strokeDasharray="26 22" mask="url(#arcMask)" />
          <Hand x={285} y={770} size={42} anchor="middle" color={C.coralDeep} opacity={lin(frame, 22, 28)}>
            (one giant leap)
          </Hand>
        </g>

        <g filter="url(#boil)">
          {/* column tags */}
          <g transform={`translate(285 680) rotate(-5) scale(${pop(frame, 8)})`}>
            <rect x={-150} y={-52} width={300} height={90} rx={16} fill={C.coral} stroke={C.ink} strokeWidth={7} />
            <Display x={0} y={22} size={66} color={C.white} outline={C.ink} ow={9}>
              COMMIT
            </Display>
          </g>
          <g transform={`translate(800 640) rotate(4) scale(${pop(frame, c1)})`}>
            <rect x={-150} y={-46} width={300} height={80} rx={16} fill={C.mustard} stroke={C.ink} strokeWidth={7} />
            <Display x={0} y={18} size={52}>
              PROTOTYPE
            </Display>
          </g>
        </g>

        {/* left Geet */}
        <g filter="url(#boil)">
          {frame < FALL0 ? (
            <Geet
              x={lx}
              y={ly}
              scale={GS}
              expr={frame >= JUMP0 ? "happy" : "determined"}
              {...(frame >= JUMP0 ? { legL: [40, 10], legR: [-45, -90], armL: [150, 165], armR: [150, 165], flap: 1 } : { armL: leftRun.armL, armR: [14, 8] })}
            />
          ) : frame < LAND ? (
            <g transform={`rotate(${lrot} ${lx} ${ly - 100})`}>
              <Geet x={lx} y={ly} scale={GS} expr="shocked" armL={[170, 180]} armR={[170, 180]} legL={[20, 40]} legR={[-20, -40]} flap={-1} />
            </g>
          ) : null}
          {frame >= FALL0 && frame < LAND + 4 ? (
            <Display x={420} y={interpolate(frame, [FALL0, LAND], [860, 1000], clamp)} size={60} color={C.red} outline={C.white} ow={8} rotate={12}>
              AAAA!
            </Display>
          ) : null}
          {frame >= LAND ? (
            <g>
              {[0, 1, 2].map((i) => (
                <Sparkle key={i} x={350 + i * 70} y={1150 - (i % 2) * 30} size={18 * pop(frame, LAND + i * 2)} rotate={frame * 4} color={C.mustard} />
              ))}
              <Hand x={400} y={1110} size={44} anchor="middle" opacity={lin(frame, LAND + 4, LAND + 8)}>
                ow.
              </Hand>
            </g>
          ) : null}
        </g>

        {/* stepping stones + right Geet */}
        <g filter="url(#boil)">
          {stones.map((s) => (frame >= s.at ? <Stone key={s.label} x={s.p[0]} y={s.p[1]} label={s.label} s={pop(frame, s.at)} color={s.color} /> : null))}
          {frame >= c1 + 4 ? (
            <>
              <DrawPath d="M720 1120 Q 800 1060 830 1010" p={ease(frame, c1 + 10, c1 + 20)} w={5} />
              <DrawPath d="M830 910 Q 790 840 735 805" p={ease(frame, c1 + 22, c1 + 32)} w={5} />
            </>
          ) : null}
          <Geet
            x={rp[0]}
            y={rp[1] + (stones.some((s) => s.p === rp) ? -6 : 0)}
            scale={GS}
            sx={inAir ? 0.9 : rsx}
            sy={inAir ? 1.12 : rsy}
            expr={arrived ? "grin" : frame >= FALL0 + 6 && frame < hops[0] ? "worried" : "smile"}
            look={frame < hops[0] ? -1 : 0}
            {...(inAir ? { legL: [30, -20], legR: [-10, -60], armL: [120, 140], armR: [120, 140] } : arrived ? { armL: [150, 170], armR: [150, 170] } : {})}
            flap={inAir ? 0.8 : 0.1}
          />
          {arrived ? (
            <>
              <Sparkle x={500} y={300} size={26 * pop(frame, hops[3] + 16)} rotate={frame * 3} />
              <Sparkle x={720} y={290} size={20 * pop(frame, hops[3] + 19)} rotate={-frame * 3} color={C.coral} />
              <Check x={760} y={520} p={ease(frame, hops[3] + 16, hops[3] + 24)} size={56} />
            </>
          ) : null}
        </g>

        {/* kinetic text */}
        <g filter="url(#boil)">
          {frame >= c2 ? (
            <g transform={`translate(290 870) rotate(-4) scale(${pop(frame, c2)})`}>
              <Display x={0} y={0} size={56} color={C.ink} outline={C.white} ow={10}>
                LESS GUESSING
              </Display>
              <Strike x1={-190} x2={190} y={-20} p={ease(frame, c2 + 6, c2 + 14)} />
            </g>
          ) : null}
          {frame >= c3 ? (
            <g transform={`translate(290 990) rotate(3) scale(${pop(frame, c3)})`}>
              <rect x={-200} y={-60} width={400} height={80} rx={14} fill={C.mustard} stroke={C.ink} strokeWidth={6} />
              <Display x={0} y={0} size={56} color={C.ink}>
                MORE LEARNING
              </Display>
            </g>
          ) : null}
        </g>
      </Stage>
    </AbsoluteFill>
  );
};
