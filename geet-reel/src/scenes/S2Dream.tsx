import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, shake } from "../anim";
import { Anshuman } from "../components/Anshuman";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Display, Hand, Heart, Sparkle, Stamp } from "../components/Doodles";
import { Geet, POSES } from "../components/Geet";
import { C } from "../theme";

/** Scalloped cloud outline around an ellipse. */
export const cloudPath = (cx: number, cy: number, rx: number, ry: number, n = 14) => {
  let d = "";
  for (let i = 0; i <= n; i++) {
    const a = (i / n) * Math.PI * 2;
    const x = cx + Math.cos(a) * rx;
    const y = cy + Math.sin(a) * ry;
    if (i === 0) d += `M${x.toFixed(1)} ${y.toFixed(1)}`;
    else {
      const r = (2 * Math.PI * Math.sqrt((rx * rx + ry * ry) / 2)) / n / 1.6;
      d += ` A${r.toFixed(1)} ${r.toFixed(1)} 0 0 1 ${x.toFixed(1)} ${y.toFixed(1)}`;
    }
  }
  return d + " Z";
};

const Bell: React.FC<{ x: number; y: number; rot: number; s: number }> = ({ x, y, rot, s }) => (
  <g transform={`translate(${x} ${y}) rotate(${rot}) scale(${s})`}>
    <line x1={0} y1={-60} x2={0} y2={-34} stroke={C.ink} strokeWidth={5} />
    <path d="M-34 20 Q-36 -36 0 -38 Q36 -36 34 20 Q42 26 40 32 H-40 Q-42 26 -34 20 Z" fill={C.mustard} stroke={C.ink} strokeWidth={6} strokeLinejoin="round" />
    <path d="M-16 -18 Q-20 0 -18 14" stroke={C.white} strokeWidth={5} strokeLinecap="round" fill="none" opacity={0.8} />
    <circle cx={0} cy={40} r={9} fill={C.coral} stroke={C.ink} strokeWidth={4} />
  </g>
);

const StampTool: React.FC<{ x: number; y: number }> = ({ x, y }) => (
  <g transform={`translate(${x} ${y})`}>
    <rect x={-26} y={-250} width={52} height={150} rx={20} fill="#A9683E" stroke={C.ink} strokeWidth={7} />
    <circle cx={0} cy={-270} r={50} fill="#A9683E" stroke={C.ink} strokeWidth={7} />
    <rect x={-170} y={-110} width={340} height={80} rx={14} fill="#C98B55" stroke={C.ink} strokeWidth={7} />
    <rect x={-160} y={-34} width={320} height={34} rx={6} fill={C.red} stroke={C.ink} strokeWidth={7} />
    <path d="M-150 -90 H120" stroke="#E4B07E" strokeWidth={8} strokeLinecap="round" />
  </g>
);

export const S2Dream: React.FC = () => {
  const frame = useCurrentFrame();
  const boil = boilOf(frame);
  const c1 = cueAt("s2", 1);
  const c2 = c1; // smug from "convinced..."
  const IMPACT = S2_IMPACT();
  const sh = shake(frame, IMPACT, 14, 26);
  const cloudS = pop(frame, 4, 140, 12);
  const smug = frame >= c2;
  const sway = Math.sin(frame / 9) * 4;

  // stamp tool flies in from the top, slams, and lifts away
  const toolY = interpolate(frame, [IMPACT - 10, IMPACT, IMPACT + 6, IMPACT + 18], [-300, 760, 740, -320], clamp);

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <g transform={`translate(${sh.x} ${sh.y})`}>
          <Paper pattern="dots" />
          <rect x={-20} y={1360} width={1120} height={600} fill={C.paperDeep} />
          <path d="M-20 1360 Q540 1350 1100 1364" stroke={C.ink} strokeWidth={7} fill="none" filter="url(#boil)" />

          {/* thought bubble */}
          <g filter="url(#boil)">
            <circle cx={318} cy={872} r={12 * pop(frame, 2)} fill={C.white} stroke={C.ink} strokeWidth={5} />
            <circle cx={352} cy={838} r={20 * pop(frame, 3)} fill={C.white} stroke={C.ink} strokeWidth={5} />
            <g transform={`translate(560 560) scale(${cloudS}) translate(-560 -560)`}>
              <path d={cloudPath(560, 552, 400, 255, 16)} fill={C.white} stroke={C.ink} strokeWidth={7} />
              {/* bells */}
              {frame >= 22 ? (
                <>
                  <Bell x={300} y={430} rot={Math.sin(frame / 4) * 18 * Math.exp(-(frame - 22) / 60)} s={pop(frame, 22)} />
                  <Bell x={380} y={420} rot={-Math.sin(frame / 4 + 1) * 18 * Math.exp(-(frame - 22) / 60)} s={pop(frame, 25) * 0.85} />
                  <Hand x={340} y={530} size={40} anchor="middle" rotate={-6} opacity={lin(frame, 26, 30)}>
                    ding dong!
                  </Hand>
                </>
              ) : null}
              {/* mandap with the couple */}
              {frame >= 18 ? (
                <g transform={`translate(600 720) scale(${pop(frame, 18)}) translate(-600 -720)`}>
                  <path d="M480 720 V520 Q600 430 720 520 V720" stroke={C.mustardDeep} strokeWidth={14} fill="none" strokeLinecap="round" />
                  <path d="M480 720 V520 Q600 430 720 520 V720" stroke={C.ink} strokeWidth={4} fill="none" strokeDasharray="2 18" strokeLinecap="round" />
                  {[0, 1, 2, 3, 4, 5, 6].map((i) => {
                    const a = Math.PI + (i / 6) * Math.PI;
                    return <circle key={i} cx={600 + Math.cos(a) * 108} cy={505 + Math.sin(a) * 62 + 20} r={10} fill={C.coral} stroke={C.ink} strokeWidth={3} />;
                  })}
                  <Geet x={560} y={712} scale={0.3} expr="dreamy" armL={[14, 8]} armR={[60, 40]} flap={0.2} />
                  <Anshuman x={642} y={712} scale={0.3} expr="smile" armL={[60, 40]} />
                  <Heart x={600 + Math.sin(frame / 6) * 6} y={540 - ((frame - 18) % 40) * 0.6} size={36} />
                </g>
              ) : null}
              {/* house + dogs */}
              {frame >= 34 ? (
                <g transform={`translate(830 640) scale(${pop(frame, 34)}) translate(-830 -640)`}>
                  <rect x={770} y={600} width={120} height={100} fill={C.coral} stroke={C.ink} strokeWidth={6} />
                  <path d="M750 606 L830 530 L910 606 Z" fill={C.mustard} stroke={C.ink} strokeWidth={6} strokeLinejoin="round" />
                  <rect x={815} y={645} width={30} height={55} fill={C.white} stroke={C.ink} strokeWidth={5} />
                  <Heart x={830} y={590} size={26} shine={false} />
                  <Hand x={855} y={750} size={38} anchor="middle" rotate={4}>
                    + 2 dogs
                  </Hand>
                </g>
              ) : null}
              {/* the dramatic runaway train */}
              {frame >= 4 ? (
                <g transform={`translate(${300 + Math.min(frame - 4, 30) * 1.2} 660) rotate(-4) scale(${pop(frame, 4)})`}>
                  {[0, 1, 2].map((i) => (
                    <line key={i} x1={-150 - i * 6} x2={-112} y1={-30 + i * 22} y2={-30 + i * 22} stroke={C.ink} strokeWidth={5} strokeLinecap="round" />
                  ))}
                  <rect x={-100} y={-50} width={190} height={84} rx={18} fill={C.coral} stroke={C.ink} strokeWidth={6} />
                  <rect x={-80} y={-36} width={44} height={34} rx={6} fill={C.white} stroke={C.ink} strokeWidth={4} />
                  <rect x={-24} y={-36} width={44} height={34} rx={6} fill={C.white} stroke={C.ink} strokeWidth={4} />
                  <path d="M50 -50 h40 v84 h-40 Z" fill={C.mustard} stroke={C.ink} strokeWidth={6} />
                  <circle cx={-60} cy={40} r={13} fill={C.ink} />
                  <circle cx={50} cy={40} r={13} fill={C.ink} />
                  <Hand x={0} y={92} size={36} anchor="middle" rotate={3}>
                    elope!!
                  </Hand>
                </g>
              ) : null}
              {/* already a couple */}
              {frame >= 30 ? (
                <g opacity={lin(frame, 30, 35)}>
                  <Hand x={650} y={768} size={38} anchor="middle" color={C.coralDeep} rotate={-3}>
                    her boyfriend
                  </Hand>
                  <path d="M650 738 Q660 728 648 718" stroke={C.coralDeep} strokeWidth={4} fill="none" strokeLinecap="round" />
                </g>
              ) : null}
              <Hand x={560} y={360} size={44} anchor="middle" rotate={-2} opacity={lin(frame, c1 - 4, c1 + 4)}>
                her future: sorted
              </Hand>
            </g>
          </g>

          {/* APPROVED */}
          <Stamp frame={frame} at={IMPACT} x={590} y={600} text="APPROVED" rotate={-11} size={118} color={C.red} />
          {frame >= IMPACT - 10 && frame <= IMPACT + 18 ? <StampTool x={590} y={toolY} /> : null}

          {/* Geet dreaming, then self-approving */}
          <g filter="url(#boil)">
            {!smug
              ? [0, 1, 2].map((i) => {
                  const k = ((frame + i * 20) % 60) / 60;
                  return <Heart key={i} x={170 + i * 40 + Math.sin(k * 6 + i) * 14} y={900 - k * 130} size={30} scale={1 - k * 0.5} />;
                })
              : null}
            <Geet
              x={240}
              y={1362}
              scale={0.95}
              expr={smug ? "smug" : "dreamy"}
              {...(smug ? { armL: POSES.hips.armL, armR: [165, 175] as [number, number] } : POSES.clasp)}
              headTilt={smug ? -4 : sway}
              lean={smug ? -2 : sway * 0.4}
              flap={Math.sin(frame / 10) * 0.25}
            />
            {smug ? (
              <Sparkle x={130} y={860} size={24 * pop(frame, c2 + 2)} rotate={frame * 3} />
            ) : null}
          </g>

          {/* the fine print */}
          {frame >= IMPACT + 6 ? (
            <g transform={`translate(740 1040) rotate(3) scale(${pop(frame, IMPACT + 6)})`} filter="url(#boil)">
              <rect x={-215} y={-125} width={430} height={250} fill="#FFE27A" stroke={C.ink} strokeWidth={6} />
              <rect x={-60} y={-142} width={120} height={34} fill={C.coral} opacity={0.75} />
              <Hand x={-185} y={-55} size={44}>
                approved by: Geet
              </Hand>
              <Hand x={-185} y={10} size={44} opacity={lin(frame, IMPACT + 11, IMPACT + 14)}>
                reviewed by: Geet
              </Hand>
              <Hand x={-185} y={75} size={44} color={C.red} opacity={lin(frame, IMPACT + 17, IMPACT + 20)}>
                evidence: vibes
              </Hand>
            </g>
          ) : null}
        </g>
      </Stage>
    </AbsoluteFill>
  );
};

export const S2_IMPACT = () => cueAt("s2", 1) + 30;
export const _ease = ease;
