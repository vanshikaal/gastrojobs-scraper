import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, squash } from "../anim";
import { Anshuman } from "../components/Anshuman";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Arrow, Bubble, Display, Hand, QMark, Sparkle, Stamp, Strike } from "../components/Doodles";
import { Arm, Geet, POSES, runPose } from "../components/Geet";
import { C, FONT_DISPLAY } from "../theme";

export const MiniClipboard: React.FC<{ s?: number; rot?: number }> = ({ s = 1, rot = -8 }) => (
  <g transform={`rotate(${rot}) scale(${s}) translate(-10 -70)`}>
    <rect x={-38} y={-10} width={76} height={98} rx={8} fill="#B07A47" stroke={C.ink} strokeWidth={5} />
    <rect x={-30} y={0} width={60} height={80} fill={C.white} stroke={C.ink} strokeWidth={3} />
    <rect x={-16} y={-16} width={32} height={14} rx={4} fill={C.silver} stroke={C.ink} strokeWidth={3} />
    {[16, 32, 48, 64].map((y) => (
      <g key={y}>
        <path d={`M-22 ${y} l4 4 l7 -9`} stroke={C.green} strokeWidth={3.5} fill="none" strokeLinecap="round" />
        <line x1={-6} x2={22} y1={y} y2={y} stroke={C.inkSoft} strokeWidth={3} strokeLinecap="round" />
      </g>
    ))}
  </g>
);

const Phone: React.FC<{ rot?: number }> = ({ rot = -20 }) => (
  <g transform={`rotate(${rot}) translate(-14 -62)`}>
    <rect x={0} y={0} width={30} height={60} rx={7} fill={C.ink} />
    <rect x={4} y={6} width={22} height={42} rx={3} fill={C.coral} />
  </g>
);

const Ticket: React.FC = () => (
  <g transform="rotate(-12) translate(-10 -50)">
    <rect x={0} y={0} width={150} height={66} rx={8} fill={C.mustard} stroke={C.ink} strokeWidth={5} />
    <line x1={110} y1={4} x2={110} y2={62} stroke={C.ink} strokeWidth={3} strokeDasharray="6 5" />
    <text x={55} y={30} textAnchor="middle" fontFamily={FONT_DISPLAY} fontWeight={800} fontSize={20} fill={C.ink}>
      RETURN
    </text>
    <text x={55} y={54} textAnchor="middle" fontFamily={FONT_DISPLAY} fontWeight={800} fontSize={20} fill={C.ink}>
      TICKET
    </text>
    <path d="M120 34 l6 7 l12 -16" stroke={C.green} strokeWidth={5} fill="none" strokeLinecap="round" />
  </g>
);

/** Slide a beat in from the right with a little overshoot. */
const slideIn = (frame: number, at: number) => interpolate(ease(frame, at, at + 9, 0, 1, (t) => 1 - Math.pow(1 - t, 3)), [0, 1], [1100, 0]);

/** Numbered label for the two Designing Your Life prototype types. */
const TypeTag: React.FC<{ n: number; text: string; frame: number; at: number; color: string }> = ({ n, text, frame, at, color }) => (
  <g transform={`translate(540 300) rotate(-2) scale(${pop(frame, at)})`}>
    <rect x={-430} y={-48} width={860} height={92} rx={18} fill={color} stroke={C.ink} strokeWidth={7} />
    <circle cx={-375} cy={-2} r={32} fill={C.white} stroke={C.ink} strokeWidth={5} />
    <Display x={-375} y={16} size={46}>
      {String(n)}
    </Display>
    <Display x={30} y={18} size={52} spacing={1}>
      {text}
    </Display>
  </g>
);

const beats = () => {
  const c1 = cueAt("s5", 1);
  const c2 = cueAt("s5", 2);
  return { CONV: 30, EXP: c1 - 3, LEARN: c2 - 3 };
};

export const S5LDL: React.FC = () => {
  const frame = useCurrentFrame();
  const boil = boilOf(frame);
  const { CONV, EXP, LEARN } = beats();
  const portalR = ease(frame, 0, 14, 0, 1300, (t) => t * t);
  const blink = frame % 70 > 66;
  const ANSWER = CONV + 30;

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        {/* ---------- Portal into the LDL universe ---------- */}
        {frame < CONV + 10 ? (
          <g>
            <Paper />
            <clipPath id="portal">
              <circle cx={540} cy={900} r={portalR} />
            </clipPath>
            <g clipPath="url(#portal)">
              <Paper tint="#FFE6DC" pattern="dots" />
              <g filter="url(#boil)">
                <g transform={`translate(540 380) rotate(-3) scale(${pop(frame, 6)})`}>
                  <rect x={-410} y={-70} width={820} height={150} rx={22} fill={C.ink} />
                  <Hand x={0} y={-12} size={48} anchor="middle" color={C.mustard}>
                    meanwhile, in the
                  </Hand>
                  <Display x={0} y={58} size={68} color={C.white} spacing={3}>
                    LDL UNIVERSE
                  </Display>
                </g>
                {(() => {
                  const [sx, sy] = squash(frame, 14, 0.28);
                  const drop = interpolate(frame, [6, 14], [-700, 0], clamp);
                  return (
                    <Geet x={540} y={1260 + drop} sx={sx} sy={sy} glasses blink={blink} expr="smug" armL={[35, 160]} armR={POSES.hips.armR} handL={<MiniClipboard s={1.1} rot={-6} />} flap={Math.sin(frame / 9) * 0.2} />
                  );
                })()}
                {frame >= 14 ? (
                  <g transform={`translate(800 790) rotate(6) scale(${pop(frame, 14)})`}>
                    <rect x={-120} y={-50} width={240} height={86} rx={16} fill={C.mustard} stroke={C.ink} strokeWidth={6} />
                    <Display x={0} y={16} size={50}>
                      LDL GEET
                    </Display>
                  </g>
                ) : null}
              </g>
            </g>
            {portalR < 1250 ? <circle cx={540} cy={900} r={portalR} fill="none" stroke={C.coral} strokeWidth={26} /> : null}
            {portalR < 1250 ? <circle cx={540} cy={900} r={portalR} fill="none" stroke={C.ink} strokeWidth={6} strokeDasharray="30 20" /> : null}
          </g>
        ) : null}

        {/* ---------- 1 · Prototype conversation ---------- */}
        {frame >= CONV && frame < EXP + 10 ? (
          <g transform={`translate(${slideIn(frame, CONV)} 0)`}>
            <Paper tint="#FFE6DC" />
            <clipPath id="panelTop">
              <path d="M70 380 H1010 V800 L70 860 Z" />
            </clipPath>
            <clipPath id="panelBot">
              <path d="M70 890 L1010 830 V1340 H70 Z" />
            </clipPath>
            <g filter="url(#boil)">
              <TypeTag n={1} text="PROTOTYPE CONVERSATION" frame={frame} at={CONV + 4} color={C.mustard} />
              <path d="M70 380 H1010 V800 L70 860 Z" fill="#FFD27A" />
              <path d="M70 890 L1010 830 V1340 H70 Z" fill={C.blueLine} />
              <g clipPath="url(#panelTop)">
                <Geet x={270} y={1040} glasses expr={frame >= ANSWER + 8 ? "worried" : "smile"} blink={blink} armL={[35, 160]} handL={<MiniClipboard rot={-6} />} armR={[60, -68]} headTilt={frame >= ANSWER + 8 ? -6 : 4} />
              </g>
              <g clipPath="url(#panelBot)">
                <Anshuman x={800} y={1560} expr={frame >= ANSWER ? "sweat" : "smile"} {...(frame >= ANSWER ? { armL: [60, 150] as Arm, armR: [60, 150] as Arm } : {})} headTilt={-4} />
              </g>
              <path d="M70 380 H1010 V800 L70 860 Z" fill="none" stroke={C.ink} strokeWidth={8} strokeLinejoin="round" />
              <path d="M70 890 L1010 830 V1340 H70 Z" fill="none" stroke={C.ink} strokeWidth={8} strokeLinejoin="round" />
              {frame >= CONV + 8 ? (
                <Bubble x={470} y={420} w={490} h={170} tx={400} ty={590} s={pop(frame, CONV + 8)}>
                  <Hand x={715} y={492} size={46} anchor="middle">
                    So... what does our future
                  </Hand>
                  <Hand x={715} y={548} size={46} anchor="middle">
                    actually look like to you?
                  </Hand>
                </Bubble>
              ) : null}
              {frame >= ANSWER ? (
                <Bubble x={110} y={950} w={480} h={160} tx={650} ty={1110} s={pop(frame, ANSWER)}>
                  <Hand x={350} y={1020} size={48} anchor="middle">
                    Honestly? Marriage...
                  </Hand>
                  <Hand x={350} y={1076} size={48} anchor="middle">
                    not just yet.
                  </Hand>
                </Bubble>
              ) : null}
              <QMark frame={frame} at={ANSWER + 10} x={420} y={600} size={80} color={C.coral} rot={10} />
            </g>
          </g>
        ) : null}

        {/* ---------- 2 · Prototype experience ---------- */}
        {frame >= EXP && frame < LEARN + 10 ? (
          <g transform={`translate(${slideIn(frame, EXP)} 0)`}>
            <Paper tint="#FFE6DC" />
            <rect x={-20} y={1270} width={1120} height={700} fill={C.paperDeep} />
            <g filter="url(#boil)">
              <path d="M-20 1270 Q540 1262 1100 1274" stroke={C.ink} strokeWidth={7} fill="none" />
              <TypeTag n={2} text="PROTOTYPE EXPERIENCE" frame={frame} at={EXP + 4} color={C.coral} />
              <Hand x={540} y={430} size={50} anchor="middle" opacity={lin(frame, EXP + 10, EXP + 14)}>
                a weekend together. no wedding.
              </Hand>
              {/* hills + sun: a little trip */}
              <path d="M-20 1270 Q200 1010 420 1270 M380 1270 Q640 960 900 1270 M840 1270 Q1000 1100 1100 1200" fill={C.mustard} stroke={C.ink} strokeWidth={6} opacity={0.55} />
              <circle cx={840} cy={640} r={70} fill={C.mustard} stroke={C.ink} strokeWidth={6} />
              <g transform={`translate(250 650) rotate(-6) scale(${pop(frame, EXP + 14)})`}>
                <rect x={-140} y={-80} width={280} height={170} rx={14} fill={C.white} stroke={C.ink} strokeWidth={6} />
                <rect x={-140} y={-80} width={280} height={50} rx={14} fill={C.red} stroke={C.ink} strokeWidth={6} />
                <Display x={0} y={-43} size={30} color={C.white}>
                  WEEKEND TRIP
                </Display>
                <Display x={0} y={40} size={58}>
                  SAT–SUN
                </Display>
              </g>
              {(() => {
                const walk = runPose(frame * 0.32);
                const gx = interpolate(frame, [EXP, EXP + 50], [170, 400], clamp);
                const leg = (l: [number, number]): [number, number] => [l[0] * 0.6, l[1] * 0.6];
                return (
                  <>
                    <Anshuman x={gx + 250} y={1262 + walk.bob * 0.3} scale={0.8} expr="smile" legL={leg(walk.legR)} legR={leg(walk.legL)} armL={[20, 10]} armR={[20, 10]} />
                    <Geet x={gx} y={1262 + walk.bob * 0.3} scale={0.8} glasses expr="happy" legL={leg(walk.legL)} legR={leg(walk.legR)} armL={[20, 10]} armR={[70, 150] as Arm} handR={<Ticket />} flap={0.3} />
                  </>
                );
              })()}
              {frame >= EXP + 30 ? (
                <Bubble x={560} y={760} w={420} h={110} tx={440} ty={880} s={pop(frame, EXP + 30)}>
                  <Hand x={770} y={830} size={44} anchor="middle">
                    {"(no ring. just notes.)"}
                  </Hand>
                </Bubble>
              ) : null}
            </g>
          </g>
        ) : null}

        {/* ---------- 3 · What she learned → reassess ---------- */}
        {frame >= LEARN ? (
          <g transform={`translate(${slideIn(frame, LEARN)} 0)`}>
            <Paper tint="#FFE6DC" pattern="dots" />
            <g filter="url(#boil)">
              <g transform="rotate(-2 540 760)">
                <rect x={140} y={290} width={800} height={960} rx={30} fill="#B07A47" stroke={C.ink} strokeWidth={8} />
                <rect x={180} y={350} width={720} height={860} fill={C.white} stroke={C.ink} strokeWidth={5} />
                <rect x={430} y={262} width={220} height={70} rx={16} fill={C.silver} stroke={C.ink} strokeWidth={6} />
                <Display x={540} y={450} size={72}>
                  MY PLAN
                </Display>
                <Display x={220} y={535} size={32} anchor="start" color={C.inkSoft} spacing={2}>
                  I ASSUMED:
                </Display>
                <Hand x={220} y={600} size={50} color={C.inkSoft}>
                  we want the same thing, now
                </Hand>
                <Strike x1={210} x2={790} y={584} w={4} p={ease(frame, LEARN + 10, LEARN + 18)} />
                <g opacity={lin(frame, LEARN + 20, LEARN + 24)}>
                  <Display x={220} y={700} size={32} anchor="start" color={C.coralDeep} spacing={2}>
                    I LEARNED:
                  </Display>
                  <Hand x={220} y={765} size={50}>
                    timelines don't match (yet)
                  </Hand>
                </g>
                <g opacity={lin(frame, LEARN + 40, LEARN + 44)}>
                  <Display x={220} y={865} size={32} anchor="start" color={C.green} spacing={2}>
                    SO, PLAN v2:
                  </Display>
                  <Hand x={220} y={930} size={50}>
                    slow down. keep talking.
                  </Hand>
                </g>
                {frame >= LEARN + 30 ? <Arrow from={[860, 745]} to={[835, 905]} bend={-50} p={ease(frame, LEARN + 30, LEARN + 42)} color={C.coralDeep} /> : null}
                <Hand x={540} y={1180} size={40} anchor="middle" color={C.inkSoft} opacity={lin(frame, LEARN + 66, LEARN + 70)}>
                  (no crystal ball. just better info.)
                </Hand>
              </g>
              <Stamp frame={frame} at={LEARN + 54} x={620} y={1060} text="REASSESSED" rotate={-8} size={60} color={C.green} />
              <Sparkle x={210} y={1190} size={24 * pop(frame, LEARN + 56)} rotate={frame * 3} />
              <Sparkle x={900} y={300} size={20 * pop(frame, LEARN + 58)} rotate={-frame * 3} color={C.coral} />
            </g>
          </g>
        ) : null}
      </Stage>
    </AbsoluteFill>
  );
};

/** Sound cues for this scene (scene-relative frames). */
export const S5_BEATS = () => {
  const { CONV, EXP, LEARN } = beats();
  return [
    { at: 0, file: "whoosh", vol: 0.6 },
    { at: 14, file: "thump", vol: 0.5 },
    { at: CONV, file: "whoosh", vol: 0.4 },
    { at: CONV + 8, file: "pop", vol: 0.4 },
    { at: CONV + 30, file: "pop", vol: 0.4 },
    { at: EXP, file: "whoosh", vol: 0.4 },
    { at: EXP + 30, file: "pop", vol: 0.35 },
    { at: LEARN, file: "whoosh", vol: 0.4 },
    { at: LEARN + 54, file: "thump", vol: 0.6 },
    { at: LEARN + 56, file: "ding", vol: 0.35 },
  ];
};
