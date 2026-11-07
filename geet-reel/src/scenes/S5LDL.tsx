import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, squash } from "../anim";
import { Anshuman } from "../components/Anshuman";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Bubble, Check, Display, Hand, Sparkle, SpeedLines, Stamp, Strike, Underline } from "../components/Doodles";
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

export const S5LDL: React.FC = () => {
  const frame = useCurrentFrame();
  const boil = boilOf(frame);
  const c1 = cueAt("s5", 1);
  const c2 = cueAt("s5", 2);
  const c3 = cueAt("s5", 3);
  const PORTAL = 46;
  const PHONE = c1 + 26;
  const VISIT = c2 - 3;
  const PLAN = c3 - 3;
  const portalR = ease(frame, PORTAL, PORTAL + 16, 0, 1300, (t) => t * t);
  const blink = frame % 70 > 66;

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        {/* ---------- Beat A: the original plan (run away) ---------- */}
        {frame < PORTAL + 16 ? (
          <g>
            <Paper />
            <rect x={-20} y={1290} width={1120} height={700} fill={C.paperDeep} />
            <path d="M-20 1290 Q540 1280 1100 1294" stroke={C.ink} strokeWidth={7} fill="none" />
            <Hand x={540} y={420} size={60} anchor="middle" rotate={-3} opacity={lin(frame, 4, 8)}>
              plan A:
            </Hand>
            <Display x={540} y={520} size={74} rotate={-3} scale={pop(frame, 6)}>
              RUN AWAY AT NIGHT
            </Display>
            {(() => {
              const r = runPose(frame * 0.55);
              const gx = interpolate(frame, [0, 44], [900, 380], clamp);
              return (
                <g filter="url(#boil)">
                  <SpeedLines x={gx + 70} y={1030} frame={frame} dir={-1} />
                  <Geet x={gx} y={1290 + r.bob} flip expr="determined" legL={r.legL} legR={r.legR} armL={r.armL} armR={[40, 90]} lean={r.lean} flap={r.flap} />
                </g>
              );
            })()}
            <Stamp frame={frame} at={24} x={540} y={900} text="NOPE" rotate={10} size={130} color={C.red} />
          </g>
        ) : null}

        {/* ---------- Portal into the LDL universe ---------- */}
        {frame >= PORTAL ? (
          <g>
            <clipPath id="portal">
              <circle cx={540} cy={900} r={portalR} />
            </clipPath>
            <g clipPath="url(#portal)">
              <Paper tint="#FFE6DC" pattern="dots" />
              <g filter="url(#boil)">
                <g transform={`translate(540 360) rotate(-3) scale(${pop(frame, PORTAL + 8)})`}>
                  <rect x={-410} y={-70} width={820} height={150} rx={22} fill={C.ink} />
                  <Hand x={0} y={-12} size={48} anchor="middle" color={C.mustard}>
                    meanwhile, in the
                  </Hand>
                  <Display x={0} y={58} size={68} color={C.white} spacing={3}>
                    LDL UNIVERSE
                  </Display>
                </g>
                {frame < PHONE + 4 ? (
                  <>
                    {(() => {
                      const [sx, sy] = squash(frame, PORTAL + 14, 0.28);
                      const drop = interpolate(frame, [PORTAL + 6, PORTAL + 14], [-700, 0], clamp);
                      return (
                        <Geet
                          x={540}
                          y={1260 + drop}
                          sx={sx}
                          sy={sy}
                          glasses
                          blink={blink}
                          expr="smug"
                          armL={[35, 160]}
                          armR={POSES.hips.armR}
                          handL={<MiniClipboard s={1.1} rot={-6} />}
                          flap={Math.sin(frame / 9) * 0.2}
                        />
                      );
                    })()}
                    {frame >= c1 ? (
                      <g transform={`translate(800 790) rotate(6) scale(${pop(frame, c1)})`}>
                        <rect x={-120} y={-50} width={240} height={86} rx={16} fill={C.mustard} stroke={C.ink} strokeWidth={6} />
                        <Display x={0} y={16} size={50}>
                          LDL GEET
                        </Display>
                      </g>
                    ) : null}
                    {frame >= c1 + 4 ? (
                      <>
                        <Hand x={170} y={700} size={42} rotate={-8} opacity={lin(frame, c1 + 4, c1 + 8)}>
                          tiny glasses
                        </Hand>
                        <path d="M240 712 Q300 760 470 800" stroke={C.ink} strokeWidth={5} fill="none" strokeLinecap="round" opacity={lin(frame, c1 + 6, c1 + 10)} />
                        <Hand x={150} y={1060} size={42} rotate={-4} opacity={lin(frame, c1 + 10, c1 + 14)}>
                          clipboard
                        </Hand>
                        <path d="M260 1060 Q330 1060 390 1010" stroke={C.ink} strokeWidth={5} fill="none" strokeLinecap="round" opacity={lin(frame, c1 + 12, c1 + 16)} />
                      </>
                    ) : null}
                  </>
                ) : null}
              </g>
            </g>
            {portalR < 1250 ? <circle cx={540} cy={900} r={portalR} fill="none" stroke={C.coral} strokeWidth={26} /> : null}
            {portalR < 1250 ? <circle cx={540} cy={900} r={portalR} fill="none" stroke={C.ink} strokeWidth={6} strokeDasharray="30 20" /> : null}
          </g>
        ) : null}

        {/* ---------- Beat B: ask the hard questions (split-screen call) ---------- */}
        {frame >= PHONE && frame < VISIT + 10 ? (
          <g transform={`translate(${slideIn(frame, PHONE)} 0)`}>
            <Paper tint="#FFE6DC" />
            <clipPath id="panelTop">
              <path d="M70 250 H1010 V780 L70 840 Z" />
            </clipPath>
            <clipPath id="panelBot">
              <path d="M70 870 L1010 810 V1340 H70 Z" />
            </clipPath>
            <g filter="url(#boil)">
              <path d="M70 250 H1010 V780 L70 840 Z" fill={C.mustard} />
              <path d="M70 870 L1010 810 V1340 H70 Z" fill={C.blueLine} />
              <g clipPath="url(#panelTop)">
                <Geet x={290} y={1010} glasses expr="determined" blink={blink} armL={[35, 160]} handL={<MiniClipboard rot={-6} />} armR={[75, 195]} handR={<Phone rot={-25} />} headTilt={4} />
              </g>
              <g clipPath="url(#panelBot)">
                <Anshuman x={800} y={1570} expr={frame >= PHONE + 34 ? "sweat" : "think"} armR={[75, 195]} handR={<Phone rot={25} />} headTilt={-4} />
              </g>
              <path d="M70 250 H1010 V780 L70 840 Z" fill="none" stroke={C.ink} strokeWidth={8} strokeLinejoin="round" />
              <path d="M70 870 L1010 810 V1340 H70 Z" fill="none" stroke={C.ink} strokeWidth={8} strokeLinejoin="round" />
              {frame >= PHONE + 4 ? (
                <Bubble x={470} y={290} w={480} h={160} tx={430} ty={480} s={pop(frame, PHONE + 4)}>
                  <Hand x={710} y={360} size={48} anchor="middle">
                    What do YOU want
                  </Hand>
                  <Hand x={710} y={415} size={48} anchor="middle">
                    from this, really?
                  </Hand>
                </Bubble>
              ) : null}
              {frame >= PHONE + 18 ? (
                <Bubble x={470} y={540} w={480} h={110} tx={420} ty={560} s={pop(frame, PHONE + 18)}>
                  <Hand x={710} y={610} size={46} anchor="middle">
                    Where would we live?
                  </Hand>
                </Bubble>
              ) : null}
              {frame >= PHONE + 32 ? (
                <Bubble x={120} y={930} w={460} h={130} tx={640} ty={1080} s={pop(frame, PHONE + 32)}>
                  <Hand x={350} y={1012} size={52} anchor="middle">
                    ...good question.
                  </Hand>
                </Bubble>
              ) : null}
            </g>
          </g>
        ) : null}

        {/* ---------- Beat C: visit without committing ---------- */}
        {frame >= VISIT && frame < PLAN + 10 ? (
          <g transform={`translate(${slideIn(frame, VISIT)} 0)`}>
            <Paper tint="#FFE6DC" />
            <rect x={-20} y={1270} width={1120} height={700} fill={C.paperDeep} />
            <g filter="url(#boil)">
              <path d="M-20 1270 Q540 1262 1100 1274" stroke={C.ink} strokeWidth={7} fill="none" />
              <g transform={`translate(540 380) rotate(-2) scale(${pop(frame, VISIT + 2)})`}>
                <Display x={0} y={0} size={84}>
                  VISIT ≠ MOVE IN
                </Display>
                <Underline x1={-330} x2={330} y={26} p={ease(frame, VISIT + 6, VISIT + 16)} />
              </g>
              {/* house */}
              <g transform="translate(780 1270)">
                <rect x={-150} y={-300} width={300} height={300} fill={C.coral} stroke={C.ink} strokeWidth={7} />
                <path d="M-180 -290 L0 -450 L180 -290 Z" fill={C.mustard} stroke={C.ink} strokeWidth={7} strokeLinejoin="round" />
                <rect x={-110} y={-230} width={80} height={70} fill={C.white} stroke={C.ink} strokeWidth={6} />
                <rect x={10} y={-170} width={90} height={170} fill={C.inkSoft} stroke={C.ink} strokeWidth={6} />
              </g>
              <Anshuman x={835} y={1262} scale={0.55} expr="smile" armL={[10, 6]} armR={[130, 175]} />
              {(() => {
                const walk = runPose(frame * 0.32);
                const gx = interpolate(frame, [VISIT, VISIT + 40], [150, 430], clamp);
                return (
                  <Geet
                    x={gx}
                    y={1262 + walk.bob * 0.3}
                    scale={0.8}
                    glasses
                    expr="smile"
                    legL={[walk.legL[0] * 0.6, walk.legL[1] * 0.6]}
                    legR={[walk.legR[0] * 0.6, walk.legR[1] * 0.6]}
                    armL={[20, 10]}
                    armR={[70, 150] as Arm}
                    handR={<Ticket />}
                    flap={0.3}
                  />
                );
              })()}
              <g transform={`translate(250 640) rotate(-6) scale(${pop(frame, VISIT + 12)})`}>
                <rect x={-140} y={-80} width={280} height={170} rx={14} fill={C.white} stroke={C.ink} strokeWidth={6} />
                <rect x={-140} y={-80} width={280} height={50} rx={14} fill={C.red} stroke={C.ink} strokeWidth={6} />
                <Display x={0} y={-43} size={30} color={C.white}>
                  WEEKEND TRIP
                </Display>
                <Display x={0} y={40} size={58}>
                  SAT–SUN
                </Display>
              </g>
            </g>
          </g>
        ) : null}

        {/* ---------- Beat D: update the plan ---------- */}
        {frame >= PLAN ? (
          <g transform={`translate(${slideIn(frame, PLAN)} 0)`}>
            <Paper tint="#FFE6DC" pattern="dots" />
            <g filter="url(#boil)">
              <g transform="rotate(-2 540 760)">
                <rect x={140} y={290} width={800} height={960} rx={30} fill="#B07A47" stroke={C.ink} strokeWidth={8} />
                <rect x={180} y={350} width={720} height={860} fill={C.white} stroke={C.ink} strokeWidth={5} />
                <rect x={430} y={262} width={220} height={70} rx={16} fill={C.silver} stroke={C.ink} strokeWidth={6} />
                <Display x={540} y={460} size={76}>
                  THE PLAN
                </Display>
                <Hand x={220} y={570} size={56} color={C.inkSoft}>
                  v1: run away tonight
                </Hand>
                <Strike x1={210} x2={680} y={552} p={ease(frame, PLAN + 6, PLAN + 14)} />
                <Hand x={220} y={680} size={60} color={C.coralDeep} opacity={lin(frame, PLAN + 14, PLAN + 18)}>
                  v2:
                </Hand>
                {[
                  { t: "ask the hard questions", at: PLAN + 18 },
                  { t: "visit for a weekend", at: PLAN + 32 },
                  { t: "see if it actually works", at: PLAN + 46 },
                ].map((it, i) => (
                  <g key={i} opacity={lin(frame, it.at - 4, it.at)}>
                    <rect x={230} y={730 + i * 120} width={56} height={56} rx={8} fill="none" stroke={C.ink} strokeWidth={5} />
                    <Check x={260} y={760 + i * 120} p={ease(frame, it.at, it.at + 6)} size={52} />
                    <Hand x={310} y={778 + i * 120} size={54}>
                      {it.t}
                    </Hand>
                  </g>
                ))}
              </g>
              <Stamp frame={frame} at={PLAN + 66} x={690} y={1130} text="UPDATED" rotate={-8} size={64} color={C.green} />
              <Sparkle x={210} y={1190} size={24 * pop(frame, PLAN + 68)} rotate={frame * 3} />
              <Sparkle x={900} y={300} size={20 * pop(frame, PLAN + 70)} rotate={-frame * 3} color={C.coral} />
            </g>
          </g>
        ) : null}
      </Stage>
    </AbsoluteFill>
  );
};
