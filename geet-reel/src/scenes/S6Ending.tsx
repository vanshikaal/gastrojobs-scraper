import React from "react";
import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { clamp, cueAt, ease, lin, pop, squash } from "../anim";
import { Paper, Stage, boilOf } from "../components/Defs";
import { Arrow, Display, Hand, Heart, Sparkle, Underline } from "../components/Doodles";
import { Geet, POSES } from "../components/Geet";
import { C } from "../theme";
import { MiniClipboard } from "./S5LDL";

export const S6Ending: React.FC = () => {
  const frame = useCurrentFrame();
  const boil = boilOf(frame);
  const c1 = cueAt("s6", 1);
  const c2 = cueAt("s6", 2);
  const END = c2 + 30;
  const beat = 1 + 0.06 * Math.max(0, Math.sin(frame / 4.5)) ** 4;
  const inL = interpolate(ease(frame, 0, 14, 0, 1, (t) => 1 - Math.pow(1 - t, 3)), [0, 1], [-420, 0]);
  const inR = interpolate(ease(frame, 4, 18, 0, 1, (t) => 1 - Math.pow(1 - t, 3)), [0, 1], [420, 0]);
  const HOP = END + 6;
  const hopY = -Math.sin(ease(frame, HOP, HOP + 12) * Math.PI) * 60;
  const [sx, sy] = frame >= HOP + 12 ? squash(frame, HOP + 12, 0.2) : [1, 1];
  const labels = lin(frame, END - 6, END, 1, 0);
  const blink = frame % 64 > 60;
  const hand = <circle r={13} fill={C.skin} stroke={C.ink} strokeWidth={4.5} />;

  return (
    <AbsoluteFill>
      <Stage boilSeed={boil}>
        <Paper pattern="dots" />
        <rect x={-20} y={1290} width={1120} height={700} fill={C.paperDeep} />
        <path d="M-20 1290 Q540 1280 1100 1294" stroke={C.ink} strokeWidth={7} fill="none" filter="url(#boil)" />

        <g filter="url(#boil)">
          {/* normal Geet with an oversized heart */}
          <Geet
            x={300 + inL}
            y={1285 + hopY}
            scale={0.95}
            sx={sx}
            sy={sy}
            expr="grin"
            blink={blink}
            armL={[60, 10]}
            armR={[60, 10]}
            flap={Math.sin(frame / 8) * 0.3}
            front={<Heart x={0} y={-222} size={214} scale={beat} />}
            handL={hand}
            handR={hand}
          />
          {/* LDL Geet with a tiny checklist */}
          <Geet
            x={780 + inR}
            y={1285 + hopY}
            scale={0.95}
            sx={sx}
            sy={sy}
            glasses
            expr="grin"
            blink={frame % 64 > 30 && frame % 64 < 34}
            armL={POSES.hips.armL}
            armR={[35, 160]}
            handR={<MiniClipboard s={0.55} rot={10} />}
            flap={Math.sin(frame / 8 + 1) * 0.3}
          />
          {frame >= 10
            ? [0, 1, 2, 3].map((i) => {
                const k = ((frame + i * 14) % 56) / 56;
                return <Heart key={i} x={200 + i * 70 + Math.sin(k * 6 + i) * 12} y={900 - k * 160} size={26} scale={1 - k * 0.6} />;
              })
            : null}
        </g>

        {/* labels */}
        <g opacity={labels} filter="url(#boil)">
          {frame >= 14 ? (
            <>
              <Hand x={110} y={730} size={52} rotate={-5} opacity={lin(frame, 14, 18)}>
                all heart
              </Hand>
              <Arrow from={[180, 750]} to={[220, 935]} bend={-40} p={ease(frame, 16, 28)} />
            </>
          ) : null}
          {frame >= c1 + 4 ? (
            <>
              <Hand x={560} y={700} size={52} rotate={4} opacity={lin(frame, c1 + 4, c1 + 8)}>
                + one small test
              </Hand>
              <Arrow from={[820, 720]} to={[870, 890]} bend={40} p={ease(frame, c1 + 6, c1 + 18)} />
            </>
          ) : null}
          {frame >= c2 ? (
            <g transform={`translate(540 560) rotate(-3) scale(${pop(frame, c2)})`}>
              <Display x={0} y={0} size={60} color={C.ink}>
                small test → big leap
              </Display>
            </g>
          ) : null}
        </g>

        {/* end card */}
        {frame >= END ? (
          <g filter="url(#boil)">
            <g transform={`translate(540 ${interpolate(ease(frame, END, END + 10, 0, 1, (t) => 1 - Math.pow(1 - t, 3)), [0, 1], [-500, 470])}) rotate(-2)`}>
              <rect x={-410} y={-200} width={820} height={380} rx={36} fill={C.white} stroke={C.ink} strokeWidth={9} />
              <rect x={-410} y={-200} width={820} height={380} rx={36} fill="none" stroke={C.mustard} strokeWidth={6} transform="translate(10 12)" opacity={0.9} />
              <Display x={0} y={-60} size={98} color={C.ink}>
                Prototype first.
              </Display>
              <Display x={0} y={80} size={98} color={C.coral} outline={C.ink} ow={12}>
                Commit better.
              </Display>
              <Underline x1={-300} x2={300} y={112} p={ease(frame, END + 12, END + 24)} color={C.mustard} w={11} />
            </g>
            <Sparkle x={130} y={300} size={30 * pop(frame, END + 12)} rotate={frame * 2} />
            <Sparkle x={950} y={640} size={26 * pop(frame, END + 15)} rotate={-frame * 2} color={C.coral} />
            <Hand x={540} y={760} size={50} anchor="middle" opacity={lin(frame, END + 22, END + 28)}>
              (keep the heart. add the test.)
            </Hand>
          </g>
        ) : null}
      </Stage>
    </AbsoluteFill>
  );
};

export const _c = clamp;
