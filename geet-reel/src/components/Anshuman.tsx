import React from "react";
import { C } from "../theme";
import { Arm, Leg, armPoints, legPoints } from "./Geet";

export type AnshExpr = "neutral" | "confused" | "sweat" | "smile" | "think";

export type AnshProps = {
  x: number;
  y: number;
  scale?: number;
  expr?: AnshExpr;
  armL?: Arm;
  armR?: Arm;
  legL?: Leg;
  legR?: Leg;
  lean?: number;
  sx?: number;
  sy?: number;
  headTilt?: number;
  handL?: React.ReactNode;
  handR?: React.ReactNode;
  look?: number;
  blink?: boolean;
  opacity?: number;
};

const OUT = C.ink;

const Limb: React.FC<{ pts: [number, number][]; w: number; fill: string; ow?: number }> = ({ pts, w, fill, ow = 6 }) => {
  const d = pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(" ");
  return (
    <g>
      <path d={d} stroke={OUT} strokeWidth={w + ow * 2} strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <path d={d} stroke={fill} strokeWidth={w} strokeLinecap="round" strokeLinejoin="round" fill="none" />
    </g>
  );
};

/**
 * Anshuman: simpler doodle guy in a slate-blue shirt, navy trousers, white sneakers.
 * Feet at (0,0), head centre at (0,-448).
 */
export const Anshuman: React.FC<AnshProps> = ({
  x,
  y,
  scale = 1,
  expr = "neutral",
  armL = [10, 6],
  armR = [10, 6],
  legL = [-3, -3],
  legR = [3, 3],
  lean = 0,
  sx = 1,
  sy = 1,
  headTilt = 0,
  handL,
  handR,
  look = 0,
  blink,
  opacity = 1,
}) => {
  const L = armPoints(-1, armL, 48, -360, 86, 80);
  const R = armPoints(1, armR, 48, -360, 86, 80);
  const LL = legPoints(-1, legL, -196, 100, 98);
  const LR = legPoints(1, legR, -196, 100, 98);
  const ey = -446;
  const sleeve = (A: { S: [number, number]; E: [number, number] }): [number, number][] => [A.S, [A.S[0] + (A.E[0] - A.S[0]) * 0.85, A.S[1] + (A.E[1] - A.S[1]) * 0.85]];
  let brows = "M-32 -470 Q-21 -476 -9 -471 M9 -471 Q21 -476 32 -470";
  if (expr === "confused") brows = "M-32 -466 Q-21 -468 -9 -466 M9 -476 Q21 -488 32 -480";
  if (expr === "sweat") brows = "M-32 -466 Q-21 -472 -9 -478 M9 -478 Q21 -472 32 -466";
  if (expr === "think") brows = "M-32 -468 Q-21 -474 -9 -470 M9 -474 Q21 -482 32 -476";
  let mouth = <path d="M-12 -418 Q0 -414 12 -418" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  if (expr === "confused") mouth = <path d="M-12 -416 Q-4 -422 2 -416 Q8 -410 13 -417" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  if (expr === "sweat") mouth = <path d="M-13 -414 Q0 -424 13 -414" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  if (expr === "smile") mouth = <path d="M-14 -420 Q2 -404 16 -422" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  if (expr === "think") mouth = <path d="M-6 -416 Q4 -419 12 -414" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  return (
    <g transform={`translate(${x} ${y}) scale(${scale * sx} ${scale * sy}) rotate(${lean})`} opacity={opacity}>
      {/* legs */}
      <Limb pts={[LL.Hp, LL.K, LL.F]} w={34} fill={C.navy} />
      <Limb pts={[LR.Hp, LR.K, LR.F]} w={34} fill={C.navy} />
      {[LL.F, LR.F].map((F, i) => (
        <g key={i} transform={`translate(${F[0] + (i ? 10 : -10)} ${F[1] + 6})`}>
          <path d="M-26 6 Q-26 -14 -4 -14 Q14 -14 26 4 Q28 12 18 12 H-22 Q-26 12 -26 6 Z" fill={C.white} stroke={OUT} strokeWidth={4} strokeLinejoin="round" />
          <path d="M-24 6 H24" stroke={OUT} strokeWidth={3} />
        </g>
      ))}
      {/* shirt */}
      <path
        d="M-50 -368 Q0 -380 50 -368 L54 -250 Q56 -205 58 -186 Q0 -176 -58 -186 Q-56 -205 -54 -250 Z"
        fill={C.shirt}
        stroke={OUT}
        strokeWidth={6}
        strokeLinejoin="round"
      />
      <path d="M0 -360 V-190" stroke={C.shirtDeep} strokeWidth={4} />
      {[-330, -294, -258, -222].map((by) => (
        <circle key={by} cx={0} cy={by} r={3.5} fill={C.white} stroke={OUT} strokeWidth={1.5} />
      ))}
      <rect x={14} y={-330} width={28} height={30} rx={3} fill="none" stroke={C.shirtDeep} strokeWidth={4} />
      {/* collar + open neck */}
      <path d="M-14 -372 L0 -338 L14 -372 Z" fill={C.skin} stroke={OUT} strokeWidth={4} strokeLinejoin="round" />
      <path d="M-16 -376 L-30 -352 L-4 -350 Z M16 -376 L30 -352 L4 -350 Z" fill={C.shirt} stroke={OUT} strokeWidth={4} strokeLinejoin="round" />
      {/* arms */}
      {[L, R].map((A, i) => (
        <g key={i}>
          <Limb pts={[A.S, A.E, A.H]} w={14} fill={C.skin} ow={5} />
          <Limb pts={sleeve(A)} w={30} fill={C.shirt} ow={5} />
          <circle cx={A.H[0]} cy={A.H[1]} r={13} fill={C.skin} stroke={OUT} strokeWidth={4.5} />
          {i === 0 ? (
            <circle
              cx={A.E[0] + (A.H[0] - A.E[0]) * 0.8}
              cy={A.E[1] + (A.H[1] - A.E[1]) * 0.8}
              r={8}
              fill="#333"
              stroke={OUT}
              strokeWidth={2}
            />
          ) : null}
        </g>
      ))}
      {/* neck + head */}
      <rect x={-12} y={-400} width={24} height={30} fill={C.skin} stroke={OUT} strokeWidth={4} />
      <g transform={`rotate(${headTilt} 0 -390)`}>
        <ellipse cx={-54} cy={-442} rx={9} ry={12} fill={C.skin} stroke={OUT} strokeWidth={4} />
        <ellipse cx={54} cy={-442} rx={9} ry={12} fill={C.skin} stroke={OUT} strokeWidth={4} />
        <path d="M-54 -460 Q-58 -400 -30 -396 Q0 -386 30 -396 Q58 -400 54 -460 Q50 -500 0 -502 Q-50 -500 -54 -460 Z" fill={C.skin} stroke={OUT} strokeWidth={5.5} />
        {/* stubble */}
        {[-30, -20, -10, 0, 10, 20, 30, -24, -12, 12, 24, -4, 6].map((sx2, i) => (
          <circle key={i} cx={sx2} cy={i > 6 ? -404 - (i % 2) * 6 : -400 - Math.abs(sx2) * 0.18} r={1.6} fill={C.inkSoft} opacity={0.65} />
        ))}
        {blink ? (
          <g stroke={OUT} strokeWidth={4} strokeLinecap="round">
            <path d={`M-28 ${ey} H-12 M12 ${ey} H28`} />
          </g>
        ) : (
          <g>
            <ellipse cx={-20} cy={ey} rx={8} ry={9} fill={C.white} stroke={OUT} strokeWidth={3} />
            <ellipse cx={20} cy={ey} rx={8} ry={9} fill={C.white} stroke={OUT} strokeWidth={3} />
            <circle cx={-20 + look * 3} cy={ey + 1} r={5} fill={OUT} />
            <circle cx={20 + look * 3} cy={ey + 1} r={5} fill={OUT} />
          </g>
        )}
        <path d={brows} stroke={OUT} strokeWidth={6} strokeLinecap="round" fill="none" />
        <path d="M2 -440 Q-4 -428 4 -426" stroke={OUT} strokeWidth={3} fill="none" strokeLinecap="round" />
        {mouth}
        {/* wavy hair */}
        <path
          d="M-58 -448 C-72 -500 -40 -536 4 -530 C44 -528 70 -500 60 -450 C56 -466 48 -478 36 -482 C30 -470 14 -470 6 -482 C-6 -472 -24 -474 -30 -486 C-42 -478 -50 -466 -58 -448 Z"
          fill={C.hair}
          stroke={OUT}
          strokeWidth={4}
          strokeLinejoin="round"
        />
        <path d="M-58 -450 Q-62 -430 -56 -420" stroke={C.hair} strokeWidth={10} strokeLinecap="round" fill="none" />
        <path d="M58 -452 Q62 -432 56 -424" stroke={C.hair} strokeWidth={10} strokeLinecap="round" fill="none" />
        {expr === "sweat" ? (
          <path d="M66 -478 Q60 -462 66 -456 Q72 -462 66 -478 Z" fill="#8FD3FF" stroke={OUT} strokeWidth={3} />
        ) : null}
      </g>
      {handL ? <g transform={`translate(${L.H[0]} ${L.H[1]})`}>{handL}</g> : null}
      {handR ? <g transform={`translate(${R.H[0]} ${R.H[1]})`}>{handR}</g> : null}
    </g>
  );
};

export const ANSH_POSES = {
  idle: { armL: [10, 6] as Arm, armR: [10, 6] as Arm },
  scratch: { armL: [10, 6] as Arm, armR: [150, 245] as Arm },
  shrug: { armL: [60, 150] as Arm, armR: [60, 150] as Arm },
  phone: { armL: [10, 6] as Arm, armR: [30, 205] as Arm },
};
