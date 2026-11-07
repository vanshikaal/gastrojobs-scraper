import React from "react";
import { C } from "../theme";

export type GeetExpr = "happy" | "smile" | "dreamy" | "shocked" | "smug" | "worried" | "determined" | "grin";
/** [upper-arm angle, forearm angle] in degrees; 0 = straight down, 90 = out to the side, 180 = up. */
export type Arm = [number, number];
/** [thigh angle, shin angle]; 0 = straight down, positive = toward screen-right. */
export type Leg = [number, number];

export type GeetProps = {
  x: number;
  y: number; // ground (feet) position
  scale?: number;
  expr?: GeetExpr;
  armL?: Arm;
  armR?: Arm;
  legL?: Leg;
  legR?: Leg;
  lean?: number; // body rotation (deg) about feet
  sx?: number; // squash/stretch
  sy?: number;
  flap?: number; // dupatta flutter, -1..1
  glasses?: boolean;
  blink?: boolean;
  look?: number; // pupil offset -1..1
  headTilt?: number;
  handL?: React.ReactNode; // drawn at left hand
  handR?: React.ReactNode;
  front?: React.ReactNode; // drawn over the body (e.g. a big heart)
  flip?: boolean;
  opacity?: number;
};

const OUT = C.ink;
const rad = (d: number) => (d * Math.PI) / 180;

/** Ink-outlined thick limb (outline stroke underneath a coloured stroke). */
const Limb: React.FC<{ pts: [number, number][]; w: number; fill: string; ow?: number }> = ({ pts, w, fill, ow = 7 }) => {
  const d = pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(" ");
  return (
    <g>
      <path d={d} stroke={OUT} strokeWidth={w + ow * 2} strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <path d={d} stroke={fill} strokeWidth={w} strokeLinecap="round" strokeLinejoin="round" fill="none" />
    </g>
  );
};

export const armPoints = (side: -1 | 1, [a, b]: Arm, sx = 40, sy = -352, u = 80, f = 76) => {
  const S: [number, number] = [side * sx, sy];
  const E: [number, number] = [S[0] + side * Math.sin(rad(a)) * u, S[1] + Math.cos(rad(a)) * u];
  const Hd: [number, number] = [E[0] + side * Math.sin(rad(b)) * f, E[1] + Math.cos(rad(b)) * f];
  return { S, E, H: Hd };
};

export const legPoints = (side: -1 | 1, [a, b]: Leg, hy = -205, t = 100, s = 104) => {
  const Hp: [number, number] = [side * 18, hy];
  const K: [number, number] = [Hp[0] + Math.sin(rad(a)) * t, Hp[1] + Math.cos(rad(a)) * t];
  const F: [number, number] = [K[0] + Math.sin(rad(b)) * s, K[1] + Math.cos(rad(b)) * s];
  return { Hp, K, F };
};

const Eye: React.FC<{ x: number; y: number; big?: number; look: number; lid?: number }> = ({ x, y, big = 1, look, lid = 0 }) => (
  <g transform={`translate(${x} ${y}) scale(${big})`}>
    <ellipse rx={11} ry={13.5} fill={C.white} stroke={OUT} strokeWidth={3.5} />
    <circle cx={look * 4} cy={1.5} r={big > 1.1 ? 4.5 : 7} fill={OUT} />
    <circle cx={look * 4 + 2.5} cy={-2} r={2.2} fill={C.white} />
    {lid > 0 ? <path d={`M-13 -15 H13 V${-15 + lid * 15} H-13 Z`} fill={C.skin} stroke="none" /> : null}
    {lid > 0 ? <path d={`M-12 ${-14 + lid * 15} H12`} stroke={OUT} strokeWidth={3.5} strokeLinecap="round" /> : null}
    {/* lashes */}
    <path d="M-10 -11 L-15 -16 M10 -11 L15 -16" stroke={OUT} strokeWidth={3} strokeLinecap="round" />
  </g>
);

const Face: React.FC<{ expr: GeetExpr; blink?: boolean; look: number; glasses?: boolean }> = ({ expr, blink, look, glasses }) => {
  const ey = -440;
  const closedHappy = expr === "dreamy" || blink;
  const big = expr === "shocked" ? 1.35 : 1;
  let browL = "M-33 -463 Q-21 -473 -9 -465";
  let browR = "M9 -465 Q21 -473 33 -463";
  if (expr === "shocked") {
    browL = "M-34 -477 Q-21 -488 -8 -479";
    browR = "M8 -479 Q21 -488 34 -477";
  } else if (expr === "smug") {
    browL = "M-33 -460 Q-21 -462 -9 -460";
    browR = "M9 -470 Q22 -482 34 -472";
  } else if (expr === "worried") {
    browL = "M-33 -460 Q-22 -466 -10 -472";
    browR = "M10 -472 Q22 -466 33 -460";
  } else if (expr === "determined") {
    browL = "M-33 -470 Q-21 -466 -9 -460";
    browR = "M9 -460 Q21 -466 33 -470";
  }
  let mouth: React.ReactNode;
  if (expr === "happy" || expr === "grin") {
    mouth = (
      <g>
        <path d="M-17 -416 Q0 -388 17 -416 Z" fill="#8E1F2C" stroke={OUT} strokeWidth={3.5} strokeLinejoin="round" />
        <path d="M-14 -415 H14 L12 -410 H-12 Z" fill={C.white} />
        <path d="M-7 -399 Q0 -405 7 -399 Q0 -394 -7 -399" fill="#F36C7D" />
      </g>
    );
  } else if (expr === "shocked") {
    mouth = <ellipse cx={0} cy={-404} rx={10} ry={14} fill="#8E1F2C" stroke={OUT} strokeWidth={3.5} />;
  } else if (expr === "smug") {
    mouth = <path d="M-12 -410 Q4 -404 16 -416" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  } else if (expr === "worried") {
    mouth = <path d="M-12 -406 Q-6 -412 0 -406 Q6 -400 12 -406" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  } else if (expr === "determined") {
    mouth = <path d="M-14 -412 Q0 -400 14 -412 Z" fill="#8E1F2C" stroke={OUT} strokeWidth={3.5} strokeLinejoin="round" />;
  } else {
    mouth = <path d="M-14 -413 Q0 -400 14 -413" stroke={OUT} strokeWidth={4} fill="none" strokeLinecap="round" />;
  }
  return (
    <g>
      <ellipse cx={-34} cy={-418} rx={11} ry={6} fill={C.coral} opacity={expr === "dreamy" ? 0.75 : 0.45} />
      <ellipse cx={34} cy={-418} rx={11} ry={6} fill={C.coral} opacity={expr === "dreamy" ? 0.75 : 0.45} />
      {closedHappy ? (
        <g stroke={OUT} strokeWidth={4} strokeLinecap="round" fill="none">
          <path d={`M-30 ${ey + 2} Q-20 ${ey - 10} -10 ${ey + 2}`} />
          <path d={`M10 ${ey + 2} Q20 ${ey - 10} 30 ${ey + 2}`} />
        </g>
      ) : (
        <g>
          <Eye x={-20} y={ey} big={big} look={look} lid={expr === "smug" ? 0.45 : 0} />
          <Eye x={20} y={ey} big={big} look={look} lid={expr === "smug" ? 0.25 : 0} />
        </g>
      )}
      <g stroke={OUT} strokeWidth={4.5} strokeLinecap="round" fill="none">
        <path d={browL} />
        <path d={browR} />
      </g>
      <path d="M1 -432 Q-3 -424 3 -422" stroke={OUT} strokeWidth={3} fill="none" strokeLinecap="round" />
      {/* tiny bindi */}
      <circle cx={0} cy={-472} r={3.2} fill={C.red} />
      {mouth}
      {glasses ? (
        <g stroke={OUT} strokeWidth={3.5} fill="rgba(255,255,255,0.25)">
          <circle cx={-19} cy={-432} r={11} />
          <circle cx={19} cy={-432} r={11} />
          <path d="M-8 -433 Q0 -438 8 -433" fill="none" />
        </g>
      ) : null}
    </g>
  );
};

const Jhumka: React.FC<{ x: number; y: number; swing: number }> = ({ x, y, swing }) => (
  <g transform={`translate(${x} ${y}) rotate(${swing})`}>
    <line x1={0} y1={0} x2={0} y2={8} stroke={OUT} strokeWidth={3} />
    <path d="M-9 20 Q0 4 9 20 Z" fill={C.silver} stroke={OUT} strokeWidth={3} strokeLinejoin="round" />
    <circle cx={-6} cy={24} r={2.2} fill={C.silver} stroke={OUT} strokeWidth={1.5} />
    <circle cx={0} cy={25} r={2.2} fill={C.silver} stroke={OUT} strokeWidth={1.5} />
    <circle cx={6} cy={24} r={2.2} fill={C.silver} stroke={OUT} strokeWidth={1.5} />
  </g>
);

/**
 * Geet: doodle-style girl in a mustard kurta, red dupatta and floral salwar.
 * Local coordinates: feet at (0,0), head centre at (0,-440). ~510 units tall.
 */
export const Geet: React.FC<GeetProps> = ({
  x,
  y,
  scale = 1,
  expr = "smile",
  armL = [14, 8],
  armR = [14, 8],
  legL = [-4, -4],
  legR = [4, 4],
  lean = 0,
  sx = 1,
  sy = 1,
  flap = 0,
  glasses,
  blink,
  look = 0,
  headTilt = 0,
  handL,
  handR,
  front,
  flip,
  opacity = 1,
}) => {
  const L = armPoints(-1, armL);
  const R = armPoints(1, armR);
  const LL = legPoints(-1, legL);
  const LR = legPoints(1, legR);
  const sleeve = (A: { S: [number, number]; E: [number, number] }): [number, number][] => [
    A.S,
    [A.S[0] + (A.E[0] - A.S[0]) * 0.55, A.S[1] + (A.E[1] - A.S[1]) * 0.55],
  ];
  const bangles = (A: { E: [number, number]; H: [number, number] }) => {
    const bx = A.E[0] + (A.H[0] - A.E[0]) * 0.78;
    const by = A.E[1] + (A.H[1] - A.E[1]) * 0.78;
    const ang = (Math.atan2(A.H[1] - A.E[1], A.H[0] - A.E[0]) * 180) / Math.PI;
    return (
      <g transform={`translate(${bx} ${by}) rotate(${ang})`}>
        <rect x={-4} y={-12} width={4} height={24} rx={2} fill={C.red} stroke={OUT} strokeWidth={1.5} />
        <rect x={1} y={-12} width={4} height={24} rx={2} fill="#3C9D5D" stroke={OUT} strokeWidth={1.5} />
        <rect x={6} y={-12} width={3.5} height={24} rx={2} fill={C.mustard} stroke={OUT} strokeWidth={1.5} />
      </g>
    );
  };
  const tailEnd = 70 + flap * 40;
  const swing = flap * 14;
  return (
    <g
      transform={`translate(${x} ${y}) scale(${(flip ? -1 : 1) * scale * sx} ${scale * sy}) rotate(${lean})`}
      opacity={opacity}
    >
      {/* dupatta tail behind */}
      <path
        d={`M40 -362 C ${80 + flap * 30} -320, ${tailEnd + 10} -260, ${tailEnd + 20} ${-170 + Math.abs(flap) * 30}`}
        stroke={OUT}
        strokeWidth={46}
        strokeLinecap="round"
        fill="none"
      />
      <path
        d={`M40 -362 C ${80 + flap * 30} -320, ${tailEnd + 10} -260, ${tailEnd + 20} ${-170 + Math.abs(flap) * 30}`}
        stroke={C.red}
        strokeWidth={33}
        strokeLinecap="round"
        fill="none"
      />
      {[-12, 0, 12].map((d) => (
        <circle key={d} cx={tailEnd + 20 + d} cy={-150 + Math.abs(flap) * 30} r={5} fill={C.red} stroke={OUT} strokeWidth={2} />
      ))}
      {/* long hair behind */}
      <g transform={`rotate(${headTilt} 0 -380)`}>
        <path
          d="M-64 -452 C-70 -520 70 -520 64 -452 L74 -300 Q54 -282 34 -300 L30 -400 L-30 -400 L-34 -300 Q-54 -282 -74 -300 Z"
          fill={C.hair}
          stroke={OUT}
          strokeWidth={4}
          strokeLinejoin="round"
        />
      </g>
      {/* legs (salwar) */}
      <Limb pts={[LL.Hp, LL.K, LL.F]} w={44} fill="url(#floral)" ow={6} />
      <Limb pts={[LR.Hp, LR.K, LR.F]} w={44} fill="url(#floral)" ow={6} />
      {[LL.F, LR.F].map((F, i) => (
        <g key={i} transform={`translate(${F[0]} ${F[1] + 6})`}>
          <ellipse cx={i ? 8 : -8} cy={8} rx={22} ry={8} fill={C.sandal} stroke={OUT} strokeWidth={3.5} />
          <ellipse cx={i ? 8 : -8} cy={1} rx={15} ry={8} fill={C.skin} stroke={OUT} strokeWidth={3} />
          <rect x={-18} y={-14} width={36} height={12} rx={5} fill="#F7D96A" stroke={OUT} strokeWidth={3} />
        </g>
      ))}
      {/* kurta */}
      <path
        d="M-44 -362 Q0 -372 44 -362 L46 -300 Q40 -280 44 -262 L66 -165 Q0 -152 -66 -165 L-44 -262 Q-40 -280 -46 -300 Z"
        fill={C.mustard}
        stroke={OUT}
        strokeWidth={6}
        strokeLinejoin="round"
      />
      <path d="M-62 -182 Q0 -170 62 -182" stroke={C.white} strokeWidth={4} strokeDasharray="2 9" strokeLinecap="round" fill="none" />
      {[
        [-22, -240],
        [24, -228],
        [-4, -200],
        [0, -300],
      ].map(([bx, by], i) => (
        <g key={i} transform={`translate(${bx} ${by})`} fill={C.white} opacity={0.9}>
          <ellipse rx={3.5} ry={7} />
          <ellipse rx={3} ry={6} transform="rotate(50) translate(0 -6)" />
          <ellipse rx={3} ry={6} transform="rotate(-50) translate(0 -6)" />
        </g>
      ))}
      {/* dupatta front drape */}
      <path d="M-44 -360 Q-6 -300 46 -358" stroke={OUT} strokeWidth={32} strokeLinecap="round" fill="none" />
      <path d="M-44 -360 Q-6 -300 46 -358" stroke={C.red} strokeWidth={20} strokeLinecap="round" fill="none" />
      {/* arms */}
      {[L, R].map((A, i) => (
        <g key={i}>
          <Limb pts={[A.S, A.E, A.H]} w={13} fill={C.skin} ow={5} />
          <Limb pts={sleeve(A)} w={30} fill={C.mustard} ow={5} />
          {bangles(A)}
          <circle cx={A.H[0]} cy={A.H[1]} r={12} fill={C.skin} stroke={OUT} strokeWidth={4.5} />
        </g>
      ))}
      {/* neck + head */}
      <rect x={-11} y={-392} width={22} height={34} fill={C.skin} stroke={OUT} strokeWidth={4} />
      <g transform={`rotate(${headTilt} 0 -380)`}>
        <ellipse cx={-57} cy={-437} rx={9} ry={12} fill={C.skin} stroke={OUT} strokeWidth={4} />
        <ellipse cx={57} cy={-437} rx={9} ry={12} fill={C.skin} stroke={OUT} strokeWidth={4} />
        <ellipse cx={0} cy={-440} rx={57} ry={60} fill={C.skin} stroke={OUT} strokeWidth={5.5} />
        <Face expr={expr} blink={blink} look={look} glasses={glasses} />
        {/* hair front with centre parting */}
        <path
          d="M-60 -446 C-64 -506 -8 -512 0 -494 C8 -512 64 -506 60 -446 C50 -476 22 -488 2 -484 L-2 -484 C-22 -488 -50 -476 -60 -446 Z"
          fill={C.hair}
          stroke={OUT}
          strokeWidth={4}
          strokeLinejoin="round"
        />
        <path d="M-60 -446 Q-66 -400 -58 -360" stroke={C.hair} strokeWidth={12} strokeLinecap="round" fill="none" />
        <path d="M60 -446 Q66 -400 58 -360" stroke={C.hair} strokeWidth={12} strokeLinecap="round" fill="none" />
        <Jhumka x={-57} y={-427} swing={swing} />
        <Jhumka x={57} y={-427} swing={swing} />
      </g>
      {front}
      {handL ? <g transform={`translate(${L.H[0]} ${L.H[1]})`}>{handL}</g> : null}
      {handR ? <g transform={`translate(${R.H[0]} ${R.H[1]})`}>{handR}</g> : null}
    </g>
  );
};

/** Running pose for a given phase (radians). Returns props to spread onto <Geet>. */
export const runPose = (phase: number) => {
  const s = Math.sin(phase);
  const c = Math.cos(phase);
  return {
    legL: [32 * s, 32 * s - 40 * Math.max(0, c)] as Leg,
    legR: [-32 * s, -32 * s - 40 * Math.max(0, -c)] as Leg,
    armL: [35 + 30 * s, 100 + 25 * s] as Arm,
    armR: [35 - 30 * s, 100 - 25 * s] as Arm,
    flap: Math.sin(phase * 2) * 0.6 + 0.4,
    bob: -Math.abs(Math.sin(phase)) * 14,
    lean: 8,
  };
};

export const POSES = {
  idle: { armL: [14, 8] as Arm, armR: [14, 8] as Arm },
  cheeks: { armL: [100, 234] as Arm, armR: [100, 234] as Arm },
  clasp: { armL: [25, 240] as Arm, armR: [25, 240] as Arm },
  hips: { armL: [60, -68] as Arm, armR: [60, -68] as Arm },
  cheer: { armL: [150, 170] as Arm, armR: [150, 170] as Arm },
  wave: { armL: [14, 8] as Arm, armR: [130, 175] as Arm },
};
