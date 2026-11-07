import React from "react";
import { C, FONT_DISPLAY, FONT_HAND } from "../theme";
import { clamp, pop } from "../anim";
import { interpolate } from "remotion";

const INK = { stroke: C.ink, strokeLinecap: "round" as const, strokeLinejoin: "round" as const, fill: "none" };

/** A path that draws itself on as `p` goes 0→1. */
export const DrawPath: React.FC<{
  d: string;
  p: number;
  w?: number;
  color?: string;
  dash?: boolean;
  fill?: string;
}> = ({ d, p, w = 6, color = C.ink, dash, fill = "none" }) => (
  <g>
    <path
      d={d}
      pathLength={1}
      {...INK}
      fill={fill}
      stroke={color}
      strokeWidth={w}
      strokeDasharray={dash ? undefined : "1 1"}
      strokeDashoffset={dash ? undefined : 1 - Math.max(0, Math.min(1, p))}
      style={dash ? { strokeDasharray: "0.02 0.025" } : undefined}
      opacity={dash ? (p > 0 ? 1 : 0) : 1}
      mask={undefined}
    />
  </g>
);

/** Hand-drawn curved arrow that draws itself on. */
export const Arrow: React.FC<{
  from: [number, number];
  to: [number, number];
  bend?: number;
  p: number;
  w?: number;
  color?: string;
}> = ({ from, to, bend = 60, p, w = 6, color = C.ink }) => {
  const [x1, y1] = from;
  const [x2, y2] = to;
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const len = Math.hypot(dx, dy) || 1;
  const cx = mx - (dy / len) * bend;
  const cy = my + (dx / len) * bend;
  // direction at end of quadratic
  const ex = x2 - cx;
  const ey = y2 - cy;
  const el = Math.hypot(ex, ey) || 1;
  const ux = ex / el;
  const uy = ey / el;
  const head = 26;
  const hx1 = x2 - ux * head - uy * head * 0.6;
  const hy1 = y2 - uy * head + ux * head * 0.6;
  const hx2 = x2 - ux * head + uy * head * 0.6;
  const hy2 = y2 - uy * head - ux * head * 0.6;
  const hp = interpolate(p, [0.8, 1], [0, 1], clamp);
  return (
    <g>
      <DrawPath d={`M${x1} ${y1} Q${cx} ${cy} ${x2} ${y2}`} p={Math.min(1, p / 0.85)} w={w} color={color} />
      <path
        d={`M${hx1} ${hy1} L${x2} ${y2} L${hx2} ${hy2}`}
        {...INK}
        stroke={color}
        strokeWidth={w}
        pathLength={1}
        strokeDasharray="1 1"
        strokeDashoffset={1 - hp}
      />
    </g>
  );
};

/** Rubber stamp imprint that slams in at frame `at` (local frame passed in). */
export const Stamp: React.FC<{
  frame: number;
  at: number;
  x: number;
  y: number;
  text: string;
  sub?: string;
  rotate?: number;
  color?: string;
  size?: number;
  width?: number;
}> = ({ frame, at, x, y, text, sub, rotate = -10, color = C.red, size = 96, width }) => {
  const t = frame - at;
  if (t < 0) return null;
  const s = interpolate(t, [0, 5, 8, 12], [2.6, 0.92, 1.04, 1], clamp);
  const o = interpolate(t, [0, 3], [0, 1], clamp);
  const w = width ?? text.length * size * 0.62 + 90;
  const h = size * (sub ? 1.9 : 1.45);
  return (
    <g transform={`translate(${x} ${y}) rotate(${rotate}) scale(${s})`} opacity={o * 0.94}>
      <rect x={-w / 2} y={-h / 2} width={w} height={h} rx={16} fill={C.white} opacity={0.72} />
      <g filter="url(#grunge)">
        <rect x={-w / 2} y={-h / 2} width={w} height={h} rx={16} fill="none" stroke={color} strokeWidth={12} />
        <rect x={-w / 2 + 16} y={-h / 2 + 16} width={w - 32} height={h - 32} rx={8} fill="none" stroke={color} strokeWidth={4} />
        <text
          x={0}
          y={sub ? -size * 0.08 : size * 0.33}
          textAnchor="middle"
          fontFamily={FONT_DISPLAY}
          fontWeight={800}
          fontSize={size}
          letterSpacing={size * 0.04}
          fill={color}
        >
          {text}
        </text>
        {sub ? (
          <text x={0} y={size * 0.62} textAnchor="middle" fontFamily={FONT_DISPLAY} fontWeight={700} fontSize={size * 0.34} letterSpacing={4} fill={color}>
            {sub}
          </text>
        ) : null}
      </g>
    </g>
  );
};

/** Classic heart shape centred on (0,0), roughly size×size. */
export const heartPath = (size: number) => {
  const s = size / 100;
  return `M0 ${32 * s} C ${-8 * s} ${22 * s} ${-50 * s} ${-2 * s} ${-50 * s} ${-26 * s} C ${-50 * s} ${-52 * s} ${-14 * s} ${-60 * s} 0 ${-34 * s} C ${14 * s} ${-60 * s} ${50 * s} ${-52 * s} ${50 * s} ${-26 * s} C ${50 * s} ${-2 * s} ${8 * s} ${22 * s} 0 ${32 * s} Z`;
};

export const Heart: React.FC<{ x: number; y: number; size: number; fill?: string; rotate?: number; scale?: number; shine?: boolean }> = ({
  x,
  y,
  size,
  fill = C.red,
  rotate = 0,
  scale = 1,
  shine = true,
}) => (
  <g transform={`translate(${x} ${y}) rotate(${rotate}) scale(${scale})`}>
    <path d={heartPath(size)} fill={fill} stroke={C.ink} strokeWidth={Math.max(4, size * 0.035)} strokeLinejoin="round" />
    {shine ? (
      <path
        d={`M${-size * 0.32} ${-size * 0.32} Q ${-size * 0.3} ${-size * 0.46} ${-size * 0.16} ${-size * 0.46}`}
        stroke={C.white}
        strokeWidth={Math.max(3, size * 0.045)}
        strokeLinecap="round"
        fill="none"
        opacity={0.85}
      />
    ) : null}
  </g>
);

export const Sparkle: React.FC<{ x: number; y: number; size: number; color?: string; rotate?: number }> = ({
  x,
  y,
  size,
  color = C.mustard,
  rotate = 0,
}) => {
  const s = size;
  return (
    <path
      transform={`translate(${x} ${y}) rotate(${rotate})`}
      d={`M0 ${-s} Q ${s * 0.15} ${-s * 0.15} ${s} 0 Q ${s * 0.15} ${s * 0.15} 0 ${s} Q ${-s * 0.15} ${s * 0.15} ${-s} 0 Q ${-s * 0.15} ${-s * 0.15} 0 ${-s} Z`}
      fill={color}
      stroke={C.ink}
      strokeWidth={4}
      strokeLinejoin="round"
    />
  );
};

/** Wobbly hand-written question mark that pops in. */
export const QMark: React.FC<{ frame: number; at: number; x: number; y: number; size?: number; color?: string; rot?: number }> = ({
  frame,
  at,
  x,
  y,
  size = 90,
  color = C.ink,
  rot = 0,
}) => {
  const s = pop(frame, at);
  if (frame < at) return null;
  const wob = Math.sin((frame - at) / 4) * 8;
  return (
    <text
      x={0}
      y={0}
      transform={`translate(${x} ${y}) rotate(${rot + wob}) scale(${s})`}
      textAnchor="middle"
      fontFamily={FONT_DISPLAY}
      fontWeight={800}
      fontSize={size}
      fill={color}
      stroke={C.ink}
      strokeWidth={color === C.ink ? 0 : 3}
      paintOrder="stroke"
    >
      ?
    </text>
  );
};

/** Hand-written annotation text. */
export const Hand: React.FC<{
  x: number;
  y: number;
  children: React.ReactNode;
  size?: number;
  color?: string;
  anchor?: "start" | "middle" | "end";
  rotate?: number;
  weight?: number;
  opacity?: number;
  scale?: number;
}> = ({ x, y, children, size = 48, color = C.ink, anchor = "start", rotate = 0, weight = 700, opacity = 1, scale = 1 }) => (
  <text
    transform={`translate(${x} ${y}) rotate(${rotate}) scale(${scale})`}
    textAnchor={anchor}
    fontFamily={FONT_HAND}
    fontWeight={weight}
    fontSize={size}
    fill={color}
    opacity={opacity}
  >
    {children}
  </text>
);

/** Bold display text with a sticker-style ink outline. */
export const Display: React.FC<{
  x: number;
  y: number;
  children: React.ReactNode;
  size?: number;
  color?: string;
  outline?: string;
  ow?: number;
  anchor?: "start" | "middle" | "end";
  rotate?: number;
  scale?: number;
  opacity?: number;
  spacing?: number;
}> = ({ x, y, children, size = 90, color = C.ink, outline, ow = 10, anchor = "middle", rotate = 0, scale = 1, opacity = 1, spacing = 0 }) => (
  <text
    transform={`translate(${x} ${y}) rotate(${rotate}) scale(${scale})`}
    textAnchor={anchor}
    fontFamily={FONT_DISPLAY}
    fontWeight={800}
    fontSize={size}
    fill={color}
    stroke={outline}
    strokeWidth={outline ? ow : 0}
    strokeLinejoin="round"
    paintOrder="stroke"
    opacity={opacity}
    letterSpacing={spacing}
  >
    {children}
  </text>
);

/** Horizontal motion lines trailing behind a moving thing. */
export const SpeedLines: React.FC<{ x: number; y: number; frame: number; dir?: 1 | -1; opacity?: number }> = ({
  x,
  y,
  frame,
  dir = 1,
  opacity = 1,
}) => (
  <g opacity={opacity}>
    {[0, 1, 2, 3].map((i) => {
      const len = 70 + ((frame * 7 + i * 37) % 60);
      const yy = y + i * 46 - 70;
      const xx = x - dir * (20 + i * 18);
      return <line key={i} x1={xx} y1={yy} x2={xx - dir * len} y2={yy} stroke={C.ink} strokeWidth={6} strokeLinecap="round" />;
    })}
  </g>
);

/** Little puff cloud (dust / steam). */
export const Puff: React.FC<{ x: number; y: number; r: number; opacity?: number; fill?: string }> = ({ x, y, r, opacity = 1, fill = C.white }) => (
  <g transform={`translate(${x} ${y})`} opacity={opacity}>
    <path
      d={`M${-r} 0 a ${r * 0.55} ${r * 0.55} 0 0 1 ${r * 0.5} ${-r * 0.55} a ${r * 0.6} ${r * 0.6} 0 0 1 ${r * 1.0} ${-r * 0.1} a ${r * 0.5} ${r * 0.5} 0 0 1 ${r * 0.5} ${r * 0.65} Z`}
      fill={fill}
      stroke={C.ink}
      strokeWidth={5}
      strokeLinejoin="round"
    />
  </g>
);

/** Starburst behind a "freeze frame" hero moment. */
export const Burst: React.FC<{ x: number; y: number; r: number; spikes?: number; fill?: string; rot?: number; scale?: number }> = ({
  x,
  y,
  r,
  spikes = 16,
  fill = C.mustard,
  rot = 0,
  scale = 1,
}) => {
  const pts: string[] = [];
  for (let i = 0; i < spikes * 2; i++) {
    const a = (i / (spikes * 2)) * Math.PI * 2;
    const rr = i % 2 === 0 ? r : r * 0.72;
    pts.push(`${Math.cos(a) * rr},${Math.sin(a) * rr}`);
  }
  return (
    <polygon
      transform={`translate(${x} ${y}) rotate(${rot}) scale(${scale})`}
      points={pts.join(" ")}
      fill={fill}
      stroke={C.ink}
      strokeWidth={7}
      strokeLinejoin="round"
    />
  );
};

/** Hand-drawn check mark that draws on. */
export const Check: React.FC<{ x: number; y: number; p: number; size?: number; color?: string }> = ({ x, y, p, size = 40, color = C.green }) => (
  <g transform={`translate(${x} ${y})`}>
    <DrawPath d={`M${-size * 0.5} 0 L${-size * 0.12} ${size * 0.4} L${size * 0.6} ${-size * 0.55}`} p={p} w={size * 0.2} color={color} />
  </g>
);

/** Scribbly strike-through. */
export const Strike: React.FC<{ x1: number; x2: number; y: number; p: number; color?: string; w?: number }> = ({ x1, x2, y, p, color = C.red, w = 7 }) => {
  const mid = (x1 + x2) / 2;
  return <DrawPath d={`M${x1} ${y + 6} Q ${mid} ${y - 10} ${x2} ${y + 2} L ${x2 - 30} ${y + 10} Q ${mid} ${y} ${x1 + 20} ${y - 2}`} p={p} w={w} color={color} />;
};

/** Hand-drawn scribble underline. */
export const Underline: React.FC<{ x1: number; x2: number; y: number; p: number; color?: string; w?: number }> = ({ x1, x2, y, p, color = C.coral, w = 10 }) => {
  const mid = (x1 + x2) / 2;
  return <DrawPath d={`M${x1} ${y} Q ${mid} ${y + 14} ${x2} ${y - 4} M ${x1 + 30} ${y + 18} Q ${mid} ${y + 26} ${x2 - 20} ${y + 12}`} p={p} w={w} color={color} />;
};

/** Paper speech/thought bubble with tail pointing to (tx, ty). */
export const Bubble: React.FC<{
  x: number;
  y: number;
  w: number;
  h: number;
  tx: number;
  ty: number;
  s?: number;
  fill?: string;
  children?: React.ReactNode;
}> = ({ x, y, w, h, tx, ty, s = 1, fill = C.white, children }) => {
  const cx = x + w / 2;
  const cy = y + h / 2;
  const bx = Math.max(x + 40, Math.min(x + w - 40, tx));
  const by = ty > cy ? y + h : y;
  return (
    <g transform={`translate(${cx} ${cy}) scale(${s}) translate(${-cx} ${-cy})`}>
      <path d={`M${bx - 26} ${by} L${tx} ${ty} L${bx + 26} ${by} Z`} fill={fill} stroke={C.ink} strokeWidth={6} strokeLinejoin="round" />
      <rect x={x} y={y} width={w} height={h} rx={34} fill={fill} stroke={C.ink} strokeWidth={6} />
      <path d={`M${bx - 22} ${by + (ty > cy ? -4 : 4)} L${bx + 22} ${by + (ty > cy ? -4 : 4)}`} stroke={fill} strokeWidth={10} />
      {children}
    </g>
  );
};

/** Strip of washi tape. */
export const Tape: React.FC<{ x: number; y: number; w?: number; rot?: number; color?: string }> = ({ x, y, w = 150, rot = -8, color = C.coral }) => (
  <rect x={-w / 2} y={-22} width={w} height={44} transform={`translate(${x} ${y}) rotate(${rot})`} fill={color} opacity={0.75} stroke={C.ink} strokeWidth={3} strokeDasharray="10 6" />
);
