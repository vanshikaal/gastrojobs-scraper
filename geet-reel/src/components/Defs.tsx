import React from "react";
import { C } from "../theme";

/**
 * Shared SVG defs: salwar floral print, kurta motif, paper grain and the
 * "line boil" filter that makes ink wobble like hand-drawn animation.
 * `boilSeed` should change every few frames (pass a frozen value to freeze).
 */
export const Defs: React.FC<{ boilSeed: number }> = ({ boilSeed }) => (
  <defs>
    <pattern id="floral" width="44" height="44" patternUnits="userSpaceOnUse" patternTransform="rotate(12)">
      <rect width="44" height="44" fill="#FBEBD3" />
      <g transform="translate(11 11)">
        {[0, 72, 144, 216, 288].map((a) => (
          <ellipse key={a} cx="0" cy="-5" rx="3.6" ry="5.5" fill="#EC4F8E" transform={`rotate(${a})`} />
        ))}
        <circle r="2.2" fill={C.mustard} />
      </g>
      <g transform="translate(33 32)">
        {[0, 72, 144, 216, 288].map((a) => (
          <ellipse key={a} cx="0" cy="-4.5" rx="3.2" ry="5" fill="#FF8A3D" transform={`rotate(${a})`} />
        ))}
        <circle r="2" fill="#B23AA8" />
      </g>
      <circle cx="34" cy="10" r="2.2" fill="#6CB86A" />
      <circle cx="9" cy="35" r="2.2" fill="#9B6BD6" />
    </pattern>
    <pattern id="graph" width="54" height="54" patternUnits="userSpaceOnUse">
      <path d="M54 0H0V54" fill="none" stroke={C.blueLine} strokeWidth="1.4" opacity="0.35" />
    </pattern>
    <pattern id="dots" width="60" height="60" patternUnits="userSpaceOnUse">
      <circle cx="30" cy="30" r="3" fill={C.coral} opacity="0.22" />
    </pattern>
    <filter id="boil" x="-10%" y="-10%" width="120%" height="120%">
      <feTurbulence type="fractalNoise" baseFrequency="0.018" numOctaves="2" seed={boilSeed} result="n" />
      <feDisplacementMap in="SourceGraphic" in2="n" scale="5" xChannelSelector="R" yChannelSelector="G" />
    </filter>
    <filter id="grunge" x="-5%" y="-5%" width="110%" height="110%">
      <feTurbulence type="fractalNoise" baseFrequency="0.9" numOctaves="2" seed="7" result="t" />
      <feColorMatrix in="t" type="matrix" values="0 0 0 0 0  0 0 0 0 0  0 0 0 0 0  0 0 0 -1.3 1.55" result="m" />
      <feComposite in="SourceGraphic" in2="m" operator="in" />
    </filter>
    <filter id="paperGrain" x="0" y="0" width="100%" height="100%">
      <feTurbulence type="fractalNoise" baseFrequency="0.75" numOctaves="3" seed="3" />
      <feColorMatrix type="matrix" values="0 0 0 0 0.45  0 0 0 0 0.33  0 0 0 0 0.2  0 0 0 0.09 0" />
    </filter>
    <filter id="paperBlotch" x="0" y="0" width="100%" height="100%">
      <feTurbulence type="fractalNoise" baseFrequency="0.004" numOctaves="3" seed="11" />
      <feColorMatrix type="matrix" values="0 0 0 0 0.85  0 0 0 0 0.62  0 0 0 0 0.35  0 0 0 0.16 0" />
    </filter>
  </defs>
);

/** Warm cream paper with grain, soft blotches and a vignette. */
export const Paper: React.FC<{ tint?: string; pattern?: "graph" | "dots" }> = ({ tint = C.paper, pattern }) => (
  <g>
    <rect width="1080" height="1920" fill={tint} />
    {pattern ? <rect width="1080" height="1920" fill={`url(#${pattern})`} /> : null}
    <rect width="1080" height="1920" filter="url(#paperBlotch)" />
    <rect width="1080" height="1920" filter="url(#paperGrain)" />
    <radialGradient id="vig" cx="50%" cy="45%" r="75%">
      <stop offset="65%" stopColor="#000" stopOpacity="0" />
      <stop offset="100%" stopColor="#5A3A12" stopOpacity="0.18" />
    </radialGradient>
    <rect width="1080" height="1920" fill="url(#vig)" />
  </g>
);

/** Standard full-frame SVG stage for a scene. */
export const Stage: React.FC<{ boilSeed: number; children: React.ReactNode; style?: React.CSSProperties }> = ({
  boilSeed,
  children,
  style,
}) => (
  <svg viewBox="0 0 1080 1920" width="1080" height="1920" style={{ position: "absolute", inset: 0, ...style }}>
    <Defs boilSeed={boilSeed} />
    {children}
  </svg>
);

/** Boil seed changes every 4 frames (hand-drawn "on twos/fours" feel). */
export const boilOf = (frame: number) => 1 + (Math.floor(frame / 4) % 6);
