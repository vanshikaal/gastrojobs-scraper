import { continueRender, delayRender, staticFile } from "remotion";

// Latin subsets copied from @fontsource (OFL) into public/fonts.
const FACES: [string, string, string][] = [
  ["Baloo 2", "600", "baloo-2-latin-600-normal.woff2"],
  ["Baloo 2", "700", "baloo-2-latin-700-normal.woff2"],
  ["Baloo 2", "800", "baloo-2-latin-800-normal.woff2"],
  ["Caveat", "600", "caveat-latin-600-normal.woff2"],
  ["Caveat", "700", "caveat-latin-700-normal.woff2"],
];

const handle = delayRender("Loading fonts");
Promise.all(
  FACES.map(async ([family, weight, file]) => {
    const face = new FontFace(family, `url(${staticFile(`fonts/${file}`)}) format('woff2')`, { weight });
    await face.load();
    document.fonts.add(face);
  }),
)
  .then(() => continueRender(handle))
  .catch((e) => {
    console.error(e);
    continueRender(handle);
  });
