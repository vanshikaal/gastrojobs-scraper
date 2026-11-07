import { Config } from "@remotion/cli/config";

Config.setVideoImageFormat("jpeg");
Config.setJpegQuality(92);
// CRF 18 is passed on the CLI (see README) so audio-only renders still work.
Config.setCodec("h264");
Config.setPixelFormat("yuv420p");
Config.setOverwriteOutput(true);
