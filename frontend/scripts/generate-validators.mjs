// Compile frozen contracts at build time: the browser never evaluates code strings.
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import standalone from "ajv/dist/standalone/index.js";

const base = "https://maintai.invalid/contracts/v1.3/schemas/";
const schema = (name) =>
  JSON.parse(
    readFileSync(
      new URL(
        `../../contracts/v1.3/schemas/${name}.schema.json`,
        import.meta.url,
      ),
      "utf8",
    ),
  );
const ajv = new Ajv2020({ strict: false, code: { source: true } });
addFormats(ajv);
for (const name of ["common", "business", "prediction", "features"])
  ajv.addSchema(schema(name), base + name + ".schema.json");
const runtime = schema("runtime");
const validators = {
  liveValidator: runtime.$defs.LiveSnapshot,
  eventValidator: runtime.$defs.SSEEvent,
  pageValidator: { $ref: base + "business.schema.json#/$defs/MachineList" },
  alertPageValidator: { $ref: base + "business.schema.json#/$defs/AlertList" },
  alertDetailValidator: {
    $ref: base + "business.schema.json#/$defs/AlertDetail",
  },
  transitionValidator: {
    $ref: base + "business.schema.json#/$defs/AlertTransitionResult",
  },
  feedbackValidator: {
    $ref: base + "business.schema.json#/$defs/FeedbackCreated",
  },
  detailValidator: { $ref: base + "business.schema.json#/$defs/MachineDetail" },
};
const exports = {};
for (const [name, value] of Object.entries(validators)) {
  const id = "urn:maintai:ui:" + name;
  ajv.addSchema(value, id);
  exports[name] = id;
}
const output = new URL("../node_modules/.cache/maintai/", import.meta.url);
mkdirSync(output, { recursive: true });
writeFileSync(new URL("validators.cjs", output), standalone(ajv, exports));
