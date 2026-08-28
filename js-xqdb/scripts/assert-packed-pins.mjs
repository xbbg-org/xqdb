// Asserts that a packed root tarball declares every platform package at an exact
// version. This is the gate that makes pack-time injection safe: the publish job
// declares `needs: [source, assemble]`, so failing here prevents a root package
// shipping without its optionalDependencies.
//
// Usage: node scripts/assert-packed-pins.mjs <tarball> <version>

import { execFileSync } from "node:child_process";
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const [tarball, version] = process.argv.slice(2);
if (!tarball || !version) {
  throw new Error("usage: assert-packed-pins.mjs <tarball> <version>");
}

const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const expected = readdirSync(join(packageDir, "npm"), { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => `@xbbg/xqdb-${entry.name}`)
  .sort();

if (expected.length === 0) {
  throw new Error("npm/ declares no platform packages");
}

const raw = execFileSync("tar", ["-xzOf", tarball, "package/package.json"], {
  encoding: "utf8",
  maxBuffer: 8 * 1024 * 1024,
});
const manifest = JSON.parse(raw);

if (manifest.version !== version) {
  throw new Error(`packed version ${manifest.version} is not ${version}`);
}

const actual = manifest.optionalDependencies ?? {};
const actualNames = Object.keys(actual).sort();
if (actualNames.length !== expected.length || actualNames.some((n, i) => n !== expected[i])) {
  throw new Error(
    `packed optionalDependencies ${JSON.stringify(actualNames)} does not match ${JSON.stringify(expected)}`,
  );
}

for (const name of expected) {
  if (actual[name] !== version) {
    throw new Error(`packed ${name} is ${actual[name]}, expected exactly ${version}`);
  }
}

process.stdout.write(`packed tarball pins ${expected.length} platform packages at ${version}\n`);
