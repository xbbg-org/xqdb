// Writes the exact platform pins into package.json immediately before `npm pack`.
//
// The committed manifest deliberately carries no `optionalDependencies`: the pins
// name a version the registry does not serve until the natives publish, which makes
// the committed tree — and therefore the release tag — fail `npm ci`. Injecting at
// pack time keeps the tag installable while still shipping exact pins to consumers.
//
// This is not `napi prepublish`: that command validates that every npm/<target>/
// directory already contains its .node file, which is untrue in the assemble job,
// where only the packed platform tarballs are present.
//
// Safety rests on the assertion that runs after `npm pack`. The publish job declares
// `needs: [source, assemble]`, so if either this script or that assertion fails, no
// publish happens.

import { readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const rootPath = join(packageDir, "package.json");
const npmDir = join(packageDir, "npm");

const readJson = async (path) => JSON.parse(await readFile(path, "utf8"));

const root = await readJson(rootPath);
const expectedCount = root.napi?.targets?.length;
if (!expectedCount) {
  throw new Error("package.json is missing napi.targets");
}

const entries = (await readdir(npmDir, { withFileTypes: true }))
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort();

if (entries.length !== expectedCount) {
  throw new Error(
    `npm/ has ${entries.length} platform directories but napi.targets declares ${expectedCount}`,
  );
}

const optionalDependencies = {};
for (const entry of entries) {
  const manifest = await readJson(join(npmDir, entry, "package.json"));
  if (typeof manifest.name !== "string" || !manifest.name.startsWith(`${root.name}-`)) {
    throw new Error(`npm/${entry} name ${manifest.name} is not a ${root.name} platform package`);
  }
  if (manifest.version !== root.version) {
    throw new Error(
      `npm/${entry} version ${manifest.version} does not match root version ${root.version}`,
    );
  }
  optionalDependencies[manifest.name] = root.version;
}

root.optionalDependencies = optionalDependencies;
await writeFile(rootPath, `${JSON.stringify(root, null, 2)}\n`);

const pins = Object.entries(optionalDependencies)
  .map(([name, version]) => `${name}@${version}`)
  .join(", ");
process.stdout.write(`set optionalDependencies: ${pins}\n`);
