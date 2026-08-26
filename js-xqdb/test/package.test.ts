import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

interface RootPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly author: string;
  readonly repository: { readonly url: string };
  readonly engines: Readonly<Record<string, string>>;
  readonly optionalDependencies: Readonly<Record<string, string>>;
  readonly exports: Readonly<Record<string, unknown>>;
  readonly scripts: Readonly<Record<string, string>>;
  readonly napi: {
    readonly binaryName: string;
    readonly targets: readonly string[];
  };
}

interface PlatformPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly author: string;
  readonly repository: { readonly url: string };
  readonly main: string;
  readonly files: readonly string[];
  readonly os: readonly string[];
  readonly cpu: readonly string[];
  readonly libc?: readonly string[];
  readonly engines: Readonly<Record<string, string>>;
}

async function readPackageMetadata<T>(relativePath: string): Promise<T> {
  const text = await readFile(new URL(relativePath, import.meta.url), "utf8");
  return JSON.parse(text) as T;
}

describe("npm package metadata", () => {
  it("uses the public package name, Node floor, napi-rs v3 targets, and generated loader build", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");

    expect(root.name).toBe("xqdb");
    expect(root.version).toBe("0.1.0");
    expect(root.author).toBe("XQDB contributors");
    expect(root.repository.url).toBe("git+https://github.com/xbbg-org/xqdb.git");
    expect(root.engines.node).toBe(">=20");
    expect(Object.keys(root.exports)).toEqual(["."]);
    expect(root.napi).toEqual({
      binaryName: "xqdb",
      targets: [
        "x86_64-pc-windows-msvc",
        "x86_64-unknown-linux-gnu",
        "aarch64-apple-darwin",
      ],
    });
    expect(root.optionalDependencies).toEqual({
      "xqdb-win32-x64-msvc": root.version,
      "xqdb-linux-x64-gnu": root.version,
      "xqdb-darwin-arm64": root.version,
    });
    expect(root.scripts["build:native"]).toContain(
      "--manifest-path ../bindings/napi-xqdb/Cargo.toml",
    );
    expect(root.scripts["build:native"]).toContain("--package-json-path ./package.json");
    expect(root.scripts["build:native"]).toContain(
      "--esm --js native.js --dts native.d.ts",
    );
  });

  it("keeps native package versions, licenses, and platform constraints synchronized", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");
    const windows = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/win32-x64-msvc/package.json",
    );
    const linux = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/linux-x64-gnu/package.json",
    );
    const darwin = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/darwin-arm64/package.json",
    );

    expect(windows).toMatchObject({
      name: "xqdb-win32-x64-msvc",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: "git+https://github.com/xbbg-org/xqdb.git" },
      main: "xqdb.win32-x64-msvc.node",
      files: ["xqdb.win32-x64-msvc.node", "LICENSE"],
      os: ["win32"],
      cpu: ["x64"],
      engines: { node: ">=20" },
    });
    expect(linux).toMatchObject({
      name: "xqdb-linux-x64-gnu",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: "git+https://github.com/xbbg-org/xqdb.git" },
      main: "xqdb.linux-x64-gnu.node",
      files: ["xqdb.linux-x64-gnu.node", "LICENSE"],
      os: ["linux"],
      cpu: ["x64"],
      libc: ["glibc"],
      engines: { node: ">=20" },
    });
    expect(darwin).toMatchObject({
      name: "xqdb-darwin-arm64",
      version: root.version,
      author: "XQDB contributors",
      repository: { url: "git+https://github.com/xbbg-org/xqdb.git" },
      main: "xqdb.darwin-arm64.node",
      files: ["xqdb.darwin-arm64.node", "LICENSE"],
      os: ["darwin"],
      cpu: ["arm64"],
      engines: { node: ">=20" },
    });

  });
});
