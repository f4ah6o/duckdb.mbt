// Keeps the DuckDB versions exercised by CI aligned with the versions the
// README advertises.
//
// Two independent drifts are possible:
// 1. package.json pins an @duckdb npm version that the newest matrix entry
//    never installs. The matrix `pnpm add` step overrides the pin, so the
//    pinned version would otherwise look green without being tested.
// 2. The README support table or ci.yml lists different native libduckdb
//    versions. Either file can be updated alone.
//
// The native matrix is also required to be ascending so "newest entry" keeps
// meaning "most recent supported release".
import { readFileSync } from "node:fs";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const ci = readFileSync(".github/workflows/ci.yml", "utf8");
const readme = readFileSync("README.mbt.md", "utf8");

function matrixEntries(name) {
  const match = ci.match(new RegExp(`${name}:\\s*\\[([^\\]]+)\\]`));
  if (!match) {
    throw new Error(`cannot find matrix.${name} in .github/workflows/ci.yml`);
  }
  return [...match[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

function parseVersion(version) {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
  if (!match) {
    throw new Error(`invalid DuckDB version '${version}' (expected x.y.z)`);
  }
  return match.slice(1).map(Number);
}

function compareVersions(left, right) {
  const a = parseVersion(left);
  const b = parseVersion(right);
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return a[i] - b[i];
    }
  }
  return 0;
}

// The Native row advertises libduckdb versions as `x.y.z` code spans, e.g.
// | Native | ... | libduckdb `1.5.6`, `2.0.0` |
function readmeNativeVersions() {
  const row = readme.match(/^\|\s*Native\s*\|.*\|\s*libduckdb\s+([^|]+)\|/m);
  if (!row) {
    throw new Error(
      "cannot find the Native row with a libduckdb version list in " +
        "README.mbt.md",
    );
  }
  const versions = [...row[1].matchAll(/`([^`]+)`/g)].map((m) => m[1]);
  if (versions.length === 0) {
    throw new Error(
      "the Native row in README.mbt.md lists no libduckdb versions",
    );
  }
  return versions;
}

const failures = [];

const checks = [
  ["@duckdb/node-api", "duckdb_node_api"],
  ["@duckdb/duckdb-wasm", "duckdb_wasm"],
];

for (const [dependency, matrix] of checks) {
  const pin = pkg.dependencies[dependency];
  const entries = matrixEntries(matrix);
  const newest = entries[entries.length - 1];
  if (pin !== newest) {
    failures.push(
      `${dependency}: package.json pins ${pin} but the newest ci.yml matrix ` +
        `entry (${matrix}) is ${newest}. Add ${pin} as the last matrix entry ` +
        "so CI exercises the pinned version, and drop the oldest entry if it " +
        "is no longer a supported version.",
    );
  }
}

const nativeMatrix = matrixEntries("duckdb");
const nativeReadme = readmeNativeVersions();

if (nativeMatrix.length === 0) {
  failures.push("the native duckdb matrix in .github/workflows/ci.yml is empty");
}
for (let i = 1; i < nativeMatrix.length; i += 1) {
  if (compareVersions(nativeMatrix[i - 1], nativeMatrix[i]) >= 0) {
    failures.push(
      `native duckdb matrix is not strictly ascending: ` +
        `${nativeMatrix[i - 1]} then ${nativeMatrix[i]}. Keep the oldest ` +
        "version first and the newest version last.",
    );
  }
}

if (nativeMatrix.join(", ") !== nativeReadme.join(", ")) {
  failures.push(
    `README.mbt.md advertises libduckdb ${nativeReadme.join(", ")} but ` +
      `ci.yml tests ${nativeMatrix.join(", ")}. Update both the Native row ` +
      "of the CI coverage table and the native matrix so they match.",
  );
}

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log(
  "package.json @duckdb pins match the newest ci.yml matrix entries: " +
    checks.map(([dep, matrix]) => `${dep}@${pkg.dependencies[dep]}`).join(", "),
);
console.log(
  `native libduckdb versions match README.mbt.md: ${nativeMatrix.join(", ")}`,
);
