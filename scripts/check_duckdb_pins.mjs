// Fails when a package.json @duckdb pin does not match the newest (last)
// entry of the corresponding version matrix in .github/workflows/ci.yml.
// The matrix `pnpm add` step is what actually exercises a version, so a bump
// that forgets the matrix update would otherwise look green while the pinned
// version is never tested.
import { readFileSync } from "node:fs";

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
const ci = readFileSync(".github/workflows/ci.yml", "utf8");

function matrixEntries(name) {
  const match = ci.match(new RegExp(`${name}:\\s*\\[([^\\]]+)\\]`));
  if (!match) {
    throw new Error(`cannot find matrix.${name} in .github/workflows/ci.yml`);
  }
  return [...match[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

const checks = [
  ["@duckdb/node-api", "duckdb_node_api"],
  ["@duckdb/duckdb-wasm", "duckdb_wasm"],
];

const failures = [];
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

if (failures.length > 0) {
  console.error(failures.join("\n"));
  process.exit(1);
}
console.log(
  "package.json @duckdb pins match the newest ci.yml matrix entries: " +
    checks.map(([dep, matrix]) => `${dep}@${pkg.dependencies[dep]}`).join(", "),
);
