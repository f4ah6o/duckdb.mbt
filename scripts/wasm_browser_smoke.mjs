import { createServer } from "node:http";
import { createReadStream, existsSync, readFileSync, realpathSync } from "node:fs";
import { basename, dirname, extname, join, normalize, relative, resolve, sep } from "node:path";
import { chromium } from "@playwright/test";

const root = process.cwd();
const duckdbDist = resolve(root, "node_modules/@duckdb/duckdb-wasm/dist");

// pnpm keeps each package's dependencies next to it inside the .pnpm virtual
// store; walking realpaths finds them regardless of the installed versions.
function virtualStoreDir(packageJsonPath) {
  let dir = dirname(realpathSync(packageJsonPath));
  while (basename(dir) !== "node_modules") {
    const parent = dirname(dir);
    if (parent === dir) {
      throw new Error(`cannot locate .pnpm store for ${packageJsonPath}`);
    }
    dir = parent;
  }
  return dir;
}

function repoUrl(absPath) {
  return "/" + relative(root, absPath).split(sep).join("/");
}

function duckdbWasmPackageJson() {
  return resolve(root, "node_modules/@duckdb/duckdb-wasm/package.json");
}

function browserImportMap() {
  const wasmStore = virtualStoreDir(duckdbWasmPackageJson());
  const arrowPkgJson = join(wasmStore, "apache-arrow/package.json");
  const arrowDir = dirname(realpathSync(arrowPkgJson));
  const arrowStore = virtualStoreDir(arrowPkgJson);
  return {
    "apache-arrow": repoUrl(join(arrowDir, "Arrow.dom.mjs")),
    "tslib": repoUrl(join(arrowStore, "tslib/tslib.es6.mjs")),
    "flatbuffers": repoUrl(join(arrowStore, "flatbuffers/mjs/flatbuffers.js")),
  };
}

function duckdbWasmVersion() {
  const pkg = JSON.parse(readFileSync(duckdbWasmPackageJson(), "utf8"));
  return pkg.version;
}

const mimeTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".mjs", "text/javascript; charset=utf-8"],
  [".wasm", "application/wasm"],
  [".map", "application/json; charset=utf-8"],
]);

function resolveSafePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  if (decoded === "/" || decoded === "/smoke.html") {
    return null;
  }
  const normalized = normalize(decoded).replace(/^(\.\.(\/|\\|$))+/, "");
  const fullPath = resolve(root, normalized.slice(1));
  if (fullPath !== root && !fullPath.startsWith(root + sep)) {
    throw new Error(`refusing to serve path outside repo: ${decoded}`);
  }
  return fullPath;
}

function smokeHtml(importMap) {
  return String.raw`<!doctype html>
<meta charset="utf-8">
<title>duckdb-wasm smoke</title>
<script type="importmap">
${JSON.stringify({ imports: importMap }, null, 2)}
</script>
<script type="module">
globalThis.runDuckDBWasmSmoke = async () => {
  if (typeof Worker === "undefined") {
    throw new Error("browser Worker support is unavailable");
  }

  const duckdb = await import("/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser.mjs");
  const bundle = {
    mainModule: "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm",
    mainWorker: "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js",
  };
  const logger = new duckdb.ConsoleLogger();
  const worker = new Worker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  const conn = await db.connect();

  const scalar = async (name, sql, params) => {
    try {
      const stmt = await conn.prepare(sql);
      const result = await stmt.query(...params);
      const rows = result.toArray().map((row) => row.toJSON());
      if (rows.length !== 1 || !("x" in rows[0])) {
        throw new Error(name + ": unexpected result shape for " + sql);
      }
      return rows[0].x;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(name + ": " + message);
    }
  };

  const cases = [
    ["decimal", "123.45", "SELECT (?::DECIMAL(10,2))::VARCHAR AS x", ["123.45"]],
    ["interval", 1, "SELECT date_part('month', ?::INTERVAL)::INTEGER AS x", ["1 months 2 days 3000000 microseconds"]],
    ["blob", "DEADBEEF", "SELECT hex(?::BLOB) AS x", [new Uint8Array([0xde, 0xad, 0xbe, 0xef])]],
    ["list", "a,b,c", "SELECT array_to_string(?::VARCHAR[], ',') AS x", [["a", "b", "c"]]],
    ["struct", "left", "SELECT (?::STRUCT(a VARCHAR, b VARCHAR)).a AS x", [{ a: "left", b: "right" }]],
    ["map", "v2", "SELECT map_extract_value(?::MAP(VARCHAR, VARCHAR), 'k2') AS x", [[{ key: "k1", value: "v1" }, { key: "k2", value: "v2" }]]],
  ];

  const checks = [];
  const unsupported = new Set(["blob", "list", "struct", "map"]);
  const failures = [];
  for (const [name, expected, sql, params] of cases) {
    try {
      const actual = await scalar(name, sql, params);
      if (unsupported.has(name)) {
        failures.push(name + ": expected direct prepared params to be unsupported");
      } else {
        checks.push([name, expected, actual]);
      }
    } catch (error) {
      if (!unsupported.has(name)) {
        failures.push(name + ": " + (error instanceof Error ? error.message : String(error)));
      }
    }
  }

  await conn.close();
  await db.terminate();
  worker.terminate();

  for (const [name, expected, actual] of checks) {
    if (actual !== expected) {
      throw new Error(name + " expected " + expected + ", got " + actual);
    }
  }
  if (failures.length > 0) {
    throw new Error("unsupported direct params: " + failures.join("; "));
  }
  return checks.map(([name]) => name).concat(Array.from(unsupported).map((name) => name + "-unsupported"));
};
</script>`;
}

async function main() {
  if (!existsSync(duckdbDist)) {
    throw new Error("missing node_modules/@duckdb/duckdb-wasm/dist; run `pnpm install` first");
  }

  console.log(`@duckdb/duckdb-wasm under test: ${duckdbWasmVersion()}`);
  const importMap = browserImportMap();

  const server = createServer((request, response) => {
    try {
      const path = resolveSafePath(request.url ?? "/");
      if (path === null) {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        response.end(smokeHtml(importMap));
        return;
      }
      if (!existsSync(path)) {
        response.writeHead(404);
        response.end("not found");
        return;
      }
      response.writeHead(200, {
        "content-type": mimeTypes.get(extname(path)) ?? "application/octet-stream",
      });
      createReadStream(path).pipe(response);
    } catch (error) {
      response.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      response.end(error instanceof Error ? error.message : String(error));
    }
  });

  await new Promise((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const url = `http://127.0.0.1:${port}/smoke.html`;

  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto(url);
    const checks = await page.evaluate(() => globalThis.runDuckDBWasmSmoke());
    console.log(`duckdb-wasm browser smoke passed: ${checks.join(", ")}`);
  } finally {
    if (browser) {
      await browser.close();
    }
    await new Promise((resolveClose) => server.close(resolveClose));
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
