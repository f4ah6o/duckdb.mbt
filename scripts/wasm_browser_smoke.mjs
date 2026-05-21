import { createServer } from "node:http";
import { createReadStream, existsSync } from "node:fs";
import { extname, normalize, resolve, sep } from "node:path";
import { chromium } from "@playwright/test";

const root = process.cwd();
const duckdbDist = resolve(root, "node_modules/@duckdb/duckdb-wasm/dist");

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

function smokeHtml() {
  return String.raw`<!doctype html>
<meta charset="utf-8">
<title>duckdb-wasm smoke</title>
<script type="importmap">
{
  "imports": {
    "apache-arrow": "/node_modules/apache-arrow/Arrow.dom.mjs",
    "tslib": "/node_modules/tslib/tslib.es6.mjs",
    "flatbuffers": "/node_modules/flatbuffers/mjs/flatbuffers.js"
  }
}
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

  const scalar = async (name, sql) => {
    try {
      const result = await conn.query(sql);
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

  const checks = [
    ["decimal", "123.45", await scalar("decimal", "SELECT (123.45::DECIMAL(10,2))::VARCHAR AS x")],
    ["interval", 1, await scalar("interval", "SELECT date_part('month', INTERVAL '1 months 2 days 3000000 microseconds')::INTEGER AS x")],
    ["blob", "DEADBEEF", await scalar("blob", "SELECT hex(from_hex('DEADBEEF')::BLOB) AS x")],
    ["list", "a,b,c", await scalar("list", "SELECT array_to_string(['a', 'b', 'c']::VARCHAR[], ',') AS x")],
    ["struct", "left", await scalar("struct", "SELECT (struct_pack(a := 'left', b := 'right')::STRUCT(a VARCHAR, b VARCHAR)).a AS x")],
    ["map", "v2", await scalar("map", "SELECT map_extract_value(map(['k1', 'k2'], ['v1', 'v2'])::MAP(VARCHAR, VARCHAR), 'k2') AS x")],
  ];

  await conn.close();
  await db.terminate();
  worker.terminate();

  for (const [name, expected, actual] of checks) {
    if (actual !== expected) {
      throw new Error(name + " expected " + expected + ", got " + actual);
    }
  }
  return checks.map(([name]) => name);
};
</script>`;
}

async function main() {
  if (!existsSync(duckdbDist)) {
    throw new Error("missing node_modules/@duckdb/duckdb-wasm/dist; run `pnpm install` first");
  }

  const server = createServer((request, response) => {
    try {
      const path = resolveSafePath(request.url ?? "/");
      if (path === null) {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        response.end(smokeHtml());
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
