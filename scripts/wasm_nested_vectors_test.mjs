// Browser-side test for the WASM emission path in
// src/duckdb_vector_js.mbt's `js_stream_next_vectors_ffi` (`runWasm`).
// Serves the repo's node_modules + the extracted extern bodies, drives a
// real duckdb-wasm connection in Chromium, and asserts the emitted slot
// tables (kinds/shapes/names/offsets/slotRows/validity + value pools),
// not just "no error". Mirrors the decoding contract of
// `js_vector_from_pools` so the assertions validate the real wire format.

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

const ffiFiles = new Map([
  ["js", "src/duckdb_js.mbt"],
  ["vector", "src/duckdb_vector_js.mbt"],
]);

// Extract an extern "js" body (a #-prefixed arrow-function expression)
// the same way the wbtests do.
function extractExternBody(file, name) {
  const source = readFileSync(resolve(root, file), "utf8");
  const pattern = 'extern "js" fn ' + name + "[\\s\\S]*?=\\n((?:\\s*#\\|.*\\n)+)";
  const match = source.match(new RegExp(pattern));
  if (!match) {
    throw new Error(`missing js ffi: ${name} in ${file}`);
  }
  return match[1]
    .split(/\r?\n/)
    .filter((line) => line.trim().startsWith("#|"))
    .map((line) => line.replace(/^\s*#\|\s?/, ""))
    .join("\n");
}

function resolveSafePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  if (decoded === "/" || decoded === "/nested.html") {
    return null;
  }
  const normalized = normalize(decoded).replace(/^(\.\.(\/|\\|$))+/, "");
  const fullPath = resolve(root, normalized.slice(1));
  if (fullPath !== root && !fullPath.startsWith(root + sep)) {
    throw new Error(`refusing to serve path outside repo: ${decoded}`);
  }
  return fullPath;
}

function testHtml(importMap) {
  return String.raw`<!doctype html>
<meta charset="utf-8">
<title>duckdb-wasm nested vectors</title>
<script type="importmap">
${JSON.stringify({ imports: importMap }, null, 2)}
</script>
<script type="module">
globalThis.runWasmNestedVectors = async () => {
  if (typeof Worker === "undefined") {
    throw new Error("browser Worker support is unavailable");
  }

  const duckdb = await import("/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser.mjs");
  const logger = new duckdb.ConsoleLogger();
  const worker = new Worker("/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js");
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate("/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm");
  const conn = await db.connect();

  // Real extern bodies, served as modules by /ffi/<file>/<name>.
  const ffi = async (file, name) => (await import("/ffi/" + file + "/" + name)).default;
  const js_query_stream = await ffi("js", "js_query_stream");
  const js_stream_close = await ffi("js", "js_stream_close");
  const js_close = await ffi("js", "js_close");
  const js_stream_next_vectors_ffi = await ffi("vector", "js_stream_next_vectors_ffi");

  const connObj = { kind: "wasm", db, conn, worker };
  const failures = [];
  const checks = [];

  const nextVectors = (stream) => new Promise((resolve, reject) => {
    js_stream_next_vectors_ffi(stream,
      (columns, typeIds, kinds, metas, offsets, slotRows, flags, shapes,
       shapeOffsets, names, nameOffsets, rowCount, i32, i64, f64, str, bytes) => {
        resolve({ columns, typeIds, kinds, metas, offsets, slotRows, flags,
                  shapes, shapeOffsets, names, nameOffsets, rowCount,
                  i32, i64, f64, str, bytes });
      }, (err) => reject(new Error(err && err.message ? err.message : String(err))));
  });

  const drain = async (sql) => {
    const stream = await new Promise((res, rej) => js_query_stream(connObj, sql, res, rej));
    const chunks = [];
    for (;;) {
      const c = await nextVectors(stream);
      if (c.columns.length === 0) break;
      chunks.push(c);
    }
    await new Promise((res, rej) => js_stream_close(stream, res, rej));
    return chunks;
  };

  // ---- slot-table decoder (mirrors js_vector_from_pools) ---------------
  const asNum = (x) => typeof x === "bigint" ? Number(x) : x;

  function decodeChunk(ch) {
    const kinds = ch.kinds.map(asNum);
    const slotRows = ch.slotRows.map(asNum);
    const offsets = ch.offsets.map(asNum);
    const flags = ch.flags.map(asNum);
    const shapes = ch.shapes.map(asNum);
    const shapeOffsets = ch.shapeOffsets.map(asNum);
    const nameOffsets = ch.nameOffsets.map(asNum);
    const names = ch.names;
    const i32 = ch.i32.map(asNum);
    const i64 = ch.i64.map((x) => BigInt(x));
    const f64 = ch.f64.map(asNum);
    const str = ch.str;
    const bytes = ch.bytes;

    const flagBase = [];
    {
      let acc = 0;
      for (let s = 0; s < kinds.length; s++) { flagBase[s] = acc; acc += slotRows[s]; }
    }
    const shapeOf = (s) => {
      const at = shapeOffsets[s];
      if (at < 0 || at >= shapes.length) return null;
      const tag = shapes[at];
      if (tag === 1) return { tag: "list", child: shapes[at + 1] };
      if (tag === 2) {
        const n = shapes[at + 1];
        return { tag: "struct",
                 children: shapes.slice(at + 2, at + 2 + n),
                 fieldNames: names.slice(nameOffsets[s], nameOffsets[s] + n) };
      }
      if (tag === 3) return { tag: "map", keys: shapes[at + 1], values: shapes[at + 2] };
      throw new Error("bad shape tag " + tag + " at " + at);
    };
    const referenced = new Set();
    for (let s = 0; s < kinds.length; s++) {
      const sh = shapeOf(s);
      if (!sh) continue;
      if (sh.tag === "list") referenced.add(sh.child);
      if (sh.tag === "struct") sh.children.forEach((c) => referenced.add(c));
      if (sh.tag === "map") { referenced.add(sh.keys); referenced.add(sh.values); }
    }
    const top = [];
    for (let s = 0; s < kinds.length; s++) if (!referenced.has(s)) top.push(s);

    const memo = new Array(kinds.length).fill(null);
    const cellAt = (s, i) => {
      const kind = kinds[s], off = offsets[s], sh = shapeOf(s);
      if (kind === 12) { // LIST/ARRAY
        const b = Number(i64[off + 2 * i]), len = Number(i64[off + 2 * i + 1]);
        return slotValues(sh.child).slice(b, b + len);
      }
      if (kind === 13) { // STRUCT
        const obj = {};
        sh.children.forEach((c, j) => { obj[sh.fieldNames[j]] = slotValues(c)[i]; });
        return obj;
      }
      if (kind === 14) { // MAP
        const b = Number(i64[off + 2 * i]), len = Number(i64[off + 2 * i + 1]);
        const ks = slotValues(sh.keys), vs = slotValues(sh.values);
        const m = {};
        for (let j = b; j < b + len; j++) m[ks[j]] = vs[j];
        return m;
      }
      switch (kind) {
        case 0: case 1: return i32[off + i];
        case 2: case 3: return i64[off + i];
        case 4: case 5: return f64[off + i];
        case 6: case 11: return str[off + i];
        case 7: return Array.from(bytes[off + i] || []);
        case 8: case 9: return (i64[off + 2 * i] << 64n) | i64[off + 2 * i + 1];
        case 10: return { months: Number(i64[off + 3 * i]),
                          days: Number(i64[off + 3 * i + 1]),
                          micros: i64[off + 3 * i + 2] };
        default: throw new Error("bad kind " + kind);
      }
    };
    const slotValues = (s) => {
      if (memo[s]) return memo[s];
      const n = slotRows[s];
      const fl = flags.slice(flagBase[s], flagBase[s] + n);
      const vals = new Array(n);
      for (let i = 0; i < n; i++) vals[i] = fl[i] ? null : cellAt(s, i);
      memo[s] = vals;
      return vals;
    };
    const flagsOf = (s) => flags.slice(flagBase[s], flagBase[s] + slotRows[s]);
    return {
      top, kinds, slotRows, offsets, flags, shapes, shapeOffsets, names,
      nameOffsets, rowCount: ch.rowCount, columns: ch.columns,
      typeIds: ch.typeIds.map(asNum), metas: ch.metas.map(asNum),
      i32, i64, f64, str, bytes, shapeOf, flagsOf,
      values: (s) => slotValues(s),
      columnValues: ch.columns.map((name, i) => ({ name, values: slotValues(top[i]) })),
    };
  }

  const norm = (v) => JSON.parse(JSON.stringify(v, (k, x) =>
    typeof x === "bigint" ? Number(x) : x));
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const assertDeep = (label, actual, expected) => {
    if (!same(norm(actual), norm(expected))) {
      failures.push(label + ": expected " + JSON.stringify(norm(expected)) +
                    ", got " + JSON.stringify(norm(actual)));
    } else {
      checks.push(label);
    }
  };
  const assertTrue = (label, ok) => {
    if (!ok) failures.push(label + ": expected true");
    else checks.push(label);
  };
  const oneChunk = async (label, sql) => {
    const chunks = await drain(sql);
    assertTrue(label + " emits one chunk", chunks.length === 1);
    if (chunks.length !== 1) return null;
    return decodeChunk(chunks[0]);
  };

  // =========================== test cases ==============================
  const mark = (label) => console.log("case:", label);
  // (1) LIST incl. list-of-list, NULL element, NULL list, empty list.
  mark("lists");
  {
    const d = await oneChunk("lists",
      "SELECT [1,2,3]::INTEGER[] AS l, [[7],[8,9]]::INTEGER[][] AS ll," +
      " [10,null,30]::INTEGER[] AS ln, null::INTEGER[] AS lnull," +
      " []::INTEGER[] AS le FROM (VALUES (0)) t(x)");
    if (d) {
      assertDeep("lists columns", d.columns, ["l", "ll", "ln", "lnull", "le"]);
      // DFS slot order: l=0,child1; ll=2,child3,child4; ln=5,child6;
      // lnull=7,child8; le=9,child10.
      assertDeep("lists kinds", d.kinds,
                 [12, 1, 12, 12, 1, 12, 1, 12, 1, 12, 1]);
      assertDeep("lists slotRows", d.slotRows,
                 [1, 3, 1, 2, 3, 1, 3, 1, 0, 1, 0]);
      assertDeep("lists flags", d.flags,
                 [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0]);
      assertDeep("lists i64 bounds", d.i64,
                 [0, 3, 0, 2, 0, 1, 1, 2, 0, 3, 0, 0, 0, 0]);
      assertDeep("lists i32 elements", d.i32, [1, 2, 3, 7, 8, 9, 10, 0, 30]);
      assertDeep("lists shapes", d.shapes,
                 [1, 1, 1, 4, 1, 3, 1, 6, 1, 8, 1, 10]);
      assertDeep("lists decoded", d.columnValues.map((c) => c.values[0]),
                 [[1, 2, 3], [[7], [8, 9]], [10, null, 30], null, []]);
      assertDeep("lists top slots", d.top, [0, 2, 5, 7, 9]);
    }
  }

  // (2) STRUCT incl. NULL field, NULL struct row, struct-in-struct.
  mark("structs");
  {
    const d = await oneChunk("structs",
      "SELECT {'x':1,'y':null}::STRUCT(x INTEGER, y INTEGER) AS s," +
      " {'a':{'p':7,'q':8},'b':2}::STRUCT(a STRUCT(p INTEGER, q INTEGER), b INTEGER) AS ss," +
      " null::STRUCT(x INTEGER, y INTEGER) AS sn FROM (VALUES (0)) t(x)");
    if (d) {
      assertDeep("struct columns", d.columns, ["s", "ss", "sn"]);
      assertDeep("struct top slots", d.top, [0, 3, 8]);
      assertDeep("struct kinds", d.kinds,
                 [13, 1, 1, 13, 13, 1, 1, 1, 13, 1, 1]);
      assertDeep("struct names", d.names,
                 ["x", "y", "a", "b", "p", "q", "x", "y"]);
      assertDeep("struct shapes", d.shapes,
                 [2, 2, 1, 2, 2, 2, 5, 6, 2, 2, 4, 7, 2, 2, 9, 10]);
      assertDeep("struct decoded", d.columnValues.map((c) => c.values[0]),
                 [{ x: 1, y: null }, { a: { p: 7, q: 8 }, b: 2 }, null]);
      assertDeep("struct flags: s", d.flagsOf(0), [0]);
      assertDeep("struct flags: s.y", d.flagsOf(2), [1]);
      assertDeep("struct flags: sn", d.flagsOf(8), [1]);
    }
  }

  // (3) MAP incl. NULL map and empty map.
  mark("maps");
  {
    const d = await oneChunk("maps",
      "SELECT MAP {'k1':1,'k2':2} AS m, null::MAP(VARCHAR,INTEGER) AS mn," +
      " map([]::VARCHAR[], []::INTEGER[]) AS me FROM (VALUES (0)) t(x)");
    if (d) {
      assertDeep("map columns", d.columns, ["m", "mn", "me"]);
      assertDeep("map top slots", d.top, [0, 3, 6]);
      assertDeep("map kinds", d.kinds, [14, 6, 1, 14, 6, 1, 14, 6, 1]);
      assertDeep("map slotRows", d.slotRows, [1, 2, 2, 1, 0, 0, 1, 0, 0]);
      assertDeep("map shapes", d.shapes,
                 [3, 1, 2, 3, 4, 5, 3, 7, 8]);
      assertDeep("map flags: m", d.flagsOf(0), [0]);
      assertDeep("map flags: mn", d.flagsOf(3), [1]);
      assertDeep("map decoded", d.columnValues.map((c) => c.values[0]),
                 [{ k1: 1, k2: 2 }, null, {}]);
    }
  }

  // (4) Fixed-size ARRAY incl. NULL element and NULL array (multi-row).
  mark("fixed arrays");
  {
    const d = await oneChunk("fixed arrays",
      "SELECT a::INT[3] AS a FROM (VALUES ([1,2,3]::INT[3])," +
      " ([4,null,6]::INT[3]), (NULL::INT[3])) t(a)");
    if (d) {
      assertDeep("array columns", d.columns, ["a"]);
      assertDeep("array top slots", d.top, [0]);
      assertDeep("array kinds", d.kinds, [12, 1]);
      assertDeep("array slotRows", d.slotRows, [3, 9]);
      assertDeep("array flags: a", d.flagsOf(0), [0, 0, 1]);
      // null element flag inside row 2
      assertDeep("array child flags (row0-1)", d.flagsOf(1).slice(0, 6),
                 [0, 0, 0, 0, 1, 0]);
      assertDeep("array i32 head", d.i32.slice(0, 6), [1, 2, 3, 4, 0, 6]);
      assertDeep("array decoded", d.values(0), [[1, 2, 3], [4, null, 6], null]);
    }
  }

  // (5) Multi-batch stream: range(5000) produces several RecordBatches.
  mark("multi-batch");
  {
    const chunks = await drain(
      "SELECT i::INTEGER AS i, [i::INTEGER, (i+100)::INTEGER]::INTEGER[] AS l" +
      " FROM range(5000) t(i)");
    assertTrue("multi-batch: >=3 chunks", chunks.length >= 3);
    let total = 0;
    for (const c of chunks) {
      const d = decodeChunk(c);
      total += d.slotRows[0];
      const is = d.columnValues[0].values;
      const ls = d.columnValues[1].values;
      assertDeep("multi-batch row " + total + " first",
                 [is[0], ls[0]], [total - is.length, [total - is.length, total - is.length + 100]]);
      assertDeep("multi-batch row " + total + " last",
                 [is[is.length - 1], ls[ls.length - 1]],
                 [total - 1, [total - 1, total + 99]]);
      assertTrue("multi-batch list slot present", d.kinds[1] === 12);
    }
    assertDeep("multi-batch total rows", total, 5000);
  }

  // (6) INTERVAL: MONTH_DAY_NANO decode via vector.data.values.
  mark("interval");
  {
    const d = await oneChunk("interval",
      "SELECT INTERVAL '1 year 2 days 3 seconds' AS iv," +
      " INTERVAL '-2 months' AS ivn, null::INTERVAL AS ivnull" +
      " FROM (VALUES (0)) t(x)");
    // duckdb-wasm emits INTERVAL as arrow Interval MONTH_DAY_NANO
    if (d) {
      assertDeep("interval kinds", d.kinds, [10, 10, 10]);
      assertDeep("interval i64 pool", d.i64,
                 [12, 2, 3000000, -2, 0, 0, 0, 0, 0]);
      assertDeep("interval flags", d.flags, [0, 0, 1]);
      assertDeep("interval decoded", d.columnValues.map((c) => c.values[0]),
                 [{ months: 12, days: 2, micros: 3000000 },
                  { months: -2, days: 0, micros: 0 }, null]);
    }
  }

  // (7) Sliced batch: synthetic RecordBatch.slice(1, 5) keeps
  // data.offset=1 on the parents; valueOffsets are rebased, list children
  // stay whole, struct children are sliced.
  mark("sliced");
  {
    const stream = await new Promise((res, rej) => js_query_stream(connObj,
      "SELECT i::INTEGER AS i, [i::INTEGER, (i*10)::INTEGER]::INTEGER[] AS l," +
      " {'x': i::INTEGER} AS s, [1,2,3]::INT[3] AS fa" +
      " FROM range(6) t(i)", res, rej));
    const batch = (await stream.reader.next()).value;
    await new Promise((res, rej) => js_stream_close(stream, res, rej));
    const sliced = batch.slice(1, 5);
    const slicedStream = {
      kind: "wasm",
      columns: (stream.columns && stream.columns.length > 0)
        ? stream.columns : sliced.schema.fields.map((f) => f.name),
      done: false,
      reader: (() => {
        let sent = false;
        return {
          schema: sliced.schema,
          next: async () => sent ? { done: true }
                                 : (sent = true, { done: false, value: sliced }),
        };
      })(),
    };
    // Offset sanity: the sliced parents carry a nonzero data.offset and
    // the LIST column's valueOffsets window is rebased.
    const iVec = sliced.getChildAt(0);
    const lVec = sliced.getChildAt(1);
    const iOff = (Array.isArray(iVec.data) ? iVec.data[0] : iVec.data).offset;
    const lData = (Array.isArray(lVec.data) ? lVec.data[0] : lVec.data);
    assertTrue("slice: nonzero data.offset on int column", iOff === 1);
    assertTrue("slice: nonzero data.offset on list column",
               lData.offset === 1);
    const c = await nextVectors(slicedStream);
    assertTrue("slice: chunk emitted", c.columns.length === 4);
    const d = decodeChunk(c);
    assertDeep("slice columns", d.columns, ["i", "l", "s", "fa"]);
    assertDeep("slice decoded i", d.columnValues[0].values, [1, 2, 3, 4]);
    assertDeep("slice decoded l", d.columnValues[1].values,
               [[1, 10], [2, 20], [3, 30], [4, 40]]);
    assertDeep("slice decoded s", d.columnValues[2].values,
               [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
    assertDeep("slice decoded fa", d.columnValues[3].values,
               [[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3]]);
    // second read ends the synthetic stream
    const end = await nextVectors(slicedStream);
    assertTrue("slice: stream ends", end.columns.length === 0);
  }

  // (8) Empty result keeps the schema: one 0-row chunk with columns.
  mark("empty");
  {
    const chunks = await drain(
      "SELECT 1::INTEGER AS a, 'x'::VARCHAR AS b," +
      " {'k': [1]::INTEGER[]} AS s WHERE false");
    assertTrue("empty: one schema chunk", chunks.length === 1);
    if (chunks.length === 1) {
      const d = decodeChunk(chunks[0]);
      assertDeep("empty columns", d.columns, ["a", "b", "s"]);
      assertDeep("empty rowCount", d.rowCount, 0);
      assertDeep("empty kinds", d.kinds, [1, 6, 13, 12, 1]);
      assertDeep("empty slotRows", d.slotRows, [0, 0, 0, 0, 0]);
      assertDeep("empty names", d.names, ["k"]);
      assertDeep("empty shapes", d.shapes, [1, 4, 2, 1, 3]);
    }
  }

  await new Promise((res, rej) => js_close(connObj, res, rej));

  if (failures.length > 0) {
    throw new Error(failures.join("; "));
  }
  return checks;
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
      const url = request.url ?? "/";
      const ffiMatch = decodeURIComponent(url.split("?")[0]).match(/^\/ffi\/([a-z]+)\/([a-z_]+)$/);
      if (ffiMatch) {
        const file = ffiFiles.get(ffiMatch[1]);
        if (!file) {
          response.writeHead(404);
          response.end("unknown ffi file");
          return;
        }
        response.writeHead(200, { "content-type": "text/javascript; charset=utf-8" });
        response.end("export default (" + extractExternBody(file, ffiMatch[2]) + ");\n");
        return;
      }
      const path = resolveSafePath(url);
      if (path === null) {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        response.end(testHtml(importMap));
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
  const url = `http://127.0.0.1:${port}/nested.html`;

  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage();
    page.on("console", (msg) => {
      const text = msg.text();
      // duckdb-wasm's ConsoleLogger emits structured JSON blobs; skip them.
      if (!text.startsWith("{timestamp:")) {
        console.log("[page]", text);
      }
    });
    await page.goto(url);
    const checks = await page.evaluate(() => globalThis.runWasmNestedVectors());
    console.log(`duckdb-wasm nested vectors passed: ${checks.length} assertions`);
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
