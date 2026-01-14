const fs = require('fs');
const path = require('path');

const cases = [
  // Original cases
  {
    name: 'basic select',
    sql: "select 1 as a, NULL as b, 'duck' as c",
  },
  {
    name: 'multi row values',
    sql: "select * from (values (1, 'alpha'), (2, NULL), (3, 'gamma')) as t(id, label)",
  },
  {
    name: 'boolean and double',
    sql: 'select true as ok, false as no, 3.5 as pi',
  },
  {
    name: 'bigint extremes',
    sql: 'select cast(9223372036854775807 as BIGINT) as max_bigint, cast(-9223372036854775808 as BIGINT) as min_bigint',
  },
  {
    name: 'simple aggregate',
    sql: 'select count(*) as total, sum(i) as sum from (values (1), (2), (3)) as t(i)',
  },
  // Date/Time types
  {
    name: 'date literal',
    sql: "select DATE '2024-06-03' as dt",
  },
  {
    name: 'time literal',
    sql: "select TIME '12:34:56.789' as tm",
  },
  {
    name: 'timestamp literal',
    sql: "select TIMESTAMP '2024-06-03 12:34:56.789' as ts",
  },
  {
    name: 'epoch date arithmetic',
    sql: "select CAST('1970-01-01'::DATE - INTERVAL '1 day' AS DATE) as epoch_minus_day, CAST('1970-01-01'::DATE + INTERVAL '1 day' AS DATE) as epoch_plus_day",
  },
  // Decimal types
  {
    name: 'decimal positive',
    sql: "select CAST(123.456 AS DECIMAL(10,3)) as dec",
  },
  {
    name: 'decimal negative',
    sql: "select CAST(-999999.99 AS DECIMAL(9,2)) as neg_dec",
  },
  // Integer type variations
  {
    name: 'smallint extremes',
    sql: 'select CAST(32767 AS SMALLINT) as max_smallint, CAST(-32768 AS SMALLINT) as min_smallint',
  },
  {
    name: 'tinyint range',
    sql: 'select CAST(127 AS TINYINT) as max_tinyint, CAST(-128 AS TINYINT) as min_tinyint',
  },
  {
    name: 'integer extremes',
    sql: 'select CAST(2147483647 AS INTEGER) as max_int, CAST(-2147483648 AS INTEGER) as min_int',
  },
  // String edge cases
  {
    name: 'string escapes',
    sql: "select 'string with ''quotes''' as quoted, 'back\\\\slash' as backslash",
  },
  {
    name: 'string whitespace',
    sql: "select 'multi\nline' as newline, 'tab\there' as tab",
  },
  {
    name: 'empty strings',
    sql: "select '' as empty_string, '   ' as spaces",
  },
  // NULL variations
  {
    name: 'single null',
    sql: 'select NULL as only_null',
  },
  {
    name: 'multiple nulls',
    sql: 'select NULL as a, NULL as b, NULL as c',
  },
  {
    name: 'mixed nulls',
    sql: 'select 1 as col1, NULL as col2, 2 as col3, NULL as col4, 3 as col5',
  },
  // Multi-row patterns
  {
    name: 'range function',
    sql: 'select * from range(5)',
  },
  {
    name: 'range with expression',
    sql: 'select range, range * 2 as double_i from range(3)',
  },
  {
    name: 'range with modulo',
    sql: 'select range, range % 2 = 0 as is_even from range(4)',
  },
  // Aggregate functions
  {
    name: 'multiple aggregates',
    sql: 'select COUNT(*) as cnt, MIN(i) as min_val, MAX(i) as max_val, AVG(i) as avg_val from (values (1), (5), (10)) as t(i)',
  },
  {
    name: 'sum aggregate',
    sql: 'select SUM(i) as total from (values (1), (2), (3), (4), (5)) as t(i)',
  },
  // Boolean operations
  {
    name: 'boolean logic',
    sql: 'select true and false as and_result, true or false as or_result, not true as not_result',
  },
  {
    name: 'comparison boolean',
    sql: 'select i > 2 as greater_than from (values (1), (2), (3)) as t(i)',
  },
  // Type casting
  {
    name: 'cast str to int',
    sql: "select CAST('42' AS INTEGER) as str_to_int, CAST(3.14 AS VARCHAR) as dbl_to_str",
  },
  {
    name: 'cast null types',
    sql: 'select CAST(NULL AS VARCHAR) as null_varchar, CAST(NULL AS INTEGER) as null_int',
  },
  // Float edge cases
  {
    name: 'float special values',
    sql: "select 'NaN'::DOUBLE as nan_val, 'Infinity'::DOUBLE as inf_val, '-Infinity'::DOUBLE as neg_inf_val",
  },
  // Complex numeric
  {
    name: 'double precision',
    sql: 'select 3.14159265359 as pi, 2.71828182846 as e, 1.41421356237 as sqrt2',
  },
  // Additional NULL handling
  {
    name: 'null in values',
    sql: "select * from (values (1, NULL), (NULL, 2), (3, 3)) as t(a, b)",
  },
  // Case expression
  {
    name: 'case expression',
    sql: "select i, case when i < 2 then 'small' when i < 4 then 'medium' else 'large' end as size from (values (1), (2), (3), (4), (5)) as t(i)",
  },
  // Concatenation
  {
    name: 'string concatenation',
    sql: "select 'hello' || ' ' || 'world' as greeting",
  },
  // Coalesce
  {
    name: 'coalesce function',
    sql: "select coalesce(NULL, 'first') as a, coalesce(NULL, NULL, 'fallback') as b, coalesce('value', 'fallback') as c",
  },
];

function toCell(value) {
  if (value === null || value === undefined) {
    return { value: '', isNull: true };
  }
  if (typeof value === 'string') {
    return { value, isNull: false };
  }
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return { value: String(value), isNull: false };
  }
  return { value: JSON.stringify(value), isNull: false };
}

function escapeMoonbitString(value) {
  return value
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}

function mbtString(value) {
  return `"${escapeMoonbitString(value)}"`;
}

function formatStringArray(values) {
  return `[${values.map(mbtString).join(', ')}]`;
}

function formatBoolArray(values) {
  return `[${values.map((value) => (value ? 'true' : 'false')).join(', ')}]`;
}

function formatStringMatrix(values) {
  return `[${values.map(formatStringArray).join(', ')}]`;
}

function formatBoolMatrix(values) {
  return `[${values.map(formatBoolArray).join(', ')}]`;
}

function formatCase(caseData) {
  return [
    '  {',
    `    name: ${mbtString(caseData.name)},`,
    `    sql: ${mbtString(caseData.sql)},`,
    `    columns: ${formatStringArray(caseData.columns)},`,
    `    rows: ${formatStringMatrix(caseData.rows)},`,
    `    nulls: ${formatBoolMatrix(caseData.nulls)},`,
    '  },',
  ].join('\n');
}

async function main() {
  const api = await import('@duckdb/node-api');
  const instance = await api.DuckDBInstance.create(':memory:');
  const connection = await instance.connect();
  const fixtures = [];

  try {
    for (const entry of cases) {
      const result = await connection.run(entry.sql);
      const columns = result.columnNames();
      const rowsJson = await result.getRowsJson();
      const rows = [];
      const nulls = [];

      for (const row of rowsJson) {
        const values = Array.isArray(row) ? row : columns.map((name) => row[name]);
        const rowValues = [];
        const rowNulls = [];
        for (const value of values) {
          const cell = toCell(value);
          rowValues.push(cell.value);
          rowNulls.push(cell.isNull);
        }
        rows.push(rowValues);
        nulls.push(rowNulls);
      }

      fixtures.push({
        name: entry.name,
        sql: entry.sql,
        columns,
        rows,
        nulls,
      });
    }
  } finally {
    if (connection && typeof connection.close === 'function') {
      await connection.close();
    }
    if (connection && typeof connection.closeSync === 'function') {
      connection.closeSync();
    }
    if (instance && typeof instance.close === 'function') {
      await instance.close();
    }
  }

  const outputPath = path.join(__dirname, '..', 'duckdb_fixture_cases.mbt');
  const lines = [
    '// Generated by scripts/generate_duckdb_fixtures.js. Do not edit by hand.',
    '///|',
    'pub let fixture_cases : Array[FixtureCase] = [',
    fixtures.map(formatCase).join('\n'),
    ']',
    '',
  ];

  fs.writeFileSync(outputPath, lines.join('\n'), 'utf8');
  console.log(`Wrote ${outputPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
