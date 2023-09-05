import {
    ConsoleLogger,
    createDuckDB,
    DuckDBDataProtocol,
    NODE_RUNTIME
} from '@duckdb/duckdb-wasm/dist/duckdb-node-blocking';
import type { DuckDBConnection } from '@duckdb/duckdb-wasm/dist/duckdb-node-blocking';
import type { DuckDBNodeBindings } from '@duckdb/duckdb-wasm/dist/types/src/bindings/bindings_node_base';
import { dirname, resolve } from 'path';

const DUCKDB_DIST = dirname(require.resolve('@duckdb/duckdb-wasm'));

/**
 * Adds a new view to the database, pointing to the provided parquet URLs.
 */
function setParquetURLs(connection: DuckDBConnection, urls: Record<string, string[]>) {
    for (const source in urls) {
        connection.query(`CREATE SCHEMA IF NOT EXISTS ${source};`);
        for (const url of urls[source]) {
            const table = url.split('/').at(-1)!.slice(0, -'.parquet'.length);
            const file_name = `${source}_${table}.parquet`;
            db.registerFileURL(file_name, url, DuckDBDataProtocol.NODE_FS, false);
            connection.query(
                `CREATE OR REPLACE VIEW ${source}.${table} AS SELECT * FROM read_parquet('${file_name}');`
            );
        }
    }
}

/**
 * Updates the duckdb search path to include only the list of included schemas
 */
function updateSearchPath(connection: DuckDBConnection, schemas: string[]): void {
    connection.query(`PRAGMA search_path='${schemas.join(',')}'`);
}

let db: DuckDBNodeBindings;
export async function initDB(): Promise<void> {
    try {
        const DUCKDB_BUNDLES = {
            mvp: {
                mainModule: resolve(DUCKDB_DIST, './duckdb-mvp.wasm'),
                mainWorker: resolve(DUCKDB_DIST, './duckdb-node-mvp.worker.cjs')
            },
            eh: {
                mainModule: resolve(DUCKDB_DIST, './duckdb-eh.wasm'),
                mainWorker: resolve(DUCKDB_DIST, './duckdb-node-eh.worker.cjs')
            }
        };
        const logger = new ConsoleLogger();

        // and synchronous database
        db = await createDuckDB(DUCKDB_BUNDLES, logger, NODE_RUNTIME);
        await db.instantiate();
        db.open({ query: { castBigIntToDouble: true, castTimestampToDate: true } });
    } catch (e) {
        throw e;
    }
}

/**
 * Initializes the database.
 */
export function getConnection(parquet_urls: Record<string, string[]>): DuckDBConnection | null {
    if (!db) {
        return null;
    }

    const connection = db.connect();

    setParquetURLs(connection, parquet_urls);
    updateSearchPath(connection, Object.keys(parquet_urls));

    return connection;
}

// generate with DESCRIBE SELECT column_type FROM test_all_types();
type DuckDBColumnType =
    | `STRUCT(${string})`
    | `${string}[]`
    | 'BIGINT'
    | 'BIT'
    | 'BOOLEAN'
    | 'BLOB'
    | 'DATE'
    | 'DOUBLE'
    | `DECIMAL(${number},${number})`
    | 'HUGEINT'
    | 'INTEGER'
    | 'INTERVAL'
    | 'FLOAT'
    | 'SMALLINT'
    | 'TIME'
    | 'TIMESTAMP'
    | 'TIMESTAMP_S'
    | 'TIMESTAMP_MS'
    | 'TIMESTAMP_NS'
    | 'TIME WITH TIME ZONE'
    | 'TIMESTAMP WITH TIME ZONE'
    | 'TINYINT'
    | 'UBIGINT'
    | 'UINTEGER'
    | 'USMALLINT'
    | 'UTINYINT'
    | 'UUID'
    | 'VARCHAR';

function is_object_type(
    column_type: DuckDBColumnType
): column_type is `STRUCT(${string})` | `${string}[]` {
    return column_type.startsWith('STRUCT') || column_type.endsWith('[]');
}

function is_decimal_type(
    column_type: DuckDBColumnType
): column_type is `DECIMAL(${number},${number})` {
    return column_type.startsWith('DECIMAL');
}

// https://github.com/duckdb/duckdb-wasm/blob/e271f8242dfdffdf5d8071c6c76c2c48b0e1596a/lib/src/arrow_type_mapping.cc#L130
function column_type_to_javascript_type(
    column_type: DuckDBColumnType
): 'number' | 'string' | 'boolean' | 'Date' | 'unknown' {
    if (is_object_type(column_type)) return 'unknown';
    if (is_decimal_type(column_type)) return 'number';

    switch (column_type) {
        case 'BOOLEAN':
            return 'boolean';
        case 'BIGINT':
        case 'DOUBLE':
        case 'FLOAT':
        case 'INTEGER':
        case 'SMALLINT':
        case 'TINYINT':
        case 'UBIGINT':
        case 'UINTEGER':
        case 'USMALLINT':
        case 'UTINYINT':
        case 'HUGEINT':
            return 'number';
        case 'UUID':
        case 'VARCHAR':
            return 'string';
        case 'DATE':
        case 'TIMESTAMP':
        case 'TIMESTAMP_S':
        case 'TIMESTAMP_MS':
        case 'TIMESTAMP_NS':
        case 'TIMESTAMP WITH TIME ZONE':
            return 'Date';

        // the badlands
        // we should probably convert these in the client library too
        case 'INTERVAL':
        // return 'Uint32Array';
        case 'TIME':
        case 'TIME WITH TIME ZONE':
        // return 'bigint';
        case 'BLOB':
        case 'BIT':
            // return 'Uint8Array';
            return 'unknown';
        default:
            // column_type should be `never`
            console.error(`Column type ${column_type} is not supported`);
            return 'unknown';
    }
}

function description_to_type(
    columns: {
        column_name: string;
        column_type: DuckDBColumnType;
    }[]
) {
    let type = '';

    for (const { column_name, column_type } of columns) {
        type += ` ${column_name}: ${column_type_to_javascript_type(column_type)}; `;
    }

    return `{ ${type} }[]`;
}

const DEFAULT_TYPE = 'Record<string, unknown>[]';

/** Generates a Typescript definition for a given SQL query */
export function getTypeDescription(connection: DuckDBConnection | null, sql_string: string) {
    if (!connection) return DEFAULT_TYPE;

    try {
        const description = connection
            .query(`DESCRIBE ${sql_string}`)
            .toArray()
            .map((column) => column.toJSON());
        return description_to_type(description);
    } catch (e) {
        console.error(e);
        return DEFAULT_TYPE;
    }
}
