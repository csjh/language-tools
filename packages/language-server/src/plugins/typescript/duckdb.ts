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
