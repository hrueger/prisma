import type { D1Database, D1Result } from '@cloudflare/workers-types'
import type { DriverAdapter, Query, Queryable, Result, ResultSet, Transaction, TransactionOptions } from '@prisma/driver-adapter-utils'
import { ColumnTypeEnum, Debug, err, ok } from '@prisma/driver-adapter-utils'
import { Mutex } from 'async-mutex'

// import { getColumnTypes, mapRow } from './conversion'

const debug = Debug('prisma:driver-adapter:dq')

const LOCK_TAG = Symbol()

class D1Queryable implements Queryable {
  readonly provider = 'sqlite';

  [LOCK_TAG] = new Mutex()

  constructor(protected readonly database: D1Database) {}

  /**
   * Execute a query given as SQL, interpolating the given parameters.
   */
  async queryRaw(query: Query): Promise<Result<ResultSet>> {
    const tag = '[js::query_raw]'
    debug(`${tag} %O`, query)

    const ioResult = await this.performIO(query)

    return ioResult.map((d) => {
      const { meta, results }: { meta: {}, results: Record<string, any>[] } = d as any;
      console.log('meta', meta)
      console.log('results', results)
      return {
        columnNames: Object.keys(results[0]), // ToDo: are we sure that all rows have the same keys?
        columnTypes: Object.keys(results[0]).map((key) => ColumnTypeEnum.Text), // ToDo: infer column types correctly
        rows: results.map((row) => Object.values(row)),
      }
    })
  }

  /**
   * Execute a query given as SQL, interpolating the given parameters and
   * returning the number of affected rows.
   * Note: Queryable expects a u64, but napi.rs only supports u32.
   */
  async executeRaw(query: Query): Promise<Result<number>> {
    const tag = '[js::execute_raw]'
    debug(`${tag} %O`, query)

    return (await this.performIO(query)).map(({ meta }) => meta.changes ?? 0)
  }

  /**
   * Run a query against the database, returning the result set.
   * Should the query fail due to a connection error, the connection is
   * marked as unhealthy.
   */
  private async performIO(query: Query): Promise<Result<D1Result>> {
    const release = await this[LOCK_TAG].acquire()
    try {
      const result = await this.database
        .prepare(query.sql)
        .bind(...query.args)
        .all()
      return ok(result)
    } catch (e) {
      const error = e as Error
      debug('Error in performIO: %O', error)
      const rawCode = error['rawCode'] ?? e.cause?.['rawCode']
      if (typeof rawCode === 'number') {
        return err({
          kind: 'Sqlite',
          extendedCode: rawCode,
          message: error.message,
        })
      }
      throw error
    } finally {
      release()
    }
  }
}


class D1Transaction extends D1Queryable implements Transaction {
  constructor(readonly db: D1Database, readonly options: TransactionOptions, readonly unlockParent: () => void) {
    super(db)
  }

  async commit(): Promise<Result<void>> {
    debug(`[js::commit]`)

    try {
      // ToDo: does this work?
      await this.db.exec('COMMIT')
    } finally {
      this.unlockParent()
    }

    return ok(undefined)
  }

  async rollback(): Promise<Result<void>> {
    debug(`[js::rollback]`)

    try {
      // ToDo: does this work?
      await this.db.exec('ROLLBACK')
    } catch (error) {
      debug('error in rollback:', error)
    } finally {
      this.unlockParent()
    }

    return ok(undefined)
  }
}

export class PrismaD1 extends D1Queryable implements DriverAdapter {
  constructor(db: D1Database) {
    super(db)

  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async startTransaction(): Promise<Result<Transaction>> {
    const options: TransactionOptions = {
      usePhantomQuery: true,
    }

    const tag = '[js::startTransaction]'
    debug(`${tag} options: %O`, options)

    const release = await this[LOCK_TAG].acquire()

    try {
      return ok(new D1Transaction(this.database, options, release))
    } catch (e) {
      // note: we only release the lock if creating the transaction fails, it must stay locked otherwise,
      // hence `catch` and rethrowing the error and not `finally`.
      release()
      throw e
    }
  }
}
