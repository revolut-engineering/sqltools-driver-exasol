import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0, IExpectedResult } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import PromiseQueue from 'promise-queue'
// TODO get `parse` object from NPM once sqltools makes it available, if ever
import parse from './parse';
import { zipObject, range, Dictionary } from 'lodash';
import Exasol from './wsjsapi';
import keywordsCompletion from './keywords';
import LRUCache from 'lru-cache';
import { IClientConfig } from 'websocket';

// DriverLib type is any since the connection object obtained from Exasol is a plain JS object
type DriverLib = any;
type DriverOptions = any;

// Types returned by the driver
// Numbers are strings, because JS string to number conversion is lossy for large numbers
type QueryData = any[][];
type QueryColumn = {
  name: string
}
type QueryResultSet = {
  columns: QueryColumn[]
  numColumns: string
  numRows: string
  numRowsInMessage: string
  data: QueryData
  resultSetHandle?: string
}
type QueryResult = {
  resultType: string
  resultSet?: QueryResultSet
  rowCount?: string
}
type QueryResponse = {
  numResults: string
  results: QueryResult[]
}
type QueryFetchResult = {
  numRows: string
  data: QueryData
}

const MAX_RESULTS = 1000 // rows
const FETCH_SIZE = 4 * 1024 * 1024 // 4MB (bytes)
const QUERY_CACHE_SIZE = 100 // queries count
const QUERY_CACHE_AGE = 1000 * 60 * 10 // 10 minutes (ms)

export default class ExasolDriver extends AbstractDriver<DriverLib, DriverOptions> implements IConnectionDriver {

  queries = queries;

  private queue = new PromiseQueue(1, Infinity);

  private cache = new LRUCache({ max: QUERY_CACHE_SIZE, maxAge: QUERY_CACHE_AGE });

  // Wraps an error callback to ensure that it send an Error object, which SQLTools is expecting
  private rejectErr = (reject) => err => reject(
    err instanceof Error ?
      err :
      new Error((err instanceof String || typeof err === "string") ? err.toString() : "")
  );

  public async open() {
    if (this.connection) {
      return this.connection;
    }

    this.log.info(`Opening connection to ${this.credentials.server}:${this.credentials.port}`);

    this.connection = await new Promise<any>((resolve, reject) =>
      Exasol.call({}, // we must pass a new thisArg object each time as connection state is kept there and we might spawn multiple connections
        `ws://${this.credentials.server}:${this.credentials.port}`, this.credentials.username, this.credentials.password,
        resolve,
        this.rejectErr(reject),
        <IClientConfig>{ maxReceivedFrameSize: 2 * FETCH_SIZE })
    ).then(db =>
      new Promise((resolve, reject) =>
        db.com({
          'command': 'setAttributes', 'attributes': {
            'autocommit': this.credentials.autocommit,
            'queryTimeout': this.credentials.queryTimeout
          }
        },
          resolve,
          this.rejectErr(reject))
      ).then(() => db) // db is ready and attributes are set
    );
    this.log.info(`Connected to ${this.credentials.name}`);

    return this.connection;
  }

  private columnarToRows(rowCount: number, columns: string[], columnarData: QueryData): Dictionary<any>[] {
    return range(rowCount).map(
      index => zipObject(
        columns,
        columnarData.map(values => values[index])
      )
    )
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    this.log.info(`Closing connection to ${this.credentials.name}`);
    (await this.connection).close();
    this.connection = null;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries: string, opt = {}) => {
    const db = await this.open();
    const splitQueries = parse(queries);

    const responses: QueryResponse[] = await Promise.all<QueryResponse>(
      splitQueries.map((query) => this.queue.add(() =>
        new Promise((resolve, reject) =>
          db.com({ 'command': 'execute', 'sqlText': query }, resolve, this.rejectErr(reject))
        )
      )));

    const res: NSDatabase.IResult[] = [];
    for (let index = 0; index < responses.length; index++) {
      const result = responses[index].results[0];
      if (result.resultType === 'rowCount') {
        const message = `Query ok with ${result.rowCount} rows affected`
        this.log.info(message)
        res.push({
          cols: [],
          connId: this.getId(),
          messages: [{ date: new Date(), message: message }],
          results: [],
          query: splitQueries[index].toString(),
          requestId: opt.requestId,
          resultId: generateId(),
        });
      } else if (result.resultType === 'resultSet') {
        const columns = result.resultSet.columns.map(column => column.name)
        const queryResults = this.columnarToRows(+result.resultSet.numRowsInMessage, columns, result.resultSet.data)
        const queryResultCount = +result.resultSet.numRows
        if (result.resultSet.resultSetHandle !== undefined) {
          const handle = +result.resultSet.resultSetHandle
          const expectedResults = opt.fullResults ? queryResultCount : Math.min(MAX_RESULTS, queryResultCount)
          while (queryResults.length < expectedResults) {
            const fetchResult: QueryFetchResult = await this.queue.add(
              () => new Promise(
                (resolve, reject) => db.fetch(handle, queryResults.length, FETCH_SIZE, resolve, this.rejectErr(reject))
              )
            );
            queryResults.push(...this.columnarToRows(+fetchResult.numRows, columns, fetchResult.data))
          }
          queryResults.length = expectedResults // Truncate to a round number (1000) so it's clear not everything is there
          await this.queue.add(
            () => new Promise(
              (resolve, reject) => db.com({ 'command': 'closeResultSet', 'resultSetHandles': [handle] },
                resolve,
                this.rejectErr(reject))
            )
          );
        }
        const message = `Query ok with ${queryResultCount} results`
          + (queryResultCount == queryResults.length ? `` : `. ${queryResults.length} rows displayed.`)
        this.log.info(message)
        res.push({
          cols: columns,
          connId: this.getId(),
          messages: [{ date: new Date(), message: message }],
          results: queryResults,
          query: splitQueries[index].toString(),
          requestId: opt.requestId,
          resultId: generateId(),
          page: 0,
          total: queryResults.length,
          pageSize: queryResults.length
        })
      } else {
        throw new Error(`Invalid result type ${result}`);
      }
    }

    return res;
  }

  /**
   * Tests whether the connection works
   */
  public async testConnection() {
    await this.open();
    await this.singleQuery('SELECT 1', {});
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.cachedQuery(this.queries.fetchSchemas(), true);
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, schema: parent.schema, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, schema: parent.schema, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.cachedQuery(this.queries.fetchColumns(item as NSDatabase.ITable), true);
    }
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.childType) {
      case ContextValue.TABLE:
        return this.cachedQuery(this.queries.fetchTables(parent as NSDatabase.ISchema), true);
      case ContextValue.VIEW:
        return this.cachedQuery(this.queries.fetchViews(parent as NSDatabase.ISchema), true);
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.DATABASE:
        return this.cachedQuery(this.queries.searchSchemas({ search }));
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        if (_extraParams.database) {
          return this.cachedQuery(this.queries.searchTables({ search, schema: _extraParams.database }));
        }
      case ContextValue.COLUMN:
        if (_extraParams.tables && _extraParams.tables.length > 0) {
          return this.cachedQuery(this.queries.searchColumns({ search, tables: _extraParams.tables || [] }));
        } else {
          return [];
        }
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return keywordsCompletion;
  }

  private cachedQuery<R>(query: IExpectedResult<R>, fullResults: boolean = false): Promise<R[]> {
    const results = this.cache.get(query.toString()) as R[];
    if (results) {
      return Promise.resolve(results);
    }
    const resultsPromise = this.queryResults(query, { 'fullResults': fullResults });
    resultsPromise.then(res => this.cache.set(query.toString(), res));
    return resultsPromise;
  }
}
