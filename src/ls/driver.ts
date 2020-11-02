import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import PromiseQueue from 'promise-queue'
// TODO get `parse` object from NPM once sqltools makes it available, if ever
import parse from './parse';
import { zipObject, range } from 'lodash';
import Exasol from './wsjsapi';
import keywordsCompletion from './keywords';

// DriverLib type is any since the connection object obtained from Exasol is a plain JS object
type DriverLib = any;
type DriverOptions = any;


export default class YourDriverClass extends AbstractDriver<DriverLib, DriverOptions> implements IConnectionDriver {

  queries = queries;

  private queue = new PromiseQueue(1, Infinity);

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

    console.debug(`Opening connection to ${this.credentials.server}:${this.credentials.port}`);

    this.connection = await new Promise<any>((resolve, reject) =>
      Exasol(`ws://${this.credentials.server}:${this.credentials.port}`, this.credentials.username, this.credentials.password,
        resolve,
        this.rejectErr(reject))
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
    console.debug(`Connected to ${this.credentials.host}`, this.connection);

    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();

    (await this.connection).close();
    this.connection = null;
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (queries, opt = {}) => {
    const db = await this.open();
    const splitQueries = parse(queries as string);

    const responseData: any = await this.queue.add(
      () => new Promise(
        (resolve, reject) => db.com({ 'command': 'executeBatch', 'sqlTexts': splitQueries },
          resolve,
          this.rejectErr(reject))
      )
    );
    const res = [];
    for (let index = 0; index < responseData.results.length; index++) {
      const result = responseData.results[index];
      if (result.resultType === 'rowCount') {
        res.push(<NSDatabase.IResult>{
          cols: [],
          connId: this.getId(),
          messages: [{ date: new Date(), message: `Query ok with ${result.rowCount} rows affected` }],
          results: [],
          query: splitQueries[index].toString(),
          requestId: opt.requestId,
          resultId: generateId(),
        });
      } else if (result.resultType === 'resultSet') {
        if (result.resultSet.resultSetHandle !== undefined) {
          result.resultSet = await this.queue.add(
            () => new Promise(
              (resolve, reject) => db.fetch(result.resultSet, 0, 100000, resolve, this.rejectErr(reject))
            )
          );
          await this.queue.add(
            () => new Promise(
              // resultSetHandle is expected to be a number, but `executeCommand` doesn't return a number
              (resolve, reject) => db.com({ 'command': 'closeResultSet', 'resultSetHandles': [+result.resultSet.resultSetHandle] },
                resolve,
                this.rejectErr(reject))
            )
          );
        }
        const columns = result.resultSet.columns.map(column => column.name)
        res.push(<NSDatabase.IResult>{
          cols: columns,
          connId: this.getId(),
          messages: [{ date: new Date(), message: `Query ok with ${result.resultSet.numRowsInMessage} results` }],
          results: range(result.resultSet.numRowsInMessage).map(
            index =>
              zipObject(
                columns,
                result.resultSet.data.map(columnValues => columnValues[index])
              )
          ),
          query: splitQueries[index].toString(),
          requestId: opt.requestId,
          resultId: generateId(),
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
        return this.queryResults(this.queries.fetchSchemas());
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, schema: parent.schema, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, schema: parent.schema, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchColumns(item as NSDatabase.ITable));
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
        return this.queryResults(this.queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchViews(parent as NSDatabase.ISchema));
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(itemType: ContextValue, search: string, _extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(this.queries.searchTables({ search }));
      case ContextValue.COLUMN:
        if (_extraParams.tables && _extraParams.tables.length > 0) {
          return this.queryResults(this.queries.searchColumns({ search, tables: _extraParams.tables || [] }));
        } else {
          return [];
        }
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver['getStaticCompletions'] = async () => {
    return keywordsCompletion;
  }
}
