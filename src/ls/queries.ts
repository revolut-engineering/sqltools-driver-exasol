import { IBaseQueries, ContextValue } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';


const SNAPSHOT_EXEC = '/*snapshot execution*/';

const describeTable: IBaseQueries['describeTable'] = undefined;

const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
${SNAPSHOT_EXEC} select
  "COLUMN_NAME" as 'label',
  "COLUMN_TYPE" as 'dataType',
  "COLUMN_IS_NULLABLE" as 'isNullable',
  "COLUMN_TABLE" as 'table',
  "COLUMN_SCHEMA" as 'schema',
  '${ContextValue.COLUMN}' as 'type',
  '${ContextValue.NO_CHILD}' as 'childType',
  'column' as 'iconName'
from
  "EXA_ALL_COLUMNS"
where
  "COLUMN_SCHEMA" = '${p => p.schema}'
  and "COLUMN_TABLE" = '${p => p.label}'
order by
  "COLUMN_ORDINAL_POSITION"
`;

const fetchSchemas: IBaseQueries['fetchSchemas'] = queryFactory`
${SNAPSHOT_EXEC} select
  "SCHEMA_NAME" as 'label',
  "SCHEMA_NAME" as 'schema',
  '${ContextValue.SCHEMA}' as 'type'
from
  "EXA_ALL_SCHEMAS"
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
select
  *
from
  "${p => p.table.schema}"."${p => p.table.label}"
order by null
limit
  ${p => p.limit || 50}
offset
  ${p => p.offset || 0};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
select
  count(1) AS 'total'
from
  "${p => p.table.schema}"."${p => p.table.label}";
`;

const fetchTables: IBaseQueries['fetchTables'] = queryFactory`
${SNAPSHOT_EXEC} select
  "TABLE_NAME" as 'label',
  "TABLE_SCHEMA" as 'schema',
  '${ContextValue.TABLE}' as 'type',
  false as 'isView'
from
  "EXA_ALL_TABLES"
where
  "TABLE_SCHEMA" = '${p => p.schema}'
`;

const fetchViews: IBaseQueries['fetchTables'] = queryFactory`
${SNAPSHOT_EXEC} select
  "VIEW_NAME" as 'label',
  "VIEW_SCHEMA" as 'schema',
  '${ContextValue.VIEW}' as 'type',
  true as 'isView'
from
  "EXA_ALL_VIEWS"
where
  "VIEW_SCHEMA" = '${p => p.schema}'
`;

// Search is used for autocomplete. In the context of Tables, Views are also relevant.
const searchTables: IBaseQueries['searchTables'] = queryFactory`
${SNAPSHOT_EXEC} select
  "VIEW_SCHEMA" || '.' || "VIEW_NAME" as 'label',
  "VIEW_SCHEMA" as 'schema',
  '${ContextValue.VIEW}' as 'type',
  true as 'isView'
from
  "EXA_ALL_VIEWS"
where
  lower("VIEW_NAME") like '%${p => p.search.toLowerCase()}%'

union all

select
  "TABLE_SCHEMA" || '.' || "TABLE_NAME" as 'label',
  "TABLE_SCHEMA" as 'schema',
  '${ContextValue.TABLE}' as 'type',
  false as 'isView'
from
  "EXA_ALL_TABLES"
where
  lower("TABLE_NAME") like '%${p => p.search.toLowerCase()}%'

`;

const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
${SNAPSHOT_EXEC} select
  "COLUMN_NAME" as 'label',
  "COLUMN_TABLE" AS 'table',
  "COLUMN_SCHEMA" AS 'schema',
  "COLUMN_TYPE" AS 'dataType',
  "COLUMN_IS_NULLABLE" AS 'isNullable',
  '${ContextValue.COLUMN}' as 'type'
from
  "EXA_ALL_COLUMNS"
where
  lower("COLUMN_NAME") like '%${p => p.search.toLowerCase()}%'
  ${p => p.tables.length
    ? ` and lower("COLUMN_TABLE") in (${p.tables.map(t => `'${t.label}'`.toLowerCase()).join(', ')})`
    : ''
  }
order by
  "COLUMN_TABLE", "COLUMN_ORDINAL_POSITION"
`;

export default {
  fetchSchemas,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchTables,
  fetchViews,
  searchTables,
  searchColumns,
  // Unused but required by the interface
  describeTable,  // Undef
}
