import NodeUtil from 'util';
import removeFalsy from 'lodash/compact';
import each from 'lodash/each';
import forOwn from 'lodash/forOwn';
import identity from 'lodash/identity';
import isEmpty from 'lodash/isEmpty';
import isObject from 'lodash/isObject';
import reduce from 'lodash/reduce';
import set from 'lodash/set';
import uniq from 'lodash/uniq';
import { v4 as uuidv4 } from 'uuid';
import { Association, BelongsToMany, BelongsTo } from '../../associations';
import HasMany from '../../associations/has-many';
import type { DataTypeValidateOptions, DataTypeStringifyOptions } from '../../data-types';
import DataTypes from '../../data-types';
import { QueryError, ValidationError, ValidationErrorItem } from '../../errors';
import { IndexHints } from '../../index-hints';
import type { BuiltModelAttribute, ModelStatic, BuiltModelAttributes, WhereOptions } from '../../model';
import Model from '../../model';
import { Op } from '../../operators';
import type { Sequelize, Dialect } from '../../sequelize.js';
import { literal } from '../../sequelize.js';
import type { Escapable } from '../../sql-string';
import SqlString from '../../sql-string';
import type { Transaction, ISOLATION_LEVELS } from '../../transaction.js';
import type { Nullish } from '../../utils';
import {
  canTreatArrayAsAnd, Cast, Col,
  Fn, getComplexKeys, getComplexSize, getOperators, inferIndexName,
  isColString,
  isModelSubclass,
  isPlainObject, joinSQLFragments,
  Literal, mapFinderOptions,
  removeNullishValuesFromHash,
  SequelizeMethod, spliceStr, TICK_CHAR, Where,
} from '../../utils';
import * as deprecations from '../../utils/deprecations';
import type { TableName, TableNameWithSchema, IndexMethod, IndexType } from './query-interface.js';
import type { AbstractDialect } from './index.js';

// TODO: dedupe types with QueryInterface

/**
 * Use this variable instead of empty objects to avoid creating throwaway objects.
 * Must be immutable.
 */
const EMPTY_OBJECT = Object.freeze(Object.create(null));

/**
 * Options used by the {@link AbstractQueryGenerator} constructor.
 *
 * @private
 */
export type QueryGeneratorOptions = {
  sequelize: Sequelize,
  _dialect: AbstractDialect,
};

/**
 * Options for {@link AbstractQueryGenerator#insertQuery}.
 */
export type InsertQueryOptions = {
  /**
   * A function that will collect bind parameters.
   * Leave empty to use the build-in bind parameter.
   * Set to false to disable.
   */
  bindParam?: ParamBinder | false,
  returning?: boolean,
  exception?: boolean,
  searchPath?: string,
  ignoreDuplicates?: boolean,
  updateOnDuplicate?: string[],
  upsertKeys?: string[],

  omitNull?: boolean,
};

export type BulkInsertQueryOptions = {
  returning?: boolean,
  updateOnDuplicate?: string[],
  upsertKeys?: string[],
  ignoreDuplicates?: boolean,
};

export type UpdateQueryOptions = WhereBuilderOptions & {
  /**
   * A function that will collect bind parameters.
   * Leave empty to use the build-in bind parameter.
   * Set to false to disable.
   */
  bindParam?: ParamBinder | false,
  searchPath?: string,
  returning?: boolean,
  limit?: number,
  omitNull?: boolean,
  /**
   * See {@link removeNullishValuesFromHash}
   */
  allowNull?: string[],
  mapToModel?: boolean,
};

export type SelectQueryOptions = {
  limit?: number,
  subQuery?: boolean,
  hasMultiAssociation?: boolean,
  attributes?: string[],
};

export type ArithmeticQueryOptions = {
  returning?: boolean,
  omitNull?: boolean,
};

/**
 * See {@link IndexesOptions.fields}
 */
export type IndexField =
  | string
  | Literal
  | Fn
  | { name: string, collate?: string, operator?: string, length?: number, order?: 'ASC' | 'DESC' };

export type AddIndexQueryOptions = {
  // TODO: dedupe with IndexesOptions
  name?: string,
  fields: IndexField[],
  operator?: string,
  prefix?: TableName,
  type?: IndexType,
  where?: WhereOptions,
  parser?: string | null,
  unique?: boolean,
  concurrently?: boolean,
  using?: IndexMethod,
};

type WhereBuilderOptions = {
  model?: ModelStatic<any>,
  field?: BuiltModelAttribute,
  type?: DataTypes.AbstractDataType,
  prefix?: TableName | Literal,
};

/**
 * A function that receives the value of a query Bind Param, and must return the ID of the bind param (e.g. $0, $1, etc…).
 */
export type ParamBinder = (val: unknown) => string;

/**
 * Used by {@link AbstractQueryGenerator#addConstraintQuery}, {@link AbstractQueryGenerator#getConstraintSnippet}
 */
type ConstraintQueryOptions = {
  fields: Array<string | { attribute?: string, name?: string } | SequelizeMethod>,
  type: 'UNIQUE' | 'CHECK' | 'DEFAULT' | 'PRIMARY KEY' | 'FOREIGN KEY',
  name?: string,
};

type ValidateOptions = {
  isList?: boolean,
} & DataTypeValidateOptions;

/**
 * Abstract Query Generator
 *
 * @private
 */
export class AbstractQueryGenerator {
  private readonly sequelize: Sequelize;

  // TODO: rename to 'dialectName' (getter)
  private readonly dialect: Dialect;
  private readonly _dialect: AbstractDialect;

  constructor(options: QueryGeneratorOptions) {
    if (!options.sequelize) {
      throw new TypeError('QueryGenerator initialized without options.sequelize');
    }

    if (!options._dialect) {
      throw new TypeError('QueryGenerator initialized without options._dialect');
    }

    this.sequelize = options.sequelize;

    this.dialect = options._dialect.name;
    this._dialect = options._dialect;

    // wrap quoteIdentifier with common logic
    this.#initQuoteIdentifier();
  }

  // todo: rename 'globalOptions'
  get options() {
    return this.sequelize.options;
  }

  extractTableDetails(tableName: TableName, options?: { schema?: string, delimiter?: string }) {
    const isObj = isPlainObject(tableName);

    return {
      schema: (isObj && tableName.schema) || options?.schema || 'public',
      tableName: isPlainObject(tableName) ? tableName.tableName : tableName,
      delimiter: (isObj && tableName.delimiter) || options?.delimiter || '.',
    };
  }

  addSchema(param: { _schema: string, tableName?: string, name?: string, _schemaDelimiter: string }): TableNameWithSchema {
    if (!param._schema) {
      return param.tableName || param;
    }

    return {
      tableName: param.tableName || param,
      table: param.tableName || param,
      name: param.name || param,
      schema: param._schema,
      delimiter: param._schemaDelimiter || '.',
    };
  }

  dropSchema(_schemaName: string): string {
    throw new Error(`dropSchema has not been implemented in ${this.constructor.name}`);
  }

  // TODO: replace these 3 parameters with the TableName type
  describeTableQuery(tableName: string, schema: string, schemaDelimiter: string): string {
    const table = this.quoteTable(
      this.addSchema({
        tableName,
        _schema: schema,
        _schemaDelimiter: schemaDelimiter,
      }),
    );

    return `DESCRIBE ${table};`;
  }

  dropTableQuery(tableName: TableName, _options?: { cascade: boolean }): string {
    // TODO: throw if 'cascade' is not supported in this schema
    return `DROP TABLE IF EXISTS ${this.quoteTable(tableName)};`;
  }

  renameTableQuery(before: TableName, after: TableName): string {
    return `ALTER TABLE ${this.quoteTable(before)}
      RENAME TO ${this.quoteTable(after)};`;
  }

  /**
   * Returns an insert into command
   *
   * @param table
   * @param insertValues       attribute value pairs
   * @param modelAttributes
   * @param [optionsInput]
   *
   * @private
   */
  insertQuery(
    table: TableName,
    insertValues: Readonly<Record<string, any>>,
    modelAttributes: Readonly<BuiltModelAttributes<any>>,
    optionsInput: Readonly<InsertQueryOptions> = EMPTY_OBJECT,
  ): { query: string, bind: unknown[] | undefined, bindParam: ParamBinder | undefined } {
    const options = {
      ...this.options,
      ...optionsInput,
    };

    const modelAttributeMap = Object.create(null);
    const bind: unknown[] = [];
    const fields = [];
    const returningModelAttributes = [];
    const values = [];
    const quotedTable = this.quoteTable(table);
    let bindParam: ParamBinder | false = options.bindParam === undefined ? this.bindParam(bind) : options.bindParam;
    let query;
    let valueQuery = '';
    let emptyQuery = '';
    let outputFragment = '';
    let returningFragment = '';
    let identityWrapperRequired = false;
    let tmpTable = ''; // tmpTable declaration for trigger

    if (modelAttributes) {
      each(modelAttributes, (attribute, key: string) => {
        modelAttributeMap[key] = attribute;
        if (attribute.field) {
          modelAttributeMap[attribute.field] = attribute;
        }
      });
    }

    if (this._dialect.supports['DEFAULT VALUES']) {
      emptyQuery += ' DEFAULT VALUES';
    } else if (this._dialect.supports['VALUES ()']) {
      emptyQuery += ' VALUES ()';
    }

    // TODO: throw if options.returning && !this._dialect.supports.returnValues
    if (this._dialect.supports.returnValues && options.returning) {
      const returnValues = this.generateReturnValues(modelAttributes, options);

      returningModelAttributes.push(...returnValues.returnFields);
      returningFragment = returnValues.returningFragment;
      tmpTable = returnValues.tmpTable || '';
      outputFragment = returnValues.outputFragment || '';
    }

    if (this.sequelize.options.dialectOptions?.prependSearchPath || options.searchPath) {
      // Not currently supported with search path (requires output of multiple queries)
      bindParam = false;
    }

    if (this._dialect.supports.EXCEPTION && options.exception) {
      // Not currently supported with bind parameters (requires output of multiple queries)
      bindParam = false;
    }

    insertValues = removeNullishValuesFromHash(insertValues, options.omitNull);

    for (const [key, value] of Object.entries(insertValues)) {
      fields.push(this.quoteIdentifier(key));

      // SERIALS' can't be NULL in postgresql, use DEFAULT where supported
      if (modelAttributeMap && modelAttributeMap[key] && modelAttributeMap[key].autoIncrement === true && value == null) {
        if (!this._dialect.supports.autoIncrement.defaultValue) {
          fields.splice(-1, 1);
        } else if (this._dialect.supports.DEFAULT) {
          values.push('DEFAULT');
        } else {
          values.push(this.escape(null));
        }
      } else {
        if (modelAttributeMap && modelAttributeMap[key] && modelAttributeMap[key].autoIncrement === true) {
          identityWrapperRequired = true;
        }

        if (value instanceof SequelizeMethod || !bindParam) {
          values.push(this.escape(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'INSERT' }));
        } else {
          values.push(this.format(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'INSERT' }, bindParam));
        }
      }
    }

    let onDuplicateKeyUpdate = '';

    // `options.updateOnDuplicate` is the list of field names to update if a duplicate key is hit during the insert.  It
    // contains just the field names.  This option is _usually_ explicitly set by the corresponding query-interface
    // upsert function.
    // TODO: throw if updateOnDuplicate is set but dialect does not support it
    if (this._dialect.supports.inserts.updateOnDuplicate && options.updateOnDuplicate) {
      if (!options.upsertKeys) {
        throw new TypeError('upsertKeys must be provided if updateOnDuplicate is set');
      }

      if (this._dialect.supports.inserts.updateOnDuplicate === ' ON CONFLICT DO UPDATE SET') { // postgres / sqlite
        // If no conflict target columns were specified, use the primary key names from options.upsertKeys
        const conflictKeys = options.upsertKeys.map(attr => this.quoteIdentifier(attr));
        const updateKeys = options.updateOnDuplicate.map(attr => `${this.quoteIdentifier(attr)}=EXCLUDED.${this.quoteIdentifier(attr)}`);
        onDuplicateKeyUpdate = ` ON CONFLICT (${conflictKeys.join(',')})`;
        // if update keys are provided, then apply them here.  if there are no updateKeys provided, then do not try to
        // do an update.  Instead, fall back to DO NOTHING.
        onDuplicateKeyUpdate += isEmpty(updateKeys) ? ' DO NOTHING ' : ` DO UPDATE SET ${updateKeys.join(',')}`;
      } else {
        const valueKeys = options.updateOnDuplicate.map(attr => `${this.quoteIdentifier(attr)}=VALUES(${this.quoteIdentifier(attr)})`);
        // the rough equivalent to ON CONFLICT DO NOTHING in mysql, etc is ON DUPLICATE KEY UPDATE id = id
        // So, if no update values were provided, fall back to the identifier columns provided in the upsertKeys array.
        // This will be the primary key in most cases, but it could be some other constraint.
        if (isEmpty(valueKeys) && options.upsertKeys) {
          valueKeys.push(...options.upsertKeys.map(attr => `${this.quoteIdentifier(attr)}=${this.quoteIdentifier(attr)}`));
        }

        // edge case... but if for some reason there were no valueKeys, and there were also no upsertKeys... then we
        // can no longer build the requested query without a syntax error.  Let's throw something more graceful here
        // so the devs know what the problem is.
        if (isEmpty(valueKeys)) {
          throw new Error('No update values found for ON DUPLICATE KEY UPDATE clause, and no identifier fields could be found to use instead.');
        }

        onDuplicateKeyUpdate += `${this._dialect.supports.inserts.updateOnDuplicate} ${valueKeys.join(',')}`;
      }
    }

    const replacements = {
      ignoreDuplicates: options.ignoreDuplicates ? this._dialect.supports.inserts.ignoreDuplicates : '',
      onConflictDoNothing: options.ignoreDuplicates ? this._dialect.supports.inserts.onConflictDoNothing : '',
      attributes: fields.join(','),
      output: outputFragment,
      values: values.join(','),
      tmpTable,
    };

    valueQuery = `${tmpTable}INSERT${replacements.ignoreDuplicates} INTO ${quotedTable} (${replacements.attributes})${replacements.output} VALUES (${replacements.values})${onDuplicateKeyUpdate}${replacements.onConflictDoNothing}${valueQuery}`;
    emptyQuery = `${tmpTable}INSERT${replacements.ignoreDuplicates} INTO ${quotedTable}${replacements.output}${onDuplicateKeyUpdate}${replacements.onConflictDoNothing}${emptyQuery}`;

    // Mostly for internal use, so we expect the user to know what he's doing!
    // pg_temp functions are private per connection, so we never risk this function interfering with another one.
    if (this._dialect.supports.EXCEPTION && options.exception) {
      const dropFunction = 'DROP FUNCTION IF EXISTS pg_temp.testfunc()';

      if (returningModelAttributes.length === 0) {
        returningModelAttributes.push('*');
      }

      const delimiter = `$func_${uuidv4().replace(/-/g, '')}$`;
      const selectQuery = `SELECT (testfunc.response).${returningModelAttributes.join(', (testfunc.response).')}, testfunc.sequelize_caught_exception FROM pg_temp.testfunc();`;

      const exception = 'WHEN unique_violation THEN GET STACKED DIAGNOSTICS sequelize_caught_exception = PG_EXCEPTION_DETAIL;';
      valueQuery = `CREATE OR REPLACE FUNCTION pg_temp.testfunc(OUT response ${quotedTable}, OUT sequelize_caught_exception text) RETURNS RECORD AS ${delimiter} BEGIN ${valueQuery} RETURNING * INTO response; EXCEPTION ${exception} END ${delimiter} LANGUAGE plpgsql; ${selectQuery} ${dropFunction}`;
    } else {
      valueQuery += returningFragment;
      emptyQuery += returningFragment;
    }

    query = `${replacements.attributes.length > 0 ? valueQuery : emptyQuery};`;
    if (this._dialect.supports.finalTable) {
      query = `SELECT * FROM FINAL TABLE(${replacements.attributes.length > 0 ? valueQuery : emptyQuery});`;
    }

    if (identityWrapperRequired && this._dialect.supports.autoIncrement.identityInsert) {
      query = `SET IDENTITY_INSERT ${quotedTable} ON; ${query} SET IDENTITY_INSERT ${quotedTable} OFF;`;
    }

    // Used by Postgres upsertQuery and calls to here with options.exception set to true
    return {
      query,
      bind: bindParam ? bind : undefined,
      bindParam: bindParam || undefined,
    };
  }

  /**
   * Returns an insert into command for multiple values.
   *
   * @param tableName
   * @param records
   * @param options
   * @param fieldMappedAttributes
   *
   * @private
   */
  bulkInsertQuery(
    tableName: TableName,
    records: Readonly<Array<Record<string, any>>>,
    options?: Readonly<BulkInsertQueryOptions>,
    fieldMappedAttributes: Readonly<BuiltModelAttributes<any>> = EMPTY_OBJECT,
  ): string {

    const serials: { [attribute: string]: boolean } = Object.create(null);
    const allAttributes: string[] = [];

    for (const fieldValueHash of records) {
      forOwn(fieldValueHash, (value, key) => {
        if (!allAttributes.includes(key)) {
          allAttributes.push(key);
        }

        if (
          fieldMappedAttributes[key]
          && fieldMappedAttributes[key].autoIncrement === true
        ) {
          serials[key] = true;
        }
      });
    }

    const tuples: string[] = [];
    for (const fieldValueHash of records) {
      const values = allAttributes.map(key => {
        if (
          this._dialect.supports.bulkDefault
          && serials[key] === true
        ) {
          // fieldValueHashes[key] ?? 'DEFAULT'
          return fieldValueHash[key] != null ? fieldValueHash[key] : 'DEFAULT';
        }

        return this.escape(fieldValueHash[key], fieldMappedAttributes[key], { context: 'INSERT' });
      });

      tuples.push(`(${values.join(',')})`);
    }

    // `options.updateOnDuplicate` is the list of field names to update if a duplicate key is hit during the insert.  It
    // contains just the field names.  This option is _usually_ explicitly set by the corresponding query-interface
    // upsert function.
    let onDuplicateKeyUpdate = '';
    // TODO: throw if updateOnDuplicate is requested but not supported
    if (this._dialect.supports.inserts.updateOnDuplicate && options?.updateOnDuplicate) {
      if (!options.upsertKeys) {
        throw new TypeError('upsertKeys must also be provided when updateOnDuplicate is provided');
      }

      if (this._dialect.supports.inserts.updateOnDuplicate === ' ON CONFLICT DO UPDATE SET') { // postgres / sqlite
        // If no conflict target columns were specified, use the primary key names from options.upsertKeys
        const conflictKeys = options.upsertKeys.map(attr => this.quoteIdentifier(attr));
        const updateKeys = options.updateOnDuplicate.map(attr => `${this.quoteIdentifier(attr)}=EXCLUDED.${this.quoteIdentifier(attr)}`);
        onDuplicateKeyUpdate = ` ON CONFLICT (${conflictKeys.join(',')}) DO UPDATE SET ${updateKeys.join(',')}`;
      } else { // mysql / maria
        const valueKeys = options.updateOnDuplicate.map(attr => `${this.quoteIdentifier(attr)}=VALUES(${this.quoteIdentifier(attr)})`);
        onDuplicateKeyUpdate = `${this._dialect.supports.inserts.updateOnDuplicate} ${valueKeys.join(',')}`;
      }
    }

    // TODO: throw if ignoreDuplicates is requested but not supported
    const ignoreDuplicates = options?.ignoreDuplicates ? this._dialect.supports.inserts.ignoreDuplicates : '';
    const attributes = allAttributes.map(attr => this.quoteIdentifier(attr)).join(',');
    const onConflictDoNothing = options?.ignoreDuplicates ? this._dialect.supports.inserts.onConflictDoNothing : '';
    let returning = '';

    // TODO: throw if returning is requested but not supported
    if (this._dialect.supports.returnValues && options?.returning) {
      const returnValues = this.generateReturnValues(fieldMappedAttributes, options);

      returning += returnValues.returningFragment;
    }

    return joinSQLFragments([
      'INSERT',
      ignoreDuplicates,
      'INTO',
      this.quoteTable(tableName),
      `(${attributes})`,
      'VALUES',
      tuples.join(','),
      onDuplicateKeyUpdate,
      onConflictDoNothing,
      returning,
      ';',
    ]);
  }

  /**
   * Returns an update query
   *
   * @param tableName
   * @param updateValues
   * @param where A hash with conditions (e.g. {name: 'foo'}) OR an ID as integer
   * @param optionsInput
   * @param attributes
   *
   * @private
   */
  updateQuery(
    tableName: TableName,
    updateValues: Readonly<Record<string, any>>,
    where: WhereOptions,
    optionsInput: Readonly<UpdateQueryOptions> = EMPTY_OBJECT,
    attributes?: Readonly<BuiltModelAttributes<any>>,
  ): { query: string, bind?: unknown[], optionOverride?: Partial<UpdateQueryOptions> } {
    const options = {
      ...this.options,
      ...optionsInput,
    };

    updateValues = removeNullishValuesFromHash(updateValues, options.omitNull, options);

    const values = [];
    const bind: unknown[] = [];
    let bindParam = options.bindParam === undefined ? this.bindParam(bind) : options.bindParam;

    if (this.sequelize.options.dialectOptions?.prependSearchPath || options.searchPath) {
      // Not currently supported with search path (requires output of multiple queries)
      bindParam = false;
    }

    let outputFragment = '';
    let tmpTable = ''; // tmpTable declaration for trigger
    let suffix = '';

    // TODO: 'LIMIT ON UPDATE' is only used here. Why are mssql & db2 declaring they support it if they're disabled here?
    // TODO: throw if options.limit is set but LIMIT ON UPDATE is not supported.
    if (options.limit && this._dialect.supports['LIMIT ON UPDATE'] && this.dialect !== 'mssql' && this.dialect !== 'db2') {
      suffix = ` LIMIT ${this.escape(options.limit)} `;
    }

    const optionOverride: Partial<UpdateQueryOptions> = Object.create(null);

    // TODO: throw if returning requested but returnValues not supported.
    if (this._dialect.supports.returnValues && options.returning) {
      const returnValues = this.generateReturnValues(attributes, options);

      suffix += returnValues.returningFragment;
      tmpTable = returnValues.tmpTable || '';
      outputFragment = returnValues.outputFragment || '';

      // ensure that the return output is properly mapped to model fields.
      if (!this._dialect.supports.returnValues.output && options.returning) {
        optionOverride.mapToModel = true;
      }
    }

    const modelAttributeMap = Object.create(null);
    if (attributes) {
      each(attributes, (attribute, key) => {
        modelAttributeMap[key] = attribute;
        if (attribute.field) {
          modelAttributeMap[attribute.field] = attribute;
        }
      });
    }

    for (const key of Object.keys(updateValues)) {
      if (modelAttributeMap && modelAttributeMap[key]
        && modelAttributeMap[key].autoIncrement === true
        && !this._dialect.supports.autoIncrement.update) {
        // not allowed to update identity column
        // TODO: throw if trying to update the identity column but not allowed
        continue;
      }

      const value = updateValues[key];

      if (value instanceof SequelizeMethod || options.bindParam === false) {
        values.push(`${this.quoteIdentifier(key)}=${this.escape(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'UPDATE' })}`);
      } else {
        values.push(`${this.quoteIdentifier(key)}=${this.format(value, modelAttributeMap && modelAttributeMap[key] || undefined, { context: 'UPDATE' }, bindParam)}`);
      }
    }

    if (values.length === 0) {
      return { query: '', bind: undefined };
    }

    const whereOptions = { ...options, bindParam };

    const query = `${tmpTable}UPDATE ${this.quoteTable(tableName)} SET ${values.join(',')}${outputFragment} ${this.whereQuery(where, whereOptions)}${suffix}`.trim();
    // Used by Postgres upsertQuery and calls to here with options.exception set to true

    optionOverride.bindParam = bindParam;

    return {
      query,
      bind: bindParam ? bind : undefined,
      optionOverride,
    };
  }

  /**
   * Returns an update query using arithmetic operator
   *
   * @param sqlOperator                    String with the arithmetic operator (e.g. '+' or '-')
   * @param tableName                   Name of the table
   * @param where                       A plain-object with conditions (e.g. {name: 'foo'}) OR an ID as integer
   * @param incrementAmountsByAttribute     A plain-object with attribute-value-pairs
   * @param extraAttributesToBeUpdated  A plain-object with attribute-value-pairs
   * @param optionsInput
   *
   * @private
   */
  arithmeticQuery(
    sqlOperator: string,
    tableName: TableName,
    where: WhereOptions,
    incrementAmountsByAttribute: Readonly<{ [attribute: string]: Escapable }>,
    extraAttributesToBeUpdated: Readonly<{ [attribute: string]: Escapable }>,
    optionsInput: Readonly<ArithmeticQueryOptions>,
  ): string {
    // TODO: this should use 'updateQuery' internally

    const options = {
      returning: true,
      ...this.options,
      ...optionsInput,
    };

    extraAttributesToBeUpdated = removeNullishValuesFromHash(extraAttributesToBeUpdated, options.omitNull);

    let outputFragment = '';
    let returningFragment = '';

    // TODO: throw if returning is requested but not supported
    if (this._dialect.supports.returnValues && options.returning) {
      const returnValues = this.generateReturnValues(null, options);

      outputFragment = returnValues.outputFragment;
      returningFragment = returnValues.returningFragment;
    }

    const updateSetSqlFragments = [];
    for (const field of Object.keys(incrementAmountsByAttribute)) {
      const incrementAmount = incrementAmountsByAttribute[field];
      const quotedField = this.quoteIdentifier(field);
      const escapedAmount = this.escape(incrementAmount);
      updateSetSqlFragments.push(`${quotedField}=${quotedField}${sqlOperator} ${escapedAmount}`);
    }

    for (const field of Object.keys(extraAttributesToBeUpdated)) {
      const newValue = extraAttributesToBeUpdated[field];
      const quotedField = this.quoteIdentifier(field);
      const escapedValue = this.escape(newValue);
      updateSetSqlFragments.push(`${quotedField}=${escapedValue}`);
    }

    return joinSQLFragments([
      'UPDATE',
      this.quoteTable(tableName),
      'SET',
      updateSetSqlFragments.join(','),
      outputFragment,
      this.whereQuery(where),
      returningFragment,
    ]);
  }

  /**
   * Returns an add index query.
   * Parameters:
   * - tableName -> Name of an existing table, possibly with schema.
   * - options:
   * - type: UNIQUE|FULLTEXT|SPATIAL
   * - name: The name of the index. Default is <table>_<attr1>_<attr2>
   * - fields: An array of attributes as string or as hash.
   * If the attribute is a hash, it must have the following content:
   * - name: The name of the attribute/column
   * - length: An integer. Optional
   * - order: 'ASC' or 'DESC'. Optional
   * - parser
   * - using
   * - operator
   * - concurrently: Pass CONCURRENT so other operations run while the index is created
   * - rawTablename, the name of the table, without schema. Used to create the name of the index
   *
   * @param tableName
   * @param options
   * @param rawTableName
   * @private
   */
  addIndexQuery(tableName: TableName, options: AddIndexQueryOptions, rawTableName?: string) {
    // TODO: remove rawTableName. They were kept for backward compatibility.

    options.prefix = options.prefix || rawTableName || tableName;
    if (options.prefix && typeof options.prefix === 'string') {
      options.prefix = options.prefix.replace(/\./g, '_');
      options.prefix = options.prefix.replace(/("|')/g, '');
    }

    const fieldsSql = options.fields.map(field => {
      if (field instanceof SequelizeMethod) {
        return this.handleSequelizeMethod(field);
      }

      if (typeof field === 'string') {
        field = {
          name: field,
        };
      }

      let result = '';

      if ('attribute' in field) {
        throw new TypeError('option "attribute" in addIndex & addIndexQuery has been renamed to "name"');
      }

      if (!field.name) {
        throw new Error(`The following index field has no name: ${NodeUtil.inspect(field)}`);
      }

      result += this.quoteIdentifier(field.name);

      if (this._dialect.supports.index.collate && field.collate) {
        // TODO: throw if collate is specified but the dialect doesn't support it
        result += ` COLLATE ${this.quoteIdentifier(field.collate)}`;
      }

      // TODO: if operator specified but unsupported, throw
      if (this._dialect.supports.index.operator) {
        const operator = field.operator || options.operator;
        if (operator) {
          result += ` ${operator}`;
        }
      }

      // TODO: if length specified but unsupported, throw
      if (this._dialect.supports.index.length && field.length) {
        result += `(${field.length})`;
      }

      if (field.order) {
        result += ` ${field.order}`;
      }

      return result;
    });

    // Mostly for cases where addIndex is called directly by the user without an options object (for example in migrations)
    // All calls that go through sequelize should already have a name
    const indexName = inferIndexName(options, options.prefix);

    // TODO: remove this expect-error once Model has been migrated to TypeScript
    // @ts-expect-error
    options = Model._conformIndex(options);

    // TODO: if type specified but unsupported, throw
    if (!this._dialect.supports.index.type) {
      delete options.type;
    }

    // TODO: if options.where specified but not supported by dialect: throw
    const where = options.where ? this.whereQuery(options.where) : '';

    const escapedTableName: string = this.quoteTable(tableName);

    // TODO: if options.concurrently is used but not supported: throw
    const concurrently = this._dialect.supports.index.concurrently && options.concurrently ? 'CONCURRENTLY' : '';
    let ind: string[];
    if (this._dialect.supports.indexViaAlter) {
      ind = [
        'ALTER TABLE',
        escapedTableName,
        concurrently,
        'ADD',
      ];
    } else {
      ind = ['CREATE'];
    }

    ind.push(
      options.unique ? 'UNIQUE' : '',
      options.type || '',
      'INDEX',
      !this._dialect.supports.indexViaAlter ? concurrently : '',
      this.quoteIdentifiers(indexName),
      // TODO: if index.using is not supported but requested: throw
      this._dialect.supports.index.using === 1 && options.using ? `USING ${options.using}` : '',
      !this._dialect.supports.indexViaAlter ? `ON ${escapedTableName}` : '',
      this._dialect.supports.index.using === 2 && options.using ? `USING ${options.using}` : '',
      `(${fieldsSql.join(', ')})`,
      // TODO: if options.parser specified but not supported by dialect: throw
      this._dialect.supports.index.parser && options.parser ? `WITH PARSER ${options.parser}` : '',
      this._dialect.supports.index.where ? where : '',
    );

    return removeFalsy(ind).join(' ');
  }

  addConstraintQuery(tableName: TableName, options: ConstraintQueryOptions): string {
    const escapedTableName: string = this.quoteTable(tableName);

    return joinSQLFragments([
      'ALTER TABLE',
      escapedTableName,
      'ADD',
      this.getConstraintSnippet(escapedTableName, options),
      ';',
    ]);
  }

  getConstraintSnippet(escapedTableName: string, options: ConstraintQueryOptions) {
    let constraintSnippet;
    let constraintName;

    const fieldsSql = options.fields.map(field => {
      if (typeof field === 'string') {
        return this.quoteIdentifier(field);
      }

      if (field instanceof SequelizeMethod) {
        return this.handleSequelizeMethod(field);
      }

      if (field.attribute) {
        field.name = field.attribute;
      }

      if (!field.name) {
        throw new Error(`The following index field has no name: ${field}`);
      }

      return this.quoteIdentifier(field.name);
    });

    const fieldsSqlQuotedString = fieldsSql.join(', ');
    const fieldsSqlString = fieldsSql.join('_');

    switch (options.type.toUpperCase()) {
      case 'UNIQUE':
        constraintName = this.quoteIdentifier(options.name || `${escapedTableName}_${fieldsSqlString}_uk`);
        constraintSnippet = `CONSTRAINT ${constraintName} UNIQUE (${fieldsSqlQuotedString})`;
        break;
      case 'CHECK':
        options.where = this.whereItemsQuery(options.where);
        constraintName = this.quoteIdentifier(options.name || `${escapedTableName}_${fieldsSqlString}_ck`);
        constraintSnippet = `CONSTRAINT ${constraintName} CHECK (${options.where})`;
        break;
      case 'DEFAULT':
        if (options.defaultValue === undefined) {
          throw new Error('Default value must be specified for DEFAULT CONSTRAINT');
        }

        if (this._dialect.name !== 'mssql') {
          throw new Error('Default constraints are supported only for MSSQL dialect.');
        }

        constraintName = this.quoteIdentifier(options.name || `${escapedTableName}_${fieldsSqlString}_df`);
        constraintSnippet = `CONSTRAINT ${constraintName} DEFAULT (${this.escape(options.defaultValue)}) FOR ${fieldsSql[0]}`;
        break;
      case 'PRIMARY KEY':
        constraintName = this.quoteIdentifier(options.name || `${escapedTableName}_${fieldsSqlString}_pk`);
        constraintSnippet = `CONSTRAINT ${constraintName} PRIMARY KEY (${fieldsSqlQuotedString})`;
        break;
      case 'FOREIGN KEY': {
        const references = options.references;
        if (!references || !references.table || !(references.field || references.fields)) {
          throw new Error('references object with table and field must be specified');
        }

        constraintName = this.quoteIdentifier(options.name || `${escapedTableName}_${fieldsSqlString}_${references.table}_fk`);
        const quotedReferences
          = typeof references.field !== 'undefined'
          ? this.quoteIdentifier(references.field)
          : references.fields.map(f => this.quoteIdentifier(f)).join(', ');
        const referencesSnippet = `${this.quoteTable(references.table)} (${quotedReferences})`;
        constraintSnippet = `CONSTRAINT ${constraintName} `;
        constraintSnippet += `FOREIGN KEY (${fieldsSqlQuotedString}) REFERENCES ${referencesSnippet}`;
        if (options.onUpdate) {
          if (!this._dialect.supports.constraints.onUpdate) {
            throw new Error(`Constraint onUpdate is not supported by ${this._dialect}`);
          }

          constraintSnippet += ` ON UPDATE ${options.onUpdate.toUpperCase()}`;
        }

        if (options.onDelete) {
          constraintSnippet += ` ON DELETE ${options.onDelete.toUpperCase()}`;
        }

        break;
      }

      default:
        throw new Error(`${options.type} is invalid.`);
    }

    if (options.deferrable && ['UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY'].includes(options.type.toUpperCase())) {
      constraintSnippet += ` ${this.deferConstraintsQuery(options)}`;
    }

    return constraintSnippet;
  }

  removeConstraintQuery(tableName: TableName, constraintName: string): string {
    const escapedTableName = this.quoteTable(tableName);

    return joinSQLFragments([
      'ALTER TABLE',
      escapedTableName,
      'DROP CONSTRAINT',
      this.quoteIdentifiers(constraintName),
    ]);
  }

  /**
   * Quote an object based on its type. This is a more general version of quoteIdentifiers
   * Strings: should proxy to quoteIdentifiers
   * Arrays:
   *   - Expects array in the form: [<model> (optional), <model> (optional),... String, String (optional)]
   *     Each <model> can be a model, or an object {model: Model, as: String}, matching include, or an
   *     association object, or the name of an association.
   *   - Zero or more models can be included in the array and are used to trace a path through the tree of
   *     included nested associations. This produces the correct table name for the ORDER BY/GROUP BY SQL
   *     and quotes it.
   *   - If a single string is appended to end of array, it is quoted.
   *     If two strings appended, the 1st string is quoted, the 2nd string unquoted.
   * Objects:
   *   - If raw is set, that value should be returned verbatim, without quoting
   *   - If fn is set, the string should start with the value of fn, starting paren, followed by
   *     the values of cols (which is assumed to be an array), quoted and joined with ', ',
   *     unless they are themselves objects
   *   - If direction is set, should be prepended
   *
   * Currently this function is only used for ordering / grouping columns and Sequelize.col(), but it could
   * potentially also be used for other places where we want to be able to call SQL functions (e.g. as default values)
   *
   * @param collection
   * @param parent
   * @param connector
   * @private
  */
  quote(collection: string | Array<string | ModelStatic<any> | { model: ModelStatic<any>, as?: string }>, parent, connector = '.') {
    // init
    const validOrderOptions = [
      'ASC',
      'DESC',
      'ASC NULLS LAST',
      'DESC NULLS LAST',
      'ASC NULLS FIRST',
      'DESC NULLS FIRST',
      'NULLS FIRST',
      'NULLS LAST',
    ];

    // just quote as identifiers if string
    if (typeof collection === 'string') {
      return this.quoteIdentifiers(collection);
    }

    if (Array.isArray(collection)) {
      // iterate through the collection and mutate objects into associations
      for (let [index, item] of collection.entries()) {
        const previous = collection[index - 1];
        let previousAssociation;
        let previousModel;

        // set the previous as the parent when previous is undefined or the target of the association
        if (!previous && parent !== undefined) {
          previousModel = parent;
        } else if (previous && previous instanceof Association) {
          previousAssociation = previous;
          previousModel = previous.target;
        }

        // if the previous item is a model, then attempt getting an association
        if (previousModel && isModelSubclass(previousModel)) {
          let model;
          let as;

          if (isModelSubclass(item)) {
            // set
            model = item;
          } else if (isPlainObject(item) && isModelSubclass(item.model)) {
            // set
            model = item.model;
            as = item.as;
          }

          if (model) {
            // set the as to either the through name or the model name
            if (!as && previousAssociation instanceof BelongsToMany && previousAssociation?.through.model === model) {
              // get from previous association
              item = new Association(previousModel, model, {
                as: model.name,
              });
            } else {
              // get association from previous model
              item = previousModel.getAssociationForAlias(model, as);

              // attempt to use the model name if the item is still null
              if (!item) {
                item = previousModel.getAssociationForAlias(model, model.name);
              }
            }

            // make sure we have an association
            if (!(item instanceof Association)) {
              throw new TypeError(NodeUtil.format('Unable to find a valid association for model, \'%s\'', model.name));
            }
          }
        }

        if (typeof item === 'string') {
          // get order index
          const orderIndex = validOrderOptions.indexOf(item.toUpperCase());

          // see if this is an order
          if (index > 0 && orderIndex !== -1) {
            item = literal(` ${validOrderOptions[orderIndex]}`);
          } else if (isModelSubclass(previousModel)) {
            // only go down this path if we have preivous model and check only once
            if (previousModel.associations !== undefined && previousModel.associations[item]) {
              // convert the item to an association
              item = previousModel.associations[item];
            } else if (previousModel.getAttributes()?.[item]?.field) {
              // convert the item attribute from its alias
              item = previousModel.getAttributes()[item].field;
            } else if (
              item.includes('.')
              && previousModel.getAttributes() !== undefined
            ) {
              const itemSplit = item.split('.');

              if (previousModel.getAttributes()[itemSplit[0]].type instanceof DataTypes.JSON) {
                // just quote identifiers for now
                const identifier = this.quoteIdentifiers(`${previousModel.name}.${previousModel.getAttributes()[itemSplit[0]].field}`);

                // get path
                const path = itemSplit.slice(1);

                // extract path
                item = this.jsonPathExtractionQuery(identifier, path);

                // literal because we don't want to append the model name when string
                item = literal(item);
              }
            }
          }
        }

        collection[index] = item;
      }

      // loop through array, adding table names of models to quoted
      const collectionLength = collection.length;
      const tableNames = [];
      let item;
      let i = 0;

      for (i = 0; i < collectionLength - 1; i++) {
        item = collection[i];
        if (typeof item === 'string' || item._modelAttribute || item instanceof SequelizeMethod) {
          break;
        } else if (item instanceof Association) {
          tableNames[i] = item.as;
        }
      }

      // start building sql
      let sql = '';

      if (i > 0) {
        sql += `${this.quoteIdentifier(tableNames.join(connector))}.`;
      } else if (typeof collection[0] === 'string' && parent) {
        sql += `${this.quoteIdentifier(parent.name)}.`;
      }

      // loop through everything past i and append to the sql
      for (const collectionItem of collection.slice(i)) {
        sql += this.quote(collectionItem, parent, connector);
      }

      return sql;
    }

    if (collection._modelAttribute) {
      return `${this.quoteTable(collection.Model.name)}.${this.quoteIdentifier(collection.fieldName)}`;
    }

    if (collection instanceof SequelizeMethod) {
      return this.handleSequelizeMethod(collection);
    }

    if (isPlainObject(collection) && collection.raw) {
      // simple objects with raw is no longer supported
      throw new Error('The `{raw: "..."}` syntax is no longer supported.  Use `sequelize.literal` instead.');
    }

    throw new Error(`Unknown structure passed to order / group: ${NodeUtil.inspect(collection)}`);
  }

  #initQuoteIdentifier() {
    this._quoteIdentifier = this.quoteIdentifier;
    this.quoteIdentifier = function quoteIdentifier(identifier, force) {
      if (identifier === '*') {
        return identifier;
      }

      return this._quoteIdentifier(identifier, force);
    };
  }

  /**
   * Adds quotes to identifier
   *
   * @param _identifier
   * @param _force
   *
   * @returns
   */
  quoteIdentifier(_identifier: string, _force?: boolean): string {
    throw new Error(`quoteIdentifier for Dialect "${this.dialect}" is not implemented`);
  }

  /**
   * Split a list of identifiers by "." and quote each part.
   *
   * @param identifiers
   *
   * @returns
   */
  quoteIdentifiers(identifiers: string): string {
    if (identifiers.includes('.')) {
      const identifierParts = identifiers.split('.');

      const head = identifierParts.slice(0, -1).join('->');
      const tail = identifierParts[identifierParts.length - 1];

      return `${this.quoteIdentifier(head)}.${this.quoteIdentifier(tail)}`;
    }

    return this.quoteIdentifier(identifiers);
  }

  quoteAttribute(attribute: string, model: ModelStatic<Model>): string {
    if (model && attribute in model.getAttributes()) {
      return this.quoteIdentifier(attribute);
    }

    return this.quoteIdentifiers(attribute);
  }

  /**
   * Quote table name with optional alias and schema attribution
   *
   * @param  param table string or object
   * @param alias alias name
   *
   * @returns
   */
  quoteTable(param: TableName, alias: boolean | string | undefined = false): string {
    let table = '';

    const tableNameIsObject = isPlainObject(param);

    if (alias === true) {
      alias = tableNameIsObject
        ? param.as || param.name
        : param;
    }

    if (tableNameIsObject) {
      if (this._dialect.supports.schemas) {
        if (param.schema) {
          table += `${this.quoteIdentifier(param.schema)}.`;
        }

        table += this.quoteIdentifier(param.tableName);
      } else {
        if (param.schema) {
          table += param.schema + (param.delimiter || '.');
        }

        table += param.tableName;
        table = this.quoteIdentifier(table);
      }
    } else {
      table = this.quoteIdentifier(param);
    }

    if (alias) {
      table += ` AS ${this.quoteIdentifier(alias)}`;
    }

    return table;
  }

  /**
   * Escape a value (e.g. a string, number or date)
   *
   * @param value
   * @param field
   * @param options
   * @param options.where
   * @param options.acceptStrings
   * @private
   */
  escape(
    value: Escapable,
    field?: BuiltModelAttribute,
    options?: { operation?: DataTypeStringifyOptions['operation'] } & ValidateOptions,
  ): string {

    if (value != null) {
      if (value instanceof SequelizeMethod) {
        return this.handleSequelizeMethod(value);
      }

      if (field?.type) {
        this.validate(value, field, options);

        if (field.type.stringify) {
          value = field.type.stringify(value, {
            // Users shouldn't have to worry about these args - just give them a function that takes a single arg
            escape: escVal => SqlString.escape(escVal, this.options.timezone, this.dialect),
            field,
            timezone: this.options.timezone,
            operation: options?.operation,
          });

          if (field.type.escape === false) {
            // The data-type already did the required escaping
            return value;
          }
        }
      }
    }

    return SqlString.escape(value, this.options.timezone, this.dialect);
  }

  /**
   * Creates a {@link ParamBinder} function.
   *
   * @param bind the array that will receive all bind parameters passed to the {@link ParamBinder} function.
   *             *This parameter will be mutated!*.
   */
  bindParam(bind: unknown[]): ParamBinder {
    return value => {
      bind.push(value);

      return `$${bind.length}`;
    };
  }

  /**
   * Returns a bind parameter representation of a value (e.g. a string, number or date)
   *
   * @param value
   * @param field
   * @param options
   * @param bindParam
   * @private
   */
  format(
    value: unknown,
    field: BuiltModelAttribute,
    options: { operation?: DataTypeStringifyOptions['operation'] } & ValidateOptions,
    bindParam: ParamBinder,
  ): string {
    options = options || {};

    if (value !== null && value !== undefined) {
      if (value instanceof SequelizeMethod) {
        throw new TypeError('Cannot pass SequelizeMethod as a bind parameter - use escape instead');
      }

      if (field && field.type) {
        this.validate(value, field, options);

        if (field.type.bindParam) {
          return field.type.bindParam(value, {
            escape: identity,
            field,
            timezone: this.options.timezone,
            operation: options.operation,
            bindParam,
          });
        }
      }
    }

    return bindParam(value);
  }

  /**
   * Validate a value against a field specification
   *
   * @param value
   * @param field
   * @param options
   * @private
   */
  validate(value: unknown, field: BuiltModelAttribute, options?: ValidateOptions) {
    if (this.options.typeValidation && field.type.validate && value) {
      try {
        if (options?.isList && Array.isArray(value)) {
          for (const item of value) {
            field.type.validate(item, options);
          }
        } else {
          field.type.validate(value, options);
        }
      } catch (error) {
        if (error instanceof ValidationError) {
          error.errors.push(new ValidationErrorItem(
            error.message,
            'validation error',
            field.fieldName,
            value,
            null,
            `${field.type.key} validator`,
          ));
        }

        throw error;
      }
    }
  }

  isIdentifierQuoted(identifier: string): boolean {
    return /^\s*(?:(["'`])(?:(?!\1).|\1{2})*\1\.?)+\s*$/i.test(identifier);
  }

  /**
   * Generates an SQL query that extract JSON property of given path.
   *
   * @param               _column   The JSON column
   * @param _path   The path to extract (optional)
   * @param              _isJson The value is JSON use alt symbols (optional)
   * @returns                        The generated sql query
   * @private
   */
  jsonPathExtractionQuery(_column: string, _path: string, _isJson: boolean) {
    throw new Error(`JSON operations are not supported in ${this.dialect}`);
  }

  /**
   * Returns a query for selecting elements in the table <tableName>.
   * Options:
   * - attributes -> An array of attributes (e.g. ['name', 'birthday']). Default: *
   * - where -> A hash with conditions (e.g. {name: 'foo'})
   *               OR an ID as integer
   * - order -> e.g. 'id DESC'
   * - group
   * - limit -> The maximum count you want to get.
   * - offset -> An offset value to start from. Only useable with limit!
   *
   * @param tableName
   * @param options
   * @param model
   * @private
   */
  selectQuery(tableName: TableName, options: SelectQueryOptions, model: ModelStatic<any>) {
    const limit = options.limit;
    const mainQueryItems = [];
    const subQueryItems = [];
    const subQuery = options.subQuery === undefined ? limit != null && options.hasMultiAssociation : options.subQuery;
    const attributes = {
      main: options.attributes && [...options.attributes],
      subQuery: null,
    };
    const mainTable = {
      name: tableName,
      quotedName: null,
      as: null,
      model,
    };
    const topLevelInfo = {
      names: mainTable,
      options,
      subQuery,
    };
    const mainJoinQueries = [];
    const subJoinQueries = [];
    let query;

    // Aliases can be passed through subqueries and we don't want to reset them
    if (this.options.minifyAliases && !options.aliasesMapping) {
      options.aliasesMapping = new Map();
      options.aliasesByTable = {};
      options.includeAliases = new Map();
    }

    // resolve table name options
    if (options.tableAs) {
      mainTable.as = this.quoteIdentifier(options.tableAs);
    } else if (!Array.isArray(mainTable.name) && mainTable.model) {
      mainTable.as = this.quoteIdentifier(mainTable.model.name);
    }

    mainTable.quotedName = !Array.isArray(mainTable.name) ? this.quoteTable(mainTable.name) : tableName.map(t => {
      return Array.isArray(t) ? this.quoteTable(t[0], t[1]) : this.quoteTable(t, true);
    }).join(', ');

    if (subQuery && attributes.main) {
      for (const keyAtt of mainTable.model.primaryKeyAttributes) {
        // Check if mainAttributes contain the primary key of the model either as a field or an aliased field
        if (!attributes.main.some(attr => keyAtt === attr || keyAtt === attr[0] || keyAtt === attr[1])) {
          const field = mainTable.model.rawAttributes[keyAtt].field;
          attributes.main.push(field ? [keyAtt, field] : keyAtt);
        }
      }
    }

    attributes.main = this.escapeAttributes(attributes.main, options, mainTable.as);
    attributes.main = attributes.main || (options.include ? [`${mainTable.as}.*`] : ['*']);

    // If subquery, we add the mainAttributes to the subQuery and set the mainAttributes to select * from subquery
    if (subQuery || options.groupedLimit) {
      // We need primary keys
      attributes.subQuery = attributes.main;
      attributes.main = [`${mainTable.as || mainTable.quotedName}.*`];
    }

    if (options.include) {
      for (const include of options.include) {
        if (include.separate) {
          continue;
        }

        const joinQueries = this.generateInclude(include, {
          externalAs: mainTable.as,
          internalAs: mainTable.as,
        }, topLevelInfo);

        subJoinQueries.push(...joinQueries.subQuery);
        mainJoinQueries.push(...joinQueries.mainQuery);

        if (joinQueries.attributes.main.length > 0) {
          attributes.main.push(...joinQueries.attributes.main);
          attributes.main = uniq(attributes.main);
        }

        if (joinQueries.attributes.subQuery.length > 0) {
          attributes.subQuery.push(...joinQueries.attributes.subQuery);
          attributes.subQuery = uniq(attributes.subQuery);
        }
      }
    }

    if (subQuery) {
      subQueryItems.push(
        this.selectFromTableFragment(options, mainTable.model, attributes.subQuery, mainTable.quotedName, mainTable.as),
        subJoinQueries.join(''),
      );
    } else {
      if (options.groupedLimit) {
        if (!mainTable.as) {
          mainTable.as = mainTable.quotedName;
        }

        const where = { ...options.where };
        let groupedLimitOrder;
        let whereKey;
        let include;
        let groupedTableName = mainTable.as;

        if (typeof options.groupedLimit.on === 'string') {
          whereKey = options.groupedLimit.on;
        } else if (options.groupedLimit.on instanceof HasMany) {
          whereKey = options.groupedLimit.on.foreignKeyField;
        }

        if (options.groupedLimit.on instanceof BelongsToMany) {
          // BTM includes needs to join the through table on to check ID
          groupedTableName = options.groupedLimit.on.manyFromSource.as;
          const groupedLimitOptions = Model._validateIncludedElements({
            include: [{
              association: options.groupedLimit.on.manyFromSource,
              duplicating: false, // The UNION'ed query may contain duplicates, but each sub-query cannot
              required: true,
              where: {
                [Op.placeholder]: true,
                ...options.groupedLimit.through && options.groupedLimit.through.where,
              },
            }],
            model,
          });

          // Make sure attributes from the join table are mapped back to models
          options.hasJoin = true;
          options.hasMultiAssociation = true;
          options.includeMap = Object.assign(groupedLimitOptions.includeMap, options.includeMap);
          options.includeNames.push(...options.includeNames);
          include = groupedLimitOptions.include;

          if (Array.isArray(options.order)) {
            // We need to make sure the order by attributes are available to the parent query
            for (let [i, order] of options.order.entries()) {
              if (Array.isArray(order)) {
                order = order[0];
              }

              let alias = `subquery_order_${i}`;
              options.attributes.push([order, alias]);

              // We don't want to prepend model name when we alias the attributes, so quote them here
              alias = literal(this.quote(alias));

              if (Array.isArray(options.order[i])) {
                options.order[i][0] = alias;
              } else {
                options.order[i] = alias;
              }
            }

            groupedLimitOrder = options.order;
          }
        } else {
          // Ordering is handled by the subqueries, so ordering the UNION'ed result is not needed
          groupedLimitOrder = options.order;
          delete options.order;
          where[Op.placeholder] = true;
        }

        // Caching the base query and splicing the where part into it is consistently > twice
        // as fast than generating from scratch each time for values.length >= 5
        const baseQuery = `SELECT * FROM (${this.selectQuery(
          tableName,
          {
            attributes: options.attributes,
            offset: options.offset,
            limit: options.groupedLimit.limit,
            order: groupedLimitOrder,
            aliasesMapping: options.aliasesMapping,
            aliasesByTable: options.aliasesByTable,
            where,
            include,
            model,
          },
          model,
        ).replace(/;$/, '')}) AS sub`; // Every derived table must have its own alias
        const placeHolder = this.whereItemQuery(Op.placeholder, true, { model });
        const splicePos = baseQuery.indexOf(placeHolder);

        mainQueryItems.push(this.selectFromTableFragment(options, mainTable.model, attributes.main, `(${
          options.groupedLimit.values.map(value => {
            let groupWhere;
            if (whereKey) {
              groupWhere = {
                [whereKey]: value,
              };
            }

            if (include) {
              groupWhere = {
                [options.groupedLimit.on.foreignIdentifierField]: value,
              };
            }

            return spliceStr(
              baseQuery,
              splicePos,
              placeHolder.length,
              this.getWhereConditions(groupWhere, groupedTableName),
            );
          }).join(
            this._dialect.supports['UNION ALL'] ? ' UNION ALL ' : ' UNION ',
          )
        })`, mainTable.as));
      } else {
        mainQueryItems.push(
          this.selectFromTableFragment(options, mainTable.model, attributes.main, mainTable.quotedName, mainTable.as),
        );
      }

      mainQueryItems.push(mainJoinQueries.join(''));
    }

    // Add WHERE to sub or main query
    if (Object.prototype.hasOwnProperty.call(options, 'where') && !options.groupedLimit) {
      options.where = this.getWhereConditions(options.where, mainTable.as || tableName, model, options);
      if (options.where) {
        if (subQuery) {
          subQueryItems.push(` WHERE ${options.where}`);
        } else {
          mainQueryItems.push(` WHERE ${options.where}`);
          // Walk the main query to update all selects
          for (const [key, value] of mainQueryItems.entries()) {
            if (value.startsWith('SELECT')) {
              mainQueryItems[key] = this.selectFromTableFragment(options, model, attributes.main,
                mainTable.quotedName, mainTable.as, options.where);
            }
          }
        }
      }
    }

    // Add GROUP BY to sub or main query
    if (options.group) {
      options.group = Array.isArray(options.group) ? options.group.map(t => this.aliasGrouping(t, model, mainTable.as, options)).join(', ') : this.aliasGrouping(options.group, model, mainTable.as, options);

      if (subQuery && options.group) {
        subQueryItems.push(` GROUP BY ${options.group}`);
      } else if (options.group) {
        mainQueryItems.push(` GROUP BY ${options.group}`);
      }
    }

    // Add HAVING to sub or main query
    if (Object.prototype.hasOwnProperty.call(options, 'having')) {
      options.having = this.getWhereConditions(options.having, tableName, model, options, false);
      if (options.having) {
        if (subQuery) {
          subQueryItems.push(` HAVING ${options.having}`);
        } else {
          mainQueryItems.push(` HAVING ${options.having}`);
        }
      }
    }

    // Add ORDER to sub or main query
    if (options.order) {
      const orders = this.getQueryOrders(options, model, subQuery);
      if (orders.mainQueryOrder.length > 0) {
        mainQueryItems.push(` ORDER BY ${orders.mainQueryOrder.join(', ')}`);
      }

      if (orders.subQueryOrder.length > 0) {
        subQueryItems.push(` ORDER BY ${orders.subQueryOrder.join(', ')}`);
      }
    }

    // Add LIMIT, OFFSET to sub or main query
    const limitOrder = this.addLimitAndOffset(options, mainTable.model);
    if (limitOrder && !options.groupedLimit) {
      if (subQuery) {
        subQueryItems.push(limitOrder);
      } else {
        mainQueryItems.push(limitOrder);
      }
    }

    if (subQuery) {
      this._throwOnEmptyAttributes(attributes.main, { modelName: model && model.name, as: mainTable.as });
      query = `SELECT ${attributes.main.join(', ')} FROM (${subQueryItems.join('')}) AS ${mainTable.as}${mainJoinQueries.join('')}${mainQueryItems.join('')}`;
    } else {
      query = mainQueryItems.join('');
    }

    if (options.lock && this._dialect.supports.lock) {
      let lock = options.lock;
      if (typeof options.lock === 'object') {
        lock = options.lock.level;
      }

      if (this._dialect.supports.lockKey && ['KEY SHARE', 'NO KEY UPDATE'].includes(lock)) {
        query += ` FOR ${lock}`;
      } else if (lock === 'SHARE') {
        query += ` ${this._dialect.supports.forShare}`;
      } else {
        query += ' FOR UPDATE';
      }

      if (this._dialect.supports.lockOf && options.lock.of && options.lock.of.prototype instanceof Model) {
        query += ` OF ${this.quoteTable(options.lock.of.name)}`;
      }

      if (this._dialect.supports.skipLocked && options.skipLocked) {
        query += ' SKIP LOCKED';
      }
    }

    return `${query};`;
  }

  aliasGrouping(field, model, tableName, options) {
    const src = Array.isArray(field) ? field[0] : field;

    return this.quote(this.#getAliasForField(tableName, src, options) || src, model);
  }

  escapeAttributes(attributes, options, mainTableAs) {
    return attributes && attributes.map(attr => {
      let addTable = true;

      if (attr instanceof SequelizeMethod) {
        return this.handleSequelizeMethod(attr);
      }

      if (Array.isArray(attr)) {
        if (attr.length !== 2) {
          throw new Error(`${JSON.stringify(attr)} is not a valid attribute definition. Please use the following format: ['attribute definition', 'alias']`);
        }

        attr = [...attr];

        if (attr[0] instanceof SequelizeMethod) {
          attr[0] = this.handleSequelizeMethod(attr[0]);
          addTable = false;
        } else if (!attr[0].includes('(') && !attr[0].includes(')')) {
          attr[0] = this.quoteIdentifier(attr[0]);
        } else {
          deprecations.noRawAttributes();
        }

        let alias = attr[1];

        if (this.options.minifyAliases) {
          alias = this.#getMinifiedAlias(alias, mainTableAs, options);
        }

        attr = [attr[0], this.quoteIdentifier(alias)].join(' AS ');
      } else {
        attr = !attr.includes(TICK_CHAR) && !attr.includes('"')
          ? this.quoteAttribute(attr, options.model)
          : this.escape(attr);
      }

      if (!isEmpty(options.include) && (!attr.includes('.') || options.dotNotation) && addTable) {
        attr = `${mainTableAs}.${attr}`;
      }

      return attr;
    });
  }

  generateInclude(include, parentTableName, topLevelInfo) {
    const joinQueries = {
      mainQuery: [],
      subQuery: [],
    };
    const mainChildIncludes = [];
    const subChildIncludes = [];
    let requiredMismatch = false;
    const includeAs = {
      internalAs: include.as,
      externalAs: include.as,
    };
    const attributes = {
      main: [],
      subQuery: [],
    };
    let joinQuery;

    topLevelInfo.options.keysEscaped = true;

    if (topLevelInfo.names.name !== parentTableName.externalAs && topLevelInfo.names.as !== parentTableName.externalAs) {
      includeAs.internalAs = `${parentTableName.internalAs}->${include.as}`;
      includeAs.externalAs = `${parentTableName.externalAs}.${include.as}`;
    }

    // includeIgnoreAttributes is used by aggregate functions
    if (topLevelInfo.options.includeIgnoreAttributes !== false) {
      include.model._expandAttributes(include);
      mapFinderOptions(include, include.model);

      const includeAttributes = include.attributes.map(attr => {
        let attrAs = attr;
        let verbatim = false;

        if (Array.isArray(attr) && attr.length === 2) {
          if (attr[0] instanceof SequelizeMethod && (
            attr[0] instanceof Literal
            || attr[0] instanceof Cast
            || attr[0] instanceof Fn
          )) {
            verbatim = true;
          }

          attr = attr.map(attrPart => {
            return attrPart instanceof SequelizeMethod
              ? this.handleSequelizeMethod(attrPart)
              : attrPart;
          });

          attrAs = attr[1];
          attr = attr[0];
        }

        if (attr instanceof Literal) {
          return attr.val; // We trust the user to rename the field correctly
        }

        if (attr instanceof Cast || attr instanceof Fn) {
          throw new TypeError(
            'Tried to select attributes using Sequelize.cast or Sequelize.fn without specifying an alias for the result, during eager loading. '
            + 'This means the attribute will not be added to the returned instance',
          );
        }

        let prefix;
        if (verbatim === true) {
          prefix = attr;
        } else if (/#>>|->>/.test(attr)) {
          prefix = `(${this.quoteIdentifier(includeAs.internalAs)}.${attr.replace(/\(|\)/g, '')})`;
        } else if (/json_extract\(/.test(attr)) {
          prefix = attr.replace(/json_extract\(/i, `json_extract(${this.quoteIdentifier(includeAs.internalAs)}.`);
        } else {
          prefix = `${this.quoteIdentifier(includeAs.internalAs)}.${this.quoteIdentifier(attr)}`;
        }

        let alias = `${includeAs.externalAs}.${attrAs}`;

        if (this.options.minifyAliases) {
          alias = this.#getMinifiedAlias(alias, includeAs.internalAs, topLevelInfo.options);
        }

        return joinSQLFragments([
          prefix,
          'AS',
          this.quoteIdentifier(alias, true),
        ]);
      });
      if (include.subQuery && topLevelInfo.subQuery) {
        for (const attr of includeAttributes) {
          attributes.subQuery.push(attr);
        }
      } else {
        for (const attr of includeAttributes) {
          attributes.main.push(attr);
        }
      }
    }

    // through
    if (include.through) {
      joinQuery = this.generateThroughJoin(include, includeAs, parentTableName.internalAs, topLevelInfo);
    } else {
      this.#generateSubQueryFilter(include, includeAs, topLevelInfo);
      joinQuery = this.generateJoin(include, topLevelInfo);
    }

    // handle possible new attributes created in join
    if (joinQuery.attributes.main.length > 0) {
      attributes.main.push(...joinQuery.attributes.main);
    }

    if (joinQuery.attributes.subQuery.length > 0) {
      attributes.subQuery.push(...joinQuery.attributes.subQuery);
    }

    if (include.include) {
      for (const childInclude of include.include) {
        if (childInclude.separate || childInclude._pseudo) {
          continue;
        }

        const childJoinQueries = this.generateInclude(childInclude, includeAs, topLevelInfo);

        if (include.required === false && childInclude.required === true) {
          requiredMismatch = true;
        }

        // if the child is a sub query we just give it to the
        if (childInclude.subQuery && topLevelInfo.subQuery) {
          subChildIncludes.push(childJoinQueries.subQuery);
        }

        if (childJoinQueries.mainQuery) {
          mainChildIncludes.push(childJoinQueries.mainQuery);
        }

        if (childJoinQueries.attributes.main.length > 0) {
          attributes.main.push(...childJoinQueries.attributes.main);
        }

        if (childJoinQueries.attributes.subQuery.length > 0) {
          attributes.subQuery.push(...childJoinQueries.attributes.subQuery);
        }
      }
    }

    if (include.subQuery && topLevelInfo.subQuery) {
      if (requiredMismatch && subChildIncludes.length > 0) {
        joinQueries.subQuery.push(` ${joinQuery.join} ( ${joinQuery.body}${subChildIncludes.join('')} ) ON ${joinQuery.condition}`);
      } else {
        joinQueries.subQuery.push(` ${joinQuery.join} ${joinQuery.body} ON ${joinQuery.condition}`);
        if (subChildIncludes.length > 0) {
          joinQueries.subQuery.push(subChildIncludes.join(''));
        }
      }

      joinQueries.mainQuery.push(mainChildIncludes.join(''));
    } else {
      if (requiredMismatch && mainChildIncludes.length > 0) {
        joinQueries.mainQuery.push(` ${joinQuery.join} ( ${joinQuery.body}${mainChildIncludes.join('')} ) ON ${joinQuery.condition}`);
      } else {
        joinQueries.mainQuery.push(` ${joinQuery.join} ${joinQuery.body} ON ${joinQuery.condition}`);
        if (mainChildIncludes.length > 0) {
          joinQueries.mainQuery.push(mainChildIncludes.join(''));
        }
      }

      joinQueries.subQuery.push(subChildIncludes.join(''));
    }

    return {
      mainQuery: joinQueries.mainQuery.join(''),
      subQuery: joinQueries.subQuery.join(''),
      attributes,
    };
  }

  #getMinifiedAlias(
    alias: string, tableName: string,
    options: { aliasesByTable: Record<string, string>, aliasesMapping: Map<string, string> },
  ): string {
    // We do not want to re-alias in case of a subquery
    if (options.aliasesByTable[`${tableName}${alias}`]) {
      return options.aliasesByTable[`${tableName}${alias}`];
    }

    // Do not alias custom suquery_orders
    if (/subquery_order_\d/.test(alias)) {
      return alias;
    }

    const minifiedAlias = `_${options.aliasesMapping.size}`;

    options.aliasesMapping.set(minifiedAlias, alias);
    options.aliasesByTable[`${tableName}${alias}`] = minifiedAlias;

    return minifiedAlias;
  }

  #getAliasForField(tableName: string, field: string, options: { aliasesByTable: Record<string, string> }): Nullish<string> {
    if (this.options.minifyAliases && options.aliasesByTable[`${tableName}${field}`]) {
      return options.aliasesByTable[`${tableName}${field}`];
    }

    return null;
  }

  generateJoin(include, topLevelInfo) {
    const association = include.association;
    const parent = include.parent;
    const parentIsTop = Boolean(parent)
      && !include.parent.association
      && include.parent.model.name === topLevelInfo.options.model.name;

    let $parent;
    let joinWhere;
    /* Attributes for the left side */
    const left = association.source;
    const attrLeft = association instanceof BelongsTo
      ? association.identifier
      : association.sourceKeyAttribute || left.primaryKeyAttribute;
    const fieldLeft = association instanceof BelongsTo
      ? association.identifierField
      : left.rawAttributes[association.sourceKeyAttribute || left.primaryKeyAttribute].field;
    let asLeft;
    /* Attributes for the right side */
    const right = include.model;
    const tableRight = right.getTableName();
    const fieldRight = association instanceof BelongsTo
      ? right.rawAttributes[association.targetIdentifier || right.primaryKeyAttribute].field
      : association.identifierField;
    let asRight = include.as;

    while (($parent = $parent && $parent.parent || include.parent) && $parent.association) {
      if (asLeft) {
        asLeft = `${$parent.as}->${asLeft}`;
      } else {
        asLeft = $parent.as;
      }
    }

    if (!asLeft) {
      asLeft = parent.as || parent.model.name;
    } else {
      asRight = `${asLeft}->${asRight}`;
    }

    let joinOn = `${this.quoteTable(asLeft)}.${this.quoteIdentifier(fieldLeft)}`;
    const subqueryAttributes = [];

    if (
      (topLevelInfo.options.groupedLimit && parentIsTop)
      || (topLevelInfo.subQuery && include.parent.subQuery && !include.subQuery)
    ) {
      if (parentIsTop) {
        // The main model attributes is not aliased to a prefix
        const tableName = this.quoteTable(parent.as || parent.model.name);

        // Check for potential aliased JOIN condition
        joinOn = this.#getAliasForField(tableName, attrLeft, topLevelInfo.options) || `${tableName}.${this.quoteIdentifier(attrLeft)}`;

        if (topLevelInfo.subQuery) {
          const dbIdentifier = `${tableName}.${this.quoteIdentifier(fieldLeft)}`;
          subqueryAttributes.push(dbIdentifier !== joinOn ? `${dbIdentifier} AS ${this.quoteIdentifier(attrLeft)}` : dbIdentifier);
        }
      } else {
        const joinSource = `${asLeft.replace(/->/g, '.')}.${attrLeft}`;

        // Check for potential aliased JOIN condition
        joinOn = this.#getAliasForField(asLeft, joinSource, topLevelInfo.options) || this.quoteIdentifier(joinSource);
      }
    }

    joinOn += ` = ${this.quoteIdentifier(asRight)}.${this.quoteIdentifier(fieldRight)}`;

    if (include.on) {
      joinOn = this.whereItemsQuery(include.on, {
        prefix: literal(this.quoteIdentifier(asRight)),
        model: include.model,
      });
    }

    if (include.where) {
      joinWhere = this.whereItemsQuery(include.where, {
        prefix: literal(this.quoteIdentifier(asRight)),
        model: include.model,
      });
      if (joinWhere) {
        if (include.or) {
          joinOn += ` OR ${joinWhere}`;
        } else {
          joinOn += ` AND ${joinWhere}`;
        }
      }
    }

    if (this.options.minifyAliases && asRight.length > 63) {
      const alias = `%${topLevelInfo.options.includeAliases.size}`;

      topLevelInfo.options.includeAliases.set(alias, asRight);
    }

    return {
      join: include.required ? 'INNER JOIN' : include.right && this._dialect.supports['RIGHT JOIN'] ? 'RIGHT OUTER JOIN' : 'LEFT OUTER JOIN',
      body: this.quoteTable(tableRight, asRight),
      condition: joinOn,
      attributes: {
        main: [],
        subQuery: subqueryAttributes,
      },
    };
  }

  /**
   * Returns the SQL fragments to handle returning the attributes from an insert/update query.
   *
   * @param modelAttributes An object with the model attributes.
   * @param options         An object with options.
   *
   * @private
   */
  generateReturnValues(modelAttributes, options) {
    const returnFields = [];
    const returnTypes = [];
    let outputFragment = '';
    let returningFragment = '';
    let tmpTable = '';

    if (Array.isArray(options.returning)) {
      returnFields.push(...options.returning.map(field => this.quoteIdentifier(field)));
    } else if (modelAttributes) {
      each(modelAttributes, attribute => {
        if (!(attribute.type instanceof DataTypes.VIRTUAL)) {
          returnFields.push(this.quoteIdentifier(attribute.field));
          returnTypes.push(attribute.type);
        }
      });
    }

    if (isEmpty(returnFields)) {
      returnFields.push('*');
    }

    if (this._dialect.supports.returnValues.returning) {
      returningFragment = ` RETURNING ${returnFields.join(',')}`;
    } else if (this._dialect.supports.returnValues.output) {
      outputFragment = ` OUTPUT ${returnFields.map(field => `INSERTED.${field}`).join(',')}`;

      // To capture output rows when there is a trigger on MSSQL DB
      if (options.hasTrigger && this._dialect.supports.tmpTableTrigger) {
        const tmpColumns = returnFields.map((field, i) => `${field} ${returnTypes[i].toSql()}`);

        tmpTable = `DECLARE
          @tmp TABLE
               (
                 ${tmpColumns.join(',')}
               ); `;
        outputFragment += ' INTO @tmp';
        returningFragment = '; SELECT * FROM @tmp';
      }
    }

    return { outputFragment, returnFields, returningFragment, tmpTable };
  }

  generateThroughJoin(include, includeAs, parentTableName, topLevelInfo) {
    const through = include.through;
    const throughTable = through.model.getTableName();
    const throughAs = `${includeAs.internalAs}->${through.as}`;
    const externalThroughAs = `${includeAs.externalAs}.${through.as}`;
    const throughAttributes = through.attributes.map(attr => {
      let alias = `${externalThroughAs}.${Array.isArray(attr) ? attr[1] : attr}`;

      if (this.options.minifyAliases) {
        alias = this.#getMinifiedAlias(alias, throughAs, topLevelInfo.options);
      }

      return joinSQLFragments([
        `${this.quoteIdentifier(throughAs)}.${this.quoteIdentifier(Array.isArray(attr) ? attr[0] : attr)}`,
        'AS',
        this.quoteIdentifier(alias),
      ]);
    });
    const association = include.association;
    const parentIsTop = !include.parent.association && include.parent.model.name === topLevelInfo.options.model.name;
    const tableSource = parentTableName;
    const identSource = association.identifierField;
    const tableTarget = includeAs.internalAs;
    const identTarget = association.foreignIdentifierField;
    const attrTarget = association.targetKeyField;

    const joinType = include.required ? 'INNER JOIN' : include.right && this._dialect.supports['RIGHT JOIN'] ? 'RIGHT OUTER JOIN' : 'LEFT OUTER JOIN';
    let joinBody;
    let joinCondition;
    const attributes = {
      main: [],
      subQuery: [],
    };
    let attrSource = association.sourceKey;
    let sourceJoinOn;
    let targetJoinOn;
    let throughWhere;
    let targetWhere;

    if (topLevelInfo.options.includeIgnoreAttributes !== false) {
      // Through includes are always hasMany, so we need to add the attributes to the mainAttributes no matter what (Real join will never be executed in subquery)
      for (const attr of throughAttributes) {
        attributes.main.push(attr);
      }
    }

    // Figure out if we need to use field or attribute
    if (!topLevelInfo.subQuery) {
      attrSource = association.sourceKeyField;
    }

    if (topLevelInfo.subQuery
      && !include.subQuery
      && !include.parent.subQuery
      && include.parent.model !== topLevelInfo.options.mainModel) {
      attrSource = association.sourceKeyField;
    }

    // Filter statement for left side of through
    // Used by both join and subquery where
    // If parent include was in a subquery need to join on the aliased attribute
    if (topLevelInfo.subQuery && !include.subQuery && include.parent.subQuery && !parentIsTop) {
      // If we are minifying aliases and our JOIN target has been minified, we need to use the alias instead of the original column name
      const joinSource = this.#getAliasForField(tableSource, `${tableSource}.${attrSource}`, topLevelInfo.options) || `${tableSource}.${attrSource}`;

      sourceJoinOn = `${this.quoteIdentifier(joinSource)} = `;
    } else {
      // If we are minifying aliases and our JOIN target has been minified, we need to use the alias instead of the original column name
      const aliasedSource = this.#getAliasForField(tableSource, attrSource, topLevelInfo.options) || attrSource;

      sourceJoinOn = `${this.quoteTable(tableSource)}.${this.quoteIdentifier(aliasedSource)} = `;
    }

    sourceJoinOn += `${this.quoteIdentifier(throughAs)}.${this.quoteIdentifier(identSource)}`;

    // Filter statement for right side of through
    // Used by both join and subquery where
    targetJoinOn = `${this.quoteIdentifier(tableTarget)}.${this.quoteIdentifier(attrTarget)} = `;
    targetJoinOn += `${this.quoteIdentifier(throughAs)}.${this.quoteIdentifier(identTarget)}`;

    if (through.where) {
      throughWhere = this.getWhereConditions(through.where, literal(this.quoteIdentifier(throughAs)), through.model);
    }

    // Generate a wrapped join so that the through table join can be dependent on the target join
    joinBody = `( ${this.quoteTable(throughTable, throughAs)} INNER JOIN ${this.quoteTable(include.model.getTableName(), includeAs.internalAs)} ON ${targetJoinOn}`;
    if (throughWhere) {
      joinBody += ` AND ${throughWhere}`;
    }

    joinBody += ')';
    joinCondition = sourceJoinOn;

    if ((include.where || include.through.where) && include.where) {
      targetWhere = this.getWhereConditions(
        include.where,
        literal(this.quoteIdentifier(includeAs.internalAs)),
        include.model,
        topLevelInfo.options,
      );
      if (targetWhere) {
        joinCondition += ` AND ${targetWhere}`;
      }
    }

    this.#generateSubQueryFilter(include, includeAs, topLevelInfo);

    return {
      join: joinType,
      body: joinBody,
      condition: joinCondition,
      attributes,
    };
  }

  /*
   * Generates subQueryFilter - a select nested in the where clause of the subQuery.
   * For a given include a query is generated that contains all the way from the subQuery
   * table to the include table plus everything that's in required transitive closure of the
   * given include.
   */
  #generateSubQueryFilter(include, includeAs, topLevelInfo) {
    if (!topLevelInfo.subQuery || !include.subQueryFilter) {
      return;
    }

    if (!topLevelInfo.options.where) {
      topLevelInfo.options.where = {};
    }

    let parent = include;
    let child = include;
    let nestedIncludes = this.#getRequiredClosure(include).include;
    let query;

    while ((parent = parent.parent)) {
      if (parent.parent && !parent.required) {
        return; // only generate subQueryFilter if all the parents of this include are required
      }

      if (parent.subQueryFilter) {
        // the include is already handled as this parent has the include on its required closure
        // skip to prevent duplicate subQueryFilter
        return;
      }

      nestedIncludes = [{ ...child, include: nestedIncludes, attributes: [] }];
      child = parent;
    }

    const topInclude = nestedIncludes[0];
    const topParent = topInclude.parent;
    const topAssociation = topInclude.association;
    topInclude.association = undefined;

    if (topInclude.through && Object(topInclude.through.model) === topInclude.through.model) {
      query = this.selectQuery(topInclude.through.model.getTableName(), {
        attributes: [topInclude.through.model.primaryKeyField],
        include: Model._validateIncludedElements({
          model: topInclude.through.model,
          include: [{
            association: topAssociation.toTarget,
            required: true,
            where: topInclude.where,
            include: topInclude.include,
          }],
        }).include,
        model: topInclude.through.model,
        where: {
          [Op.and]: [
            literal([
              `${this.quoteTable(topParent.model.name)}.${this.quoteIdentifier(topParent.model.primaryKeyField)}`,
              `${this.quoteIdentifier(topInclude.through.model.name)}.${this.quoteIdentifier(topAssociation.identifierField)}`,
            ].join(' = ')),
            topInclude.through.where,
          ],
        },
        limit: 1,
        includeIgnoreAttributes: false,
      }, topInclude.through.model);
    } else {
      const isBelongsTo = topAssociation.associationType === 'BelongsTo';
      const sourceField = isBelongsTo
        ? topAssociation.identifierField
        : topAssociation.sourceKeyField || topParent.model.primaryKeyField;

      const targetField = isBelongsTo
        ? topAssociation.sourceKeyField || topInclude.model.primaryKeyField
        : topAssociation.identifierField;

      const join = [
        `${this.quoteIdentifier(topInclude.as)}.${this.quoteIdentifier(targetField)}`,
        `${this.quoteTable(topParent.as || topParent.model.name)}.${this.quoteIdentifier(sourceField)}`,
      ].join(' = ');

      query = this.selectQuery(topInclude.model.getTableName(), {
        attributes: [targetField],
        include: Model._validateIncludedElements(topInclude).include,
        model: topInclude.model,
        where: {
          [Op.and]: [
            topInclude.where,
            { [Op.join]: literal(join) },
          ],
        },
        limit: 1,
        tableAs: topInclude.as,
        includeIgnoreAttributes: false,
      }, topInclude.model);
    }

    if (!topLevelInfo.options.where[Op.and]) {
      topLevelInfo.options.where[Op.and] = [];
    }

    topLevelInfo.options.where[`__${includeAs.internalAs}`] = literal([
      '(',
      query.replace(/;$/, ''),
      ')',
      'IS NOT NULL',
    ].join(' '));
  }

  /*
   * For a given include hierarchy creates a copy of it where only the required includes
   * are preserved.
   */
  #getRequiredClosure(include) {
    const copy = { ...include, attributes: [], include: [] };

    if (Array.isArray(include.include)) {
      copy.include = include.include
        .filter(i => i.required)
        .map(inc => this.#getRequiredClosure(inc));
    }

    return copy;
  }

  getQueryOrders(options, model, subQuery) {
    const mainQueryOrder = [];
    const subQueryOrder = [];

    if (Array.isArray(options.order)) {
      for (let order of options.order) {

        // wrap if not array
        if (!Array.isArray(order)) {
          order = [order];
        }

        if (
          subQuery
          && Array.isArray(order)
          && order[0]
          && !(order[0] instanceof Association)
          && !(typeof order[0] === 'function' && order[0].prototype instanceof Model)
          && !(typeof order[0].model === 'function' && order[0].model.prototype instanceof Model)
          && !(typeof order[0] === 'string' && model && model.associations !== undefined && model.associations[order[0]])
        ) {
          subQueryOrder.push(this.quote(order, model, '->'));
        }

        if (subQuery) {
          // Handle case where sub-query renames attribute we want to order by,
          // see https://github.com/sequelize/sequelize/issues/8739
          const subQueryAttribute = options.attributes.find(a => Array.isArray(a) && a[0] === order[0] && a[1]);
          if (subQueryAttribute) {
            const modelName = this.quoteIdentifier(model.name);

            order[0] = new Col(this.#getAliasForField(modelName, subQueryAttribute[1], options) || subQueryAttribute[1]);
          }
        }

        mainQueryOrder.push(this.quote(order, model, '->'));
      }
    } else if (options.order instanceof SequelizeMethod) {
      const sql = this.quote(options.order, model, '->');
      if (subQuery) {
        subQueryOrder.push(sql);
      }

      mainQueryOrder.push(sql);
    } else {
      throw new TypeError('Order must be type of array or instance of a valid sequelize method.');
    }

    return { mainQueryOrder, subQueryOrder };
  }

  _throwOnEmptyAttributes(attributes, extraInfo = {}) {
    if (attributes.length > 0) {
      return;
    }

    const asPart = extraInfo.as && `as ${extraInfo.as}` || '';
    const namePart = extraInfo.modelName && `for model '${extraInfo.modelName}'` || '';
    const message = `Attempted a SELECT query ${namePart} ${asPart} without selecting any columns`;
    throw new QueryError(message.replace(/ +/g, ' '));
  }

  selectFromTableFragment(options, model, attributes, tables, mainTableAs) {
    this._throwOnEmptyAttributes(attributes, { modelName: model && model.name, as: mainTableAs });

    let fragment = `SELECT ${attributes.join(', ')} FROM ${tables}`;

    if (mainTableAs) {
      fragment += ` AS ${mainTableAs}`;
    }

    if (options.indexHints && this._dialect.supports.indexHints) {
      for (const hint of options.indexHints) {
        if (IndexHints[hint.type]) {
          fragment += ` ${IndexHints[hint.type]} INDEX (${hint.values.map(indexName => this.quoteIdentifiers(indexName)).join(',')})`;
        }
      }
    }

    return fragment;
  }

  /**
   * Returns an SQL fragment for adding result constraints.
   *
   * @param options An object with selectQuery options.
   * @param options.offset
   * @param options.limit
   * @returns         The generated sql query.
   * @private
   */
  addLimitAndOffset(options: { offset: string, limit: string }) {
    let fragment = '';

    if (options.offset != null && options.limit == null) {
      fragment += ` LIMIT ${this.escape(options.offset)}, ${10_000_000_000_000}`;
    } else if (options.limit != null) {
      if (options.offset != null) {
        fragment += ` LIMIT ${this.escape(options.offset)}, ${this.escape(options.limit)}`;
      } else {
        fragment += ` LIMIT ${this.escape(options.limit)}`;
      }
    }

    return fragment;
  }

  handleSequelizeMethod(
    smth: SequelizeMethod,
    tableName?: TableName,
    model?: ModelStatic<any>,
    options?: WhereBuilderOptions,
    prepend?: boolean,
  ): string {
    let result;

    if (smth instanceof Where) {
      let value = smth.logic;
      let key;

      if (Object.prototype.hasOwnProperty.call(this.OperatorMap, smth.comparator)) {
        smth.comparator = this.OperatorMap[smth.comparator];
      }

      if (smth.attribute instanceof SequelizeMethod) {
        key = this.getWhereConditions(smth.attribute, tableName, model, options, prepend);
      } else {
        key = `${this.quoteTable(smth.attribute.Model.name)}.${this.quoteIdentifier(smth.attribute.field || smth.attribute.fieldName)}`;
      }

      if (value && value instanceof SequelizeMethod) {
        value = this.getWhereConditions(value, tableName, model, options, prepend);

        if (value === 'NULL') {
          if (smth.comparator === '=') {
            smth.comparator = 'IS';
          }

          if (smth.comparator === '!=') {
            smth.comparator = 'IS NOT';
          }
        }

        return [key, value].join(` ${smth.comparator} `);
      }

      if (isPlainObject(value)) {
        // TODO
        // if (smth.attribute instanceof SequelizeMethod) {
        // throw new TypeError('');
        // }

        return this.whereItemQuery(smth.attribute, value, { model });
      }

      if ([this.OperatorMap[Op.between], this.OperatorMap[Op.notBetween]].includes(smth.comparator)) {
        value = `${this.escape(value[0])} AND ${this.escape(value[1])}`;
      } else if (typeof value === 'boolean') {
        value = this.booleanValue(value);
      } else {
        value = this.escape(value);
      }

      if (value === 'NULL') {
        if (smth.comparator === '=') {
          smth.comparator = 'IS';
        }

        if (smth.comparator === '!=') {
          smth.comparator = 'IS NOT';
        }
      }

      return [key, value].join(` ${smth.comparator} `);
    }

    if (smth instanceof Literal) {
      return smth.val;
    }

    if (smth instanceof Cast) {
      if (smth.val instanceof SequelizeMethod) {
        result = this.handleSequelizeMethod(smth.val, tableName, model, options, prepend);
      } else if (isPlainObject(smth.val)) {
        result = this.whereItemsQuery(smth.val);
      } else {
        result = this.escape(smth.val);
      }

      return `CAST(${result} AS ${smth.type.toUpperCase()})`;
    }

    if (smth instanceof Fn) {
      return `${smth.fn}(${
        smth.args.map(arg => {
          if (arg instanceof SequelizeMethod) {
            return this.handleSequelizeMethod(arg, tableName, model, options, prepend);
          }

          // TODO: why is WhereOptions allowed here?
          if (isPlainObject(arg)) {
            // @ts-expect-error 'Date' is an object but it's not a plain object
            return this.whereItemsQuery(arg);
          }

          return this.escape(typeof arg === 'string' ? arg.replace('$', '$$$') : arg);
        }).join(', ')
      })`;
    }

    if (smth instanceof Col) {
      if (smth.col.startsWith('*')) {
        return '*';
      }

      return this.quote(smth.col, model);
    }

    throw new TypeError(`Unknown Sequelize Method ${smth.constructor.name}`);
  }

  whereQuery(where: WhereOptions, options?: WhereBuilderOptions) {
    const query = this.whereItemsQuery(where, options);
    if (query && query.length > 0) {
      return `WHERE ${query}`;
    }

    return '';
  }

  whereItemsQuery(where: WhereOptions, options?: WhereBuilderOptions, logicalBinding: string = 'AND') {
    if (
      where === null
      || where === undefined
      || getComplexSize(where) === 0
    ) {
      // NO OP
      return '';
    }

    if (typeof where === 'string') {
      throw new TypeError('Support for `{where: \'raw query\'}` has been removed.');
    }

    const items = [];

    if (!logicalBinding.startsWith(' ')) {
      logicalBinding = ` ${logicalBinding} `;
    }

    if (isPlainObject(where)) {
      for (const prop of getComplexKeys(where)) {
        const item = where[prop];
        items.push(this.whereItemQuery(prop, item, options));
      }
    } else {
      items.push(this.whereItemQuery(undefined, where, options));
    }

    return items.length && items.filter(item => item && item.length).join(logicalBinding) || '';
  }

  whereItemQuery(key: string | symbol, value, options: WhereBuilderOptions = {}) {
    if (value === undefined) {
      throw new Error(`WHERE parameter "${String(key)}" has invalid "undefined" value`);
    }

    if (typeof key === 'string' && key.includes('.') && options.model) {
      const keyParts = key.split('.');

      if (
        options.model.rawAttributes[keyParts[0]]
        && options.model.rawAttributes[keyParts[0]].type instanceof DataTypes.JSON
      ) {
        const tmp = {};
        const field = options.model.rawAttributes[keyParts[0]];
        set(tmp, keyParts.slice(1), value);

        return this.whereItemQuery(field.field || keyParts[0], tmp, { field, ...options });
      }
    }

    const field = this.#findField(key, options);
    const fieldType = field?.type ?? options.type;

    const bPlainObject = isPlainObject(value);
    const isArray = !bPlainObject && Array.isArray(value);
    key = this.OperatorsAliasMap && this.OperatorsAliasMap[key] || key;
    if (bPlainObject) {
      value = this._replaceAliases(value);
    }

    const valueKeys = bPlainObject && getComplexKeys(value);

    if (key === undefined) {
      if (typeof value === 'string') {
        return value;
      }

      if (bPlainObject && valueKeys.length === 1) {
        return this.whereItemQuery(valueKeys[0], value[valueKeys[0]], options);
      }
    }

    if (value === null) {
      const opValue = options.bindParam ? 'NULL' : this.escape(value, field);

      return this._joinKeyValue(key, opValue, this.OperatorMap[Op.is], options.prefix);
    }

    if (!value) {
      const opValue = options.bindParam ? this.format(value, field, options, options.bindParam) : this.escape(value, field);

      return this._joinKeyValue(key, opValue, this.OperatorMap[Op.eq], options.prefix);
    }

    if (value instanceof SequelizeMethod && !(key !== undefined && value instanceof Fn)) {
      return this.handleSequelizeMethod(value);
    }

    // Convert where: [] to Op.and if possible, else treat as literal/replacements
    if (key === undefined && isArray) {
      if (canTreatArrayAsAnd(value)) {
        key = Op.and;
      } else {
        throw new Error('Support for literal replacements in the `where` object has been removed.');
      }
    }

    if (key === Op.or || key === Op.and || key === Op.not) {
      return this._whereGroupBind(key, value, options);
    }

    if (value[Op.or]) {
      return this._whereBind(this.OperatorMap[Op.or], key, value[Op.or], options);
    }

    if (value[Op.and]) {
      return this._whereBind(this.OperatorMap[Op.and], key, value[Op.and], options);
    }

    if (isArray && fieldType instanceof DataTypes.ARRAY) {
      const opValue = options.bindParam ? this.format(value, field, options, options.bindParam) : this.escape(value, field);

      return this._joinKeyValue(key, opValue, this.OperatorMap[Op.eq], options.prefix);
    }

    if (bPlainObject && fieldType instanceof DataTypes.JSON && options.json !== false) {
      return this._whereJSON(key, value, options);
    }

    // If multiple keys we combine the different logic conditions
    if (bPlainObject && valueKeys.length > 1) {
      return this._whereBind(this.OperatorMap[Op.and], key, value, options);
    }

    if (isArray) {
      return this._whereParseSingleValueObject(key, field, Op.in, value, options);
    }

    if (bPlainObject) {
      if (this.OperatorMap[valueKeys[0]]) {
        return this._whereParseSingleValueObject(key, field, valueKeys[0], value[valueKeys[0]], options);
      }

      return this._whereParseSingleValueObject(key, field, this.OperatorMap[Op.eq], value, options);
    }

    if (key === Op.placeholder) {
      const opValue = options.bindParam ? this.format(value, field, options, options.bindParam) : this.escape(value, field);

      return this._joinKeyValue(this.OperatorMap[key], opValue, this.OperatorMap[Op.eq], options.prefix);
    }

    const opValue = options.bindParam ? this.format(value, field, options, options.bindParam) : this.escape(value, field);

    return this._joinKeyValue(key, opValue, this.OperatorMap[Op.eq], options.prefix);
  }

  #findField(key: string, options: { model?: ModelStatic<any>, field?: BuiltModelAttribute }): Nullish<BuiltModelAttribute> {
    if (options.field) {
      return options.field;
    }

    if (options.model && options.model.getAttributes() && options.model.getAttributes()[key]) {
      return options.model.getAttributes()[key];
    }

    if (options.model && options.model.fieldRawAttributesMap && options.model.fieldRawAttributesMap[key]) {
      return options.model.fieldRawAttributesMap[key];
    }

    return null;
  }

  // OR/AND/NOT grouping logic
  _whereGroupBind(key, value, options) {
    const binding = key === Op.or ? this.OperatorMap[Op.or] : this.OperatorMap[Op.and];
    const outerBinding = key === Op.not ? 'NOT ' : '';

    if (Array.isArray(value)) {
      value = value.map(item => {
        let itemQuery = this.whereItemsQuery(item, options, this.OperatorMap[Op.and]);
        if (itemQuery && itemQuery.length > 0 && (Array.isArray(item) || isPlainObject(item)) && getComplexSize(item) > 1) {
          itemQuery = `(${itemQuery})`;
        }

        return itemQuery;
      }).filter(item => item && item.length);

      value = value.length && value.join(binding);
    } else {
      value = this.whereItemsQuery(value, options, binding);
    }

    // Op.or: [] should return no data.
    // Op.not of no restriction should also return no data
    if ((key === Op.or || key === Op.not) && !value) {
      return '0 = 1';
    }

    return value ? `${outerBinding}(${value})` : undefined;
  }

  _whereBind(binding, key, value, options) {
    if (isPlainObject(value)) {
      value = getComplexKeys(value).map(prop => {
        const item = value[prop];

        return this.whereItemQuery(key, { [prop]: item }, options);
      });
    } else {
      value = value.map(item => this.whereItemQuery(key, item, options));
    }

    value = value.filter(item => item && item.length);

    return value.length > 0 ? `(${value.join(binding)})` : undefined;
  }

  _whereJSON(key, value, options) {
    const items = [];
    let baseKey = this.quoteIdentifier(key);
    if (options.prefix) {
      if (options.prefix instanceof Literal) {
        baseKey = `${this.handleSequelizeMethod(options.prefix)}.${baseKey}`;
      } else {
        baseKey = `${this.quoteTable(options.prefix)}.${baseKey}`;
      }
    }

    for (const op of getOperators(value)) {
      const where = {
        [op]: value[op],
      };
      items.push(this.whereItemQuery(key, where, { ...options, json: false }));
    }

    forOwn(value, (item, prop) => {
      this._traverseJSON(items, baseKey, prop, item, [prop]);
    });

    const result = items.join(this.OperatorMap[Op.and]);

    return items.length > 1 ? `(${result})` : result;
  }

  _traverseJSON(items, baseKey, prop, item, path) {
    let cast;

    if (path[path.length - 1].includes('::')) {
      const tmp = path[path.length - 1].split('::');
      cast = tmp[1];
      path[path.length - 1] = tmp[0];
    }

    let pathKey = this.jsonPathExtractionQuery(baseKey, path);

    if (isPlainObject(item)) {
      for (const op of getOperators(item)) {
        const value = this._toJSONValue(item[op]);
        let isJson = false;
        if (typeof value === 'string' && op === Op.contains) {
          try {
            JSON.stringify(value);
            isJson = true;
          } catch {
            // failed to parse, is not json so isJson remains false
          }
        }

        pathKey = this.jsonPathExtractionQuery(baseKey, path, isJson);
        items.push(this.whereItemQuery(this._castKey(pathKey, value, cast), { [op]: value }));
      }

      forOwn(item, (value, itemProp) => {
        this._traverseJSON(items, baseKey, itemProp, value, [...path, itemProp]);
      });

      return;
    }

    item = this._toJSONValue(item);
    items.push(this.whereItemQuery(this._castKey(pathKey, item, cast), { [Op.eq]: item }));
  }

  _toJSONValue(value) {
    return value;
  }

  _castKey(key, value, cast, json) {
    cast = cast || this._getJsonCast(Array.isArray(value) ? value[0] : value);
    if (cast) {
      return new Literal(this.handleSequelizeMethod(new Cast(new Literal(key), cast, json)));
    }

    return new Literal(key);
  }

  _getJsonCast(value) {
    if (typeof value === 'number') {
      return 'double precision';
    }

    if (value instanceof Date) {
      return 'timestamptz';
    }

    if (typeof value === 'boolean') {
      return 'boolean';
    }

    return '';
  }

  _joinKeyValue(key, value, comparator, prefix) {
    if (!key) {
      return value;
    }

    if (comparator === undefined) {
      throw new Error(`${key} and ${value} has no comparator`);
    }

    key = this.#getSafeKey(key, prefix);

    return [key, value].join(` ${comparator} `);
  }

  #getSafeKey(key: string | SequelizeMethod, prefix: TableName) {
    if (key instanceof SequelizeMethod) {
      key = this.handleSequelizeMethod(key);

      return this.#prefixKey(this.handleSequelizeMethod(key), prefix);
    }

    if (isColString(key)) {
      let keyParts = key.slice(1, 1 + key.length - 2).split('.');

      if (keyParts.length > 2) {
        keyParts = [
          // join the tables by -> to match out internal namings
          keyParts.slice(0, -1).join('->'),
          keyParts[keyParts.length - 1],
        ];
      }

      return keyParts.map(identifier => this.quoteIdentifier(identifier)).join('.');
    }

    return this.#prefixKey(this.quoteIdentifier(key), prefix);
  }

  #prefixKey(key: string, prefix: TableName): string {
    if (prefix) {
      if (prefix instanceof Literal) {
        return `${this.handleSequelizeMethod(prefix)}.${key}`;
      }

      return `${this.quoteTable(prefix)}.${key}`;
    }

    return key;
  }

  _whereParseSingleValueObject(key, field, prop, value, options) {
    if (prop === Op.not) {
      if (Array.isArray(value)) {
        prop = Op.notIn;
      } else if (value !== null && value !== true && value !== false) {
        prop = Op.ne;
      }
    }

    let comparator = this.OperatorMap[prop] || this.OperatorMap[Op.eq];

    switch (prop) {
      case Op.in:
      case Op.notIn:
        if (value instanceof Literal) {
          return this._joinKeyValue(key, value.val, comparator, options.prefix);
        }

        if (value.length > 0) {
          return this._joinKeyValue(key, `(${value.map(item => this.escape(item, field, { where: true })).join(', ')})`, comparator, options.prefix);
        }

        if (comparator === this.OperatorMap[Op.in]) {
          return this._joinKeyValue(key, '(NULL)', comparator, options.prefix);
        }

        return '';
      case Op.any:
      case Op.all:
        comparator = `${this.OperatorMap[Op.eq]} ${comparator}`;
        if (value[Op.values]) {
          return this._joinKeyValue(key, `(VALUES ${value[Op.values].map(item => `(${this.escape(item)})`).join(', ')})`, comparator, options.prefix);
        }

        return this._joinKeyValue(key, `(${this.escape(value, field)})`, comparator, options.prefix);
      case Op.between:
      case Op.notBetween:
        return this._joinKeyValue(key, `${this.escape(value[0], field)} AND ${this.escape(value[1], field)}`, comparator, options.prefix);
      case Op.raw:
        throw new Error('The `$raw` where property is no longer supported.  Use `sequelize.literal` instead.');
      case Op.col:
        comparator = this.OperatorMap[Op.eq];
        value = value.split('.');

        if (value.length > 2) {
          value = [
            // join the tables by -> to match out internal namings
            value.slice(0, -1).join('->'),
            value[value.length - 1],
          ];
        }

        return this._joinKeyValue(key, value.map(identifier => this.quoteIdentifier(identifier)).join('.'), comparator, options.prefix);
      case Op.startsWith:
      case Op.endsWith:
      case Op.substring: {
        comparator = this.OperatorMap[Op.like];

        if (value instanceof Literal) {
          value = value.val;
        }

        let pattern = `${value}%`;

        if (prop === Op.endsWith) {
          pattern = `%${value}`;
        }

        if (prop === Op.substring) {
          pattern = `%${value}%`;
        }

        return this._joinKeyValue(key, this.escape(pattern), comparator, options.prefix);
      }

      default:
    }

    const escapeOptions = {
      acceptStrings: comparator.includes(this.OperatorMap[Op.like]),
    };

    if (isPlainObject(value)) {
      if (value[Op.col]) {
        return this._joinKeyValue(key, this.whereItemQuery(null, value), comparator, options.prefix);
      }

      if (value[Op.any]) {
        escapeOptions.isList = true;

        return this._joinKeyValue(key, `(${this.escape(value[Op.any], field, escapeOptions)})`, `${comparator} ${this.OperatorMap[Op.any]}`, options.prefix);
      }

      if (value[Op.all]) {
        escapeOptions.isList = true;

        return this._joinKeyValue(key, `(${this.escape(value[Op.all], field, escapeOptions)})`, `${comparator} ${this.OperatorMap[Op.all]}`, options.prefix);
      }
    }

    if (value === null && comparator === this.OperatorMap[Op.eq]) {
      return this._joinKeyValue(key, this.escape(value, field, escapeOptions), this.OperatorMap[Op.is], options.prefix);
    }

    if (value === null && comparator === this.OperatorMap[Op.ne]) {
      return this._joinKeyValue(key, this.escape(value, field, escapeOptions), this.OperatorMap[Op.not], options.prefix);
    }

    return this._joinKeyValue(key, this.escape(value, field, escapeOptions), comparator, options.prefix);
  }

  /**
   * Takes something and transforms it into values of a where condition.
   *
   * @param smth
   * @param tableName
   * @param factory
   * @param options
   * @param prepend
   * @private
  */
  // https://github.com/sequelize/sequelize/pull/14020 will re-build the WHERE generator,
  // so it's not necessary to type smth properly right now.
  getWhereConditions(
    smth: any,
    tableName?: TableName,
    factory?: ModelStatic<any>,
    options?: WhereBuilderOptions,
    prepend: boolean = true,
  ) {
    const where = {};

    if (Array.isArray(tableName)) {
      tableName = tableName[0];
      if (Array.isArray(tableName)) {
        tableName = tableName[1];
      }
    }

    if (smth && smth instanceof SequelizeMethod) { // Checking a property is cheaper than a lot of instanceof calls
      return this.handleSequelizeMethod(smth, tableName, factory, options, prepend);
    }

    if (isPlainObject(smth)) {
      return this.whereItemsQuery(smth, {
        model: factory,
        prefix: prepend ? tableName : undefined,
        type: options?.type,
      });
    }

    if (typeof smth === 'number') {
      let primaryKeys = factory ? Object.keys(factory.primaryKeys) : [];

      if (primaryKeys.length > 0) {
        // Since we're just a number, assume only the first key
        primaryKeys = primaryKeys[0];
      } else {
        primaryKeys = 'id';
      }

      where[primaryKeys] = smth;

      return this.whereItemsQuery(where, {
        model: factory,
        prefix: prepend && tableName,
      });
    }

    if (typeof smth === 'string') {
      return this.whereItemsQuery(smth, {
        model: factory,
        prefix: prepend && tableName,
      });
    }

    if (Buffer.isBuffer(smth)) {
      return this.escape(smth);
    }

    if (Array.isArray(smth)) {
      if (smth.length === 0 || smth.length > 0 && smth[0].length === 0) {
        return '1=1';
      }

      if (canTreatArrayAsAnd(smth)) {
        const _smth = { [Op.and]: smth };

        return this.getWhereConditions(_smth, tableName, factory, options, prepend);
      }

      throw new Error('Support for literal replacements in the `where` object has been removed.');
    }

    if (smth === null) {
      return this.whereItemsQuery(smth, {
        model: factory,
        prefix: prepend ? tableName : undefined,
      });
    }

    return '1=1';
  }

  /**
   * A recursive parser for nested where conditions
   *
   * @param conditions
   * @param path
   */
  parseConditionObject(
    conditions: { [key: string]: unknown },
    path: string[] = [],
  ): Array<{ path: string[], value: unknown }> {
    return reduce(conditions, (result: Array<{ path: string[], value: unknown }>, value: unknown, key: string) => {
      const currentPath = [...path, key];

      if (isObject(value)) {
        // Recursively parse objects
        result.push(...this.parseConditionObject(value, currentPath));

        return result;
      }

      result.push({ path: currentPath, value });

      return result;
    }, []);
  }

  booleanValue(value) {
    return value;
  }

  // ========================== OPERATORS ==========================

  OperatorMap = {
    [Op.eq]: '=',
    [Op.ne]: '!=',
    [Op.gte]: '>=',
    [Op.gt]: '>',
    [Op.lte]: '<=',
    [Op.lt]: '<',
    [Op.not]: 'IS NOT',
    [Op.is]: 'IS',
    [Op.in]: 'IN',
    [Op.notIn]: 'NOT IN',
    [Op.like]: 'LIKE',
    [Op.notLike]: 'NOT LIKE',
    [Op.iLike]: 'ILIKE',
    [Op.notILike]: 'NOT ILIKE',
    [Op.startsWith]: 'LIKE',
    [Op.endsWith]: 'LIKE',
    [Op.substring]: 'LIKE',
    [Op.regexp]: '~',
    [Op.notRegexp]: '!~',
    [Op.iRegexp]: '~*',
    [Op.notIRegexp]: '!~*',
    [Op.between]: 'BETWEEN',
    [Op.notBetween]: 'NOT BETWEEN',
    [Op.overlap]: '&&',
    [Op.contains]: '@>',
    [Op.contained]: '<@',
    [Op.adjacent]: '-|-',
    [Op.strictLeft]: '<<',
    [Op.strictRight]: '>>',
    [Op.noExtendRight]: '&<',
    [Op.noExtendLeft]: '&>',
    [Op.any]: 'ANY',
    [Op.all]: 'ALL',
    [Op.and]: ' AND ',
    [Op.or]: ' OR ',
    [Op.col]: 'COL',
    [Op.placeholder]: '$$PLACEHOLDER$$',
    [Op.match]: '@@',
  };

  OperatorsAliasMap = {};

  setOperatorsAliases(aliases) {
    if (!aliases || isEmpty(aliases)) {
      this.OperatorsAliasMap = false;
    } else {
      this.OperatorsAliasMap = { ...aliases };
    }
  }

  _replaceAliases(orig) {
    const obj = {};
    if (!this.OperatorsAliasMap) {
      return orig;
    }

    for (const op of getOperators(orig)) {
      const item = orig[op];
      if (isPlainObject(item)) {
        obj[op] = this._replaceAliases(item);
      } else {
        obj[op] = item;
      }
    }

    forOwn(orig, (item, prop) => {
      prop = this.OperatorsAliasMap[prop] || prop;
      if (isPlainObject(item)) {
        item = this._replaceAliases(item);
      }

      obj[prop] = item;
    });

    return obj;
  }

  // ========================== TRANSACTIONS ==========================

  /**
   * Returns a query that sets the transaction isolation level.
   *
   * @param value   The isolation level.
   * @param options An object with options.
   * @param options.parent
   * @returns         The generated sql query.
   * @private
   */
  setIsolationLevelQuery(value: ISOLATION_LEVELS, options: { parent: Transaction | null }): string {
    if (options.parent) {
      return '';
    }

    return `SET TRANSACTION ISOLATION LEVEL ${value};`;
  }

  generateTransactionId() {
    return uuidv4();
  }

  /**
   * Returns a query that starts a transaction.
   *
   * @param transaction
   * @returns         The generated sql query.
   * @private
   */
  startTransactionQuery(transaction: Transaction): string {
    if (transaction.parent) {
      // force quoting of savepoint identifiers for postgres
      return `SAVEPOINT ${this.quoteIdentifier(transaction.name, true)};`;
    }

    return 'START TRANSACTION;';
  }

  deferConstraintsQuery() {}

  setConstraintQuery() {}

  setDeferredQuery(_constraints: string[]): string {
    throw new Error(`setDeferredQuery is not implemented in ${this.constructor.name}`);
  }

  setImmediateQuery(_constraints: string[]): string {
    throw new Error(`setImmediateQuery is not implemented in ${this.constructor.name}`);
  }

  /**
   * Returns a query that commits a transaction.
   *
   * @param transaction An object with options.
   * @returns         The generated sql query.
   * @private
   */
  commitTransactionQuery(transaction: Transaction): string {
    if (transaction.parent) {
      return '';
    }

    return 'COMMIT;';
  }

  /**
   * Returns a query that rollbacks a transaction.
   *
   * @param transaction
   * @returns         The generated sql query.
   * @private
   */
  rollbackTransactionQuery(transaction: Transaction): string {
    if (transaction.parent) {
      // force quoting of savepoint identifiers for postgres
      return `ROLLBACK TO SAVEPOINT ${this.quoteIdentifier(transaction.name, true)};`;
    }

    return 'ROLLBACK;';
  }
}

