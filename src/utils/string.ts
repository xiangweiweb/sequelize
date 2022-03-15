import _inflection from 'inflection';
import type { TableName } from '../../types/index.js';
import type { IndexField } from '../dialects/abstract/query-generator.js';
import { SequelizeMethod } from './sequelize-method.js';

/* Inflection */
type Inflection = typeof _inflection;

let inflection: Inflection = _inflection;

export function useInflection(newInflection: Inflection) {
  inflection = newInflection;
}

/* String utils */

export function camelizeIf(str: string, condition: boolean): string {
  let result = str;

  if (condition) {
    result = camelize(str);
  }

  return result;
}

export function camelize(str: string): string {
  return str.trim().replace(/[-_\s]+(.)?/g, (match, c) => c.toUpperCase());
}

export function underscoredIf(str: string, condition: boolean): string {
  let result = str;

  if (condition) {
    result = underscore(str);
  }

  return result;
}

export function underscore(str: string): string {
  return inflection.underscore(str);
}

export function spliceStr(
  str: string,
  index: number,
  count: number,
  add: string,
): string {
  return str.slice(0, index) + add + str.slice(index + count);
}

export function singularize(str: string): string {
  return inflection.singularize(str);
}

export function pluralize(str: string): string {
  return inflection.pluralize(str);
}

type NameIndexIndex = {
  fields: IndexField[],
  name?: string,
};

/**
 * @param index
 * @param index.fields
 * @param index.name
 * @param tableName
 *
 * @private
 */
export function inferIndexName(
  index: Readonly<NameIndexIndex>,
  tableName: Readonly<TableName>,
): string {
  if (index.name) {
    return index.name;
  }

  if (typeof tableName !== 'string' && tableName.tableName) {
    tableName = tableName.tableName;
  }

  const fields = index.fields.map(field => {
    if (typeof field === 'string') {
      return field;
    }

    if (field instanceof SequelizeMethod) {
      throw new TypeError(`Sequelize cannot infer the name of an index for you if you use ${field.constructor.name}, you need to provide one manually using the 'name' option.`);
    }

    return (typeof field === 'string' ? field : field.name);
  });

  return underscore(`${tableName}_${fields.join('_')}`);
}

/**
 * Stringify a value as JSON with some differences:
 * - bigints are stringified as a json string. (`safeStringifyJson({ val: 1n })` outputs `'{ "val": "1" }'`).
 *   This is because of a decision by TC39 to not support bigint in JSON.stringify https://github.com/tc39/proposal-bigint/issues/24
 *
 * @param stringifyTarget the value to stringify.
 * @returns the resulting json.
 */
export function safeStringifyJson(stringifyTarget: any): string {
  return JSON.stringify(stringifyTarget, (key, value) => {
    if (typeof value === 'bigint') {
      return String(value);
    }

    return value;
  });
}
