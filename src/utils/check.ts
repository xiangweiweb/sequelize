import isEmpty from 'lodash/isEmpty';
import _isPlainObject from 'lodash/isPlainObject';
import type { DataType, ModelStatic } from '..';
import { getOperators } from './format';
import { Where } from './sequelize-method';
// eslint-disable-next-line import/order -- caused by temporarily mixing require with import
import { Model } from '..';

const DataTypes = require('../data-types');

export function isPrimitive(val: any): val is string | number | boolean {
  const type = typeof val;

  return ['string', 'number', 'boolean'].includes(type);
}

/**
 * Like lodash's {@link _isPlainObject}, but typed
 *
 * @param val
 */
export function isPlainObject(val: any): val is object {
  return _isPlainObject(val);
}

export function isModelSubclass(val: any): val is ModelStatic<any> {
  return typeof val === 'function' && val.prototype instanceof Model;
}

/**
 * Returns whether `value` is using the nested syntax for attributes.
 *
 * @param value The attribute reference to check.
 *
 * @example
 * isColString('$id$'); // true
 * isColString('$project.name$'); // true
 * isColString('name'); // false
 */
export function isColString(value: any): value is string {
  return (
    typeof value === 'string'
    && value.startsWith('$')
    && value.endsWith('$')
  );
}

export function canTreatArrayAsAnd(arr: unknown[]): arr is Array<object | Where> {
  return arr.some(arg => isPlainObject(arg) || arg instanceof Where);
}

/**
 * Determine if the default value provided exists and can be described
 * in a db schema using the DEFAULT directive.
 *
 * @param value Any default value.
 * @private
 */
export function defaultValueSchemable(value: DataType): boolean {
  if (value === undefined) {
    return false;
  }

  // TODO this will be schemable when all supported db
  //  have been normalized for this case
  if (value instanceof DataTypes.NOW) {
    return false;
  }

  if (value instanceof DataTypes.UUIDV1 || value instanceof DataTypes.UUIDV4) {
    return false;
  }

  return typeof value !== 'function';
}

/**
 * Returns true if a where clause is empty, even with Symbols
 *
 * @param obj
 */
export function isWhereEmpty(obj: object): boolean {
  return Boolean(obj) && isEmpty(obj) && getOperators(obj).length === 0;
}
