import isObject from 'lodash/isObject';
import type { Op, WhereOperators, WhereLeftOperand } from '..';
import type { Escapable } from '../sql-string.js';

/**
 * Utility functions for representing SQL functions, and columns that should be escaped.
 * Please do not use these functions directly, use Sequelize.fn and Sequelize.col instead.
 *
 * @private
 */
export class SequelizeMethod {}

/**
 * Do not use me directly. Use {@link Sequelize.fn}
 */
export class Fn extends SequelizeMethod {
  readonly fn: string;

  readonly args: Array<Escapable | SequelizeMethod>;

  constructor(
    fn: Fn['fn'],
    ...args: Fn['args']
  ) {
    super();
    this.fn = fn;
    this.args = args ?? [];
  }

  clone(): Fn {
    return new Fn(this.fn, this.args);
  }
}

/**
 * Do not use me directly. Use {@link Sequelize.col}
 */
export class Col extends SequelizeMethod {
  readonly col: string;

  constructor(col: string) {
    super();
    this.col = col;
  }
}

/**
 * Do not use me directly. Use {@link Sequelize.cast}
 */
export class Cast extends SequelizeMethod {
  readonly val: any;
  readonly type: string;
  private readonly json: boolean;

  constructor(val: unknown, type: string = '', json: boolean = false) {
    super();
    this.val = val;
    this.type = type.trim();
    this.json = json;
  }
}

/**
 * Do not use me directly. Use {@link Sequelize.literal}
 */
export class Literal extends SequelizeMethod {
  /** this (type-only) brand prevents TypeScript from thinking Cast is assignable to Literal because they share the same shape */
  declare private readonly brand: 'literal';

  readonly val: string;

  constructor(val: string) {
    super();
    this.val = val;
  }
}

/**
 * Do not use me directly. Use {@link Sequelize.json}
 */
export class Json extends SequelizeMethod {
  readonly conditions?: { [key: string]: any };
  private readonly path?: string;
  private readonly value?: string | number | boolean | null;

  constructor(
    conditionsOrPath: { [key: string]: any } | string,
    value?: string | number | boolean | null,
  ) {
    super();

    if (typeof conditionsOrPath === 'string') {
      this.path = conditionsOrPath;

      if (value) {
        this.value = value;
      }
    } else if (isObject(conditionsOrPath)) {
      this.conditions = conditionsOrPath;
    }
  }
}

/**
 * Do not use me directly. Use {@link Sequelize.where}
 */
export class Where<Operator extends keyof WhereOperators = typeof Op.eq> extends SequelizeMethod {
  // TODO [=7]: rename to leftOperand after typescript migration
  readonly attribute: WhereLeftOperand;
  // TODO [=7]: rename to operator after typescript migration
  // TODO: make readonly again during https://github.com/sequelize/sequelize/pull/14020 rewrite
  comparator: string | Operator;
  // TODO [=7]: rename to rightOperand after typescript migration
  readonly logic: WhereOperators[Operator] | any;

  constructor(leftOperand: WhereLeftOperand, operator: Operator, rightOperand: WhereOperators[Operator]);
  constructor(leftOperand: WhereLeftOperand, operator: string, rightOperand: any);
  constructor(leftOperand: WhereLeftOperand, rightOperand: WhereOperators[typeof Op.eq]);
  constructor(
    leftOperand: WhereLeftOperand,
    operatorOrRightOperand: string | Operator | WhereOperators[Operator],
    rightOperand?: WhereOperators[Operator] | any,
  ) {
    super();

    this.attribute = leftOperand;

    if (rightOperand !== undefined) {
      this.logic = rightOperand;
      this.comparator = operatorOrRightOperand;
    } else {
      this.logic = operatorOrRightOperand;
      this.comparator = '=';
    }
  }
}
