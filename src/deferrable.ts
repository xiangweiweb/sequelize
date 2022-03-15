import type { AbstractQueryGenerator } from './dialects/abstract/query-generator.js';
import { classToInvokable } from './utils/index.js';

/**
 * Can be used to
 * make foreign key constraints deferrable and to set the constaints within a
 * transaction. This is only supported in PostgreSQL.
 *
 * The foreign keys can be configured like this. It will create a foreign key
 * that will check the constraints immediately when the data was inserted.
 *
 * ```js
 * class MyModel extends Model {}
 * MyModel.init({
 *   foreign_id: {
 *     type: Sequelize.INTEGER,
 *     references: {
 *       model: OtherModel,
 *       key: 'id',
 *       deferrable: Sequelize.Deferrable.INITIALLY_IMMEDIATE
 *     }
 *   }
 * }, { sequelize });
 * ```
 *
 * The constraints can be configured in a transaction like this. It will
 * trigger a query once the transaction has been started and set the constraints
 * to be checked at the very end of the transaction.
 *
 * ```js
 * sequelize.transaction({
 *   deferrable: Sequelize.Deferrable.SET_DEFERRED
 * });
 * ```
 */
class ABSTRACT {
  static toString(queryGenerator: AbstractQueryGenerator) {
    return new this().toString(queryGenerator);
  }

  toString(queryGenerator: AbstractQueryGenerator) {
    return this.toSql(queryGenerator);
  }

  toSql(_queryGenerator: AbstractQueryGenerator) {
    throw new Error('toSql implementation missing');
  }
}

class INITIALLY_DEFERRED extends ABSTRACT {
  toSql() {
    return 'DEFERRABLE INITIALLY DEFERRED';
  }
}

class INITIALLY_IMMEDIATE extends ABSTRACT {
  toSql() {
    return 'DEFERRABLE INITIALLY IMMEDIATE';
  }
}

/**
 * Will set the constraints to not deferred. This is the default in PostgreSQL and it make
 * it impossible to dynamically defer the constraints within a transaction.
 */
class NOT extends ABSTRACT {
  toSql() {
    return 'NOT DEFERRABLE';
  }
}

/**
 * Will trigger an additional query at the beginning of a
 * transaction which sets the constraints to deferred.
 */
class SET_DEFERRED extends ABSTRACT {
  private readonly constraints: string[];
  /**
   * @param constraints An array of constraint names. Will defer all constraints by default.
   */
  constructor(constraints: string[]) {
    super();
    this.constraints = constraints;
  }

  toSql(queryGenerator: AbstractQueryGenerator): string {
    return queryGenerator.setDeferredQuery(this.constraints);
  }
}

/**
 * Will trigger an additional query at the beginning of a
 * transaction which sets the constraints to immediately.
 *
 * @param constraints An array of constraint names. Will defer all constraints by default.
 */
class SET_IMMEDIATE extends ABSTRACT {
  private readonly constraints: string[];

  /**
   * @param constraints An array of constraint names. Will defer all constraints by default.
   */
  constructor(constraints: string[]) {
    super();
    this.constraints = constraints;
  }

  toSql(queryGenerator: AbstractQueryGenerator): string {
    return queryGenerator.setImmediateQuery(this.constraints);
  }
}

/**
 * A collection of properties related to deferrable constraints. It can be used to
 * make foreign key constraints deferrable and to set the constraints within a
 * transaction. This is only supported in PostgreSQL.
 *
 * The foreign keys can be configured like this. It will create a foreign key
 * that will check the constraints immediately when the data was inserted.
 *
 * ```js
 * sequelize.define('Model', {
 *   foreign_id: {
 *     type: Sequelize.INTEGER,
 *     references: {
 *       model: OtherModel,
 *       key: 'id',
 *       deferrable: Sequelize.Deferrable.INITIALLY_IMMEDIATE
 *     }
 *   }
 * });
 * ```
 *
 * The constraints can be configured in a transaction like this. It will
 * trigger a query once the transaction has been started and set the constraints
 * to be checked at the very end of the transaction.
 *
 * ```js
 * sequelize.transaction({
 *   deferrable: Sequelize.Deferrable.SET_DEFERRED
 * });
 * ```
 *
 * @property INITIALLY_DEFERRED    Use when declaring a constraint. Allow and enable by default this constraint's checks to be deferred at the end of transactions.
 * @property INITIALLY_IMMEDIATE   Use when declaring a constraint. Allow the constraint's checks to be deferred at the end of transactions.
 * @property NOT                   Use when declaring a constraint. Set the constraint to not deferred. This is the default in PostgreSQL and makes it impossible to dynamically defer the constraints within a transaction.
 * @property SET_DEFERRED          Use when declaring a transaction. Defer the deferrable checks involved in this transaction at commit.
 * @property SET_IMMEDIATE         Use when declaring a transaction. Execute the deferrable checks involved in this transaction immediately.
 */
const Deferrable = {
  INITIALLY_DEFERRED: classToInvokable(INITIALLY_DEFERRED),
  INITIALLY_IMMEDIATE: classToInvokable(INITIALLY_IMMEDIATE),
  NOT: classToInvokable(NOT),
  SET_DEFERRED: classToInvokable(SET_DEFERRED),
  SET_IMMEDIATE: classToInvokable(SET_IMMEDIATE),
};

export {
  ABSTRACT as AbstractDeferrable,
  Deferrable,
};
