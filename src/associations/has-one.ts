import upperFirst from 'lodash/upperFirst';
import type { DataType } from '../data-types.js';
import type {
  CreateOptions,
  CreationAttributes,
  FindOptions,
  SaveOptions,
  Model,
  ModelStatic,
  AttributeNames,
  ModelAttributeColumnOptions,
  Attributes,
  BuiltModelName,
} from '../model';
import { Op } from '../operators';
import * as Utils from '../utils';
import { getColumnName } from '../utils';
import type { AssociationOptions } from './base';
import { Association } from './base';
import * as Helpers from './helpers';

/**
 * One-to-one association
 *
 * In the API reference below, add the name of the association to the method, e.g. for `User.hasOne(Project)` the getter will be `user.getProject()`.
 * This is almost the same as `belongsTo` with one exception - The foreign key will be defined on the target model.
 *
 * @see {@link Model.hasOne}
 */
export class HasOne<S extends Model = Model, T extends Model = Model> extends Association<S, T, HasOneOptions<S>> {
  associationType = 'HasOne';
  isSingleAssociation = true;

  sourceKey: string;
  sourceKeyAttribute: string;

  /**
   * The Column Name of the source key.
   */
  sourceKeyField: string;
  associationAccessor: string;

  /**
   * A column name
   */
  identifierField: string | undefined;

  readonly accessors: {
    get: string,
    set: string,
    create: string,
  };

  constructor(source: ModelStatic<S>, target: ModelStatic<T>, options: HasOneOptions<S>) {
    super(source, target, options);

    if (
      this.options.sourceKey
      && !this.source.getAttributes()[this.options.sourceKey]
    ) {
      throw new Error(`Unknown attribute "${this.options.sourceKey}" passed as sourceKey, define this attribute on model "${this.source.name}" first`);
    }

    // TODO: throw is source model has a composite primary key.

    this.sourceKey = this.options.sourceKey || this.source.primaryKeyAttribute;
    this.sourceKeyAttribute = this.sourceKey;
    this.sourceKeyField = this.source.getAttributes()[this.sourceKey].field || this.sourceKey;

    this.associationAccessor = this.as;
    this.options.useHooks = options.useHooks;

    if (this.target.getAttributes()[this.foreignKey]) {
      this.identifierField = getColumnName(this.target.getAttributes()[this.foreignKey]);
    }

    // Get singular name, trying to uppercase the first letter, unless the model forbids it
    const singular = upperFirst(this.options.name.singular);

    this.accessors = {
      get: `get${singular}`,
      set: `set${singular}`,
      create: `create${singular}`,
    };
  }

  /**
   * @private
   */
  _injectAttributes() {
    // the id is in the target table

    const newAttributes: Record<string, ModelAttributeColumnOptions> = {
      [this.foreignKey]: {
        type: this.options.keyType || this.source.rawAttributes[this.sourceKey].type,
        allowNull: true,
        ...this.foreignKeyAttribute,
      },
    };

    if (this.options.constraints !== false) {
      const target = this.target.rawAttributes[this.foreignKey] || newAttributes[this.foreignKey];
      this.options.onDelete = this.options.onDelete || (target.allowNull ? 'SET NULL' : 'CASCADE');
      this.options.onUpdate = this.options.onUpdate || 'CASCADE';
    }

    Helpers.addForeignKeyConstraints(newAttributes[this.foreignKey], this.source, this.options, this.sourceKeyField);

    this.target.mergeAttributes(newAttributes);

    this.identifierField = this.target.rawAttributes[this.foreignKey].field || this.foreignKey;

    Helpers.checkNamingCollision(this);

    return this;
  }

  /**
    * @param mixinTargetPrototype - a Model prototype, not an actual instance
    */
  mixin(mixinTargetPrototype: Model) {
    Helpers.mixinMethods(this, mixinTargetPrototype, ['get', 'set', 'create']);
  }

  /**
   * Get the associated instance.
   * See {@link Model.findOne} for a full explanation of options.
   *
   * This method is mixed-in the source model prototype. See {@link HasOneGetAssociationMixin}.
   *
   * @param instances source instances
   * @param options find options
   */
  // TODO: when is this called with an array? Is it ever?
  async get(instances: S | S[], options: HasOneGetAssociationMixinOptions<T>) {
    options = Utils.cloneDeep(options);

    let Target = this.target;
    if (Object.prototype.hasOwnProperty.call(options, 'scope')) {
      if (!options.scope) {
        Target = Target.unscoped();
      } else if (options.scope !== true) { // 'true' means default scope. Which is the same as not doing anything.
        Target = Target.scope(options.scope);
      }
    }

    if (options.schema != null) {
      Target = Target.schema(options.schema, options.schemaDelimiter);
    }

    if (!Array.isArray(instances)) {
      instances = [instances];
    }

    const where = Object.create(null);

    if (instances.length > 1) {
      where[this.foreignKey] = {
        [Op.in]: instances.map(instance => instance.get(this.sourceKey)),
      };
    } else {
      where[this.foreignKey] = instances[0].get(this.sourceKey);
    }

    // FIXME: scopes should be combined using AND instance of overwriting.
    if (this.scope) {
      Object.assign(where, this.scope);
    }

    options.where = options.where
      ? { [Op.and]: [where, options.where] }
      : where;

    if (instances.length > 1) {
      const results = await Target.findAll(options);
      const result: Record<any, T | null> = Object.create(null);
      for (const instance of instances) {
        // @ts-expect-error
        result[instance.get(this.sourceKey, { raw: true })] = null;
      }

      for (const instance of results) {
        // @ts-expect-error
        result[instance.get(this.foreignKey, { raw: true })] = instance;
      }

      return result;
    }

    return Target.findOne(options);
  }

  /**
   * Set the associated model.
   *
   * @param sourceInstance the source instance
   * @param associatedInstance An persisted instance or the primary key of an instance to associate with this. Pass `null` or `undefined` to remove the association.
   * @param options Options passed to getAssociation and `target.save`
   *
   * @returns
   */
  async set(sourceInstance: S, associatedInstance: T | null, options?: HasOneSetAssociationMixinOptions<T>) {
    options = { ...options, scope: false };

    // calls the 'get' mixin
    const oldInstance: T | null = await this.get(sourceInstance, options);

    const alreadyAssociated = oldInstance && associatedInstance && associatedInstance.equals(oldInstance);

    if (oldInstance && !alreadyAssociated) {
      oldInstance.set(this.foreignKey, null);

      await oldInstance.save({
        ...options,
        fields: [this.foreignKey],
        // TODO: what is this 'allowNull' for?
        // allowNull: [this.foreignKey],
        association: true,
      });
    }

    if (associatedInstance && !alreadyAssociated) {
      if (!(associatedInstance instanceof this.target)) {
        const tmpInstance = Object.create(null);
        tmpInstance[this.target.primaryKeyAttribute] = associatedInstance;
        associatedInstance = this.target.build(tmpInstance, {
          isNewRecord: false,
        });
      }

      Object.assign(associatedInstance, this.scope);
      associatedInstance.set(this.foreignKey, sourceInstance.get(this.sourceKeyAttribute));

      return associatedInstance.save(options);
    }

    return null;
  }

  /**
   * Create a new instance of the associated model and associate it with this.
   *
   * See {@link Model#create} for a full explanation of options.
   *
   * @param sourceInstance - the source instance
   * @param values - values to create associated model instance with
   * @param options - Options passed to `target.create` and setAssociation.
   *
   * @returns The created target model
   */
  async create(
    sourceInstance: S,
    // @ts-expect-error -- {} is not always assignable to 'values', but Target.create will enforce this, not us.
    values: CreationAttributes<T> = {},
    options: HasOneCreateAssociationMixinOptions<T> = {},
  ) {

    if (this.scope) {
      for (const attribute of Object.keys(this.scope)) {
        // @ts-expect-error
        values[attribute] = this.scope[attribute];
        if (options.fields) {
          options.fields.push(attribute);
        }
      }
    }

    // @ts-expect-error
    values[this.foreignKey] = sourceInstance.get(this.sourceKeyAttribute);
    if (options.fields) {
      options.fields.push(this.foreignKey);
    }

    return this.target.create(values, options);
  }

  verifyAssociationAlias(alias: string | BuiltModelName): boolean {
    if (typeof alias === 'string') {
      return this.as === alias;
    }

    if (alias?.singular) {
      return this.as === alias.singular;
    }

    return !this.isAliased;
  }
}

/**
 * Options provided when associating models with hasOne relationship
 */
export interface HasOneOptions<Source extends Model> extends AssociationOptions {

  /**
   * The name of the field to use as the key for the association in the source table. Defaults to the primary
   * key of the source table.
   *
   * This is the attribute the foreign key will target. Not to be confused with {@link AssociationOptions.foreignKey}.
   */
  sourceKey?: AttributeNames<Source>;

  /**
   * A string or a data type to represent the identifier in the table
   */
  keyType?: DataType;

  useHooks?: boolean;
}

/**
 * The options for the getAssociation mixin of the hasOne association.
 *
 * @see HasOneGetAssociationMixin
 */
export interface HasOneGetAssociationMixinOptions<M extends Model> extends FindOptions<Attributes<M>> {
  /**
   * Apply a scope on the related model, or remove its default scope by passing false.
   */
  scope?: string | string[] | boolean;

  schema?: string;
  schemaDelimiter?: string;
}

/**
 * The getAssociation mixin applied to models with hasOne.
 * An example of usage is as follows:
 *
 * ```js
 *
 * User.hasOne(Role);
 *
 * interface UserInstance extends Sequelize.Instance<UserInstance, UserAttrib>, UserAttrib {
 *  getRole: Sequelize.HasOneGetAssociationMixin<RoleInstance>;
 *  // setRole...
 *  // createRole...
 * }
 * ```
 *
 * @see https://sequelize.org/master/class/lib/associations/has-one.js~HasOne.html
 * @see Instance
 */
export type HasOneGetAssociationMixin<T extends Model> = (options?: HasOneGetAssociationMixinOptions<T>) => Promise<T>;

/**
 * The options for the setAssociation mixin of the hasOne association.
 *
 * @see HasOneSetAssociationMixin
 */
export interface HasOneSetAssociationMixinOptions<T extends Model>
  extends HasOneGetAssociationMixinOptions<T>, SaveOptions<any> {
  /**
   * Skip saving this after setting the foreign key if false.
   */
  save?: boolean;
}

/**
 * The setAssociation mixin applied to models with hasOne.
 * An example of usage is as follows:
 *
 * ```js
 *
 * User.hasOne(Role);
 *
 * interface UserInstance extends Sequelize.Instance<UserInstance, UserAttributes>, UserAttributes {
 *  // getRole...
 *  setRole: Sequelize.HasOneSetAssociationMixin<RoleInstance, RoleId>;
 *  // createRole...
 * }
 * ```
 *
 * @see https://sequelize.org/master/class/lib/associations/has-one.js~HasOne.html
 * @see Instance
 */
export type HasOneSetAssociationMixin<T extends Model, TModelPrimaryKey> = (
  newAssociation?: T | TModelPrimaryKey,
  options?: HasOneSetAssociationMixinOptions<T>
) => Promise<void>;

/**
 * The options for the createAssociation mixin of the hasOne association.
 *
 * @see HasOneCreateAssociationMixin
 */
export interface HasOneCreateAssociationMixinOptions<M extends Model>
  extends Omit<HasOneSetAssociationMixinOptions<M>, 'fields'>, CreateOptions<Attributes<M>> {}

/**
 * The createAssociation mixin applied to models with hasOne.
 * An example of usage is as follows:
 *
 * ```js
 *
 * User.hasOne(Role);
 *
 * interface UserInstance extends Sequelize.Instance<UserInstance, UserAttributes>, UserAttributes {
 *  // getRole...
 *  // setRole...
 *  createRole: Sequelize.HasOneCreateAssociationMixin<RoleAttributes>;
 * }
 * ```
 *
 * @see https://sequelize.org/master/class/lib/associations/has-one.js~HasOne.html
 * @see Instance
 */
export type HasOneCreateAssociationMixin<M extends Model> = (
  // TODO: omit the foreign key
  values?: CreationAttributes<M>,
  options?: HasOneCreateAssociationMixinOptions<M>
) => Promise<M>;