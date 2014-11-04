#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

import argparse
import datetime
import inspect
import json
import ndb_datastore
import os

verbose = False

def log(*args):
  global verbose
  if not verbose:
    return
  msg = ""
  msg += __file__
  msg += ":"
  msg += str(inspect.currentframe().f_back.f_lineno)
  msg += " "
  for a in args:
    msg += a.__str__()
    msg += " "
  print msg

# Global value holding the datastore.
datastore = None

# Initializes and opens the datastore.
# Only one datastore can be open at a time.
def setDatastore(d):
  global datastore
  if datastore is not None:
    raise ValueError("The datastore can be opened only once")
    return
  log("Opening specified datastore")
  datastore = d

# Closes the datastore.
def closeDatastore():
  global datastore
  if datastore is not None:
    log("Closing datastore")
    datastore.close()
  datastore = None

# Returns the datastore.
def getDatastore():
  global datastore
  return datastore

def initDatastoreFromParams():
  global verbose
  parser = argparse.ArgumentParser(description="Initializes the ndb datatore.")
  parser.add_argument("--datastore_type", choices=["leveldb", "lmdb", "memory"], default="leveldb", help="Which datastore implementation to use")
  parser.add_argument("--datastore_path", default="datastore.db", help="Path to a directory used to store the datastore files")
  parser.add_argument("--datastore_verbose", default=False, help="Log datastore actions and errors to standard output")
  args = parser.parse_args()
  verbose = args.datastore_verbose
  if args.datastore_type == "leveldb":
    import ndb_datastore_leveldb
    setDatastore(ndb_datastore_leveldb.LevelDBDatastore(args.datastore_path))
  elif args.datastore_type == "lmdb":
    import ndb_datastore_lmdb
    setDatastore(ndb_datastore_lmdb.LMDBDatastore(args.datastore_path))
  elif args.datastore_type == "memory":
    setDatastore(ndb_datastore.MemDatastore())
  else:
    raise ValueError("Bad datastore type in CLI args")

# Class used to define and store objects. Analogous to an SQL table.
# Derive this class to create a model.
# Properties (like SQL columns) must be defined as *class attributes* and derive Property (you should use one of the
# specializations provided below).
# Model instances use *instance attributes* with the same names as the *class attributes* properties to hold the
# native Python values of the properties. E.g. if UserModel.email is a StringProperty, userModel.email is a Python
# string (str or unicode).
# The key must be a non-empty string (str or unicode).
class Model:
  # Creates a Model instance with an optional key, and an optional list of initializers for the properties.
  def __init__(self, key=None, **kwds):
    self.key = key
    for k in kwds:
      self.__dict__[k] = kwds[k]
    pass

  @classmethod
  def get_datastore(cls):
    return getDatastore()

  # Returns the model instance for the given key if it exists, or raises a KeyError otherwise.
  @classmethod
  def get_by_id(cls, key):
    if not key:
      raise ValueError("Empty or missing key for model instance")
    encoded = cls.get_datastore().get(cls.__name__, key)
    if encoded is None:
      raise KeyError("No model instance found for given key")
    dict = json.loads(encoded)
    kwds = {}
    for k in cls.__dict__:
      if isinstance(cls.__dict__[k], Property):
        v = None
        if k in dict:
          v = dict[k]
        v = cls.__dict__[k]._from_datastore(v)
        kwds[k] = v
    return cls(key, **kwds)

  # Returns the model instance for the given key if it exists, or creates a new one, initializes the properties
  # using the provided property_initializers, and returns it.
  @classmethod
  def get_or_insert(cls, key, **property_initializers):
    try:
      return cls.get_by_id(key)
    except KeyError:
      obj = cls(key)
      for k in property_initializers:
        v = property_initializers[k]
        if k in cls.__dict__ and isinstance(cls.__dict__[k], Property):
          cls.__dict__[k].name = k
          obj.__dict__[k] = v
      obj.put()
      return cls.get_by_id(key)

  # Returns a Query object that can be used to iterate over the existing instances for this model.
  # Filters are optional and filters can also be added to the Query object afterwards.
  @classmethod
  def query(cls, *filters):
    q = Query(cls)
    for f in filters:
      q = q.filter(f)
    return q

  # Returns the name of this model, i.e. the name of the class deriving Model.
  @classmethod
  def kind(cls):
    return cls.__name__

  @classmethod
  def generateKey(cls):
    return str((datetime.datetime.now() - datetime.datetime.utcfromtimestamp(0)).total_seconds()).encode("hex") + str(os.urandom(16).encode("hex"))

  # Stores this model instance in the datastore, and returns the key.
  def put(self):
    if not self.key:
      self.key = self.generateKey()
    self.get_datastore().set(self.kind(), self.key, json.dumps(self._to_dict_datastore()))
    return self.key

  # Deletes this model instance from the datastore.
  def delete(self):
    if not self.key:
      raise KeyError("Cannot delete model instance without a key")
    self.get_datastore().delete(self.kind(), self.key)
    return self.key

  # Returns a Python dictionary containing the property values of this model instance.
  # Use include or exclude (lists of properties) to restrict the properties that are returned.
  def to_dict(self, include=None, exclude=None):
    dict = {}
    for k in self.__class__.__dict__:
      if isinstance(self.__class__.__dict__[k], Property):
        if include is not None:
          if self.__class__.__dict__[k] not in include:
            continue
        if exclude is not None:
          if self.__class__.__dict__[k] in exclude:
            continue
        v = None
        if k in self.__dict__:
          v = self.__dict__[k]
        self.__class__.__dict__[k].name = k
        v = self.__class__.__dict__[k].validate(v)
        dict[k] = v
    return dict

  # Returns a dictionary used to store this instance in the datastore.
  def _to_dict_datastore(self):
    dict = {}
    for k in self.__class__.__dict__:
      if isinstance(self.__class__.__dict__[k], Property):
        v = None
        if k in self.__dict__:
          v = self.__dict__[k]
        self.__class__.__dict__[k].name = k
        v = self.__class__.__dict__[k].validate(v)
        v = self.__class__.__dict__[k]._to_datastore(v)
        dict[k] = v
    return dict


# Class used to store query filters.
# You should not use it directly from outside this module.
class Filter:
  def __init__(self, property, operator, value):
    self.property = property
    self.propertyName = None # Set by Query
    self.operator = operator
    self.value = value

  # Returns True if the model instance obj is accepted by this filter.
  def _apply(self, obj):
    v = None
    if self.propertyName in obj.__dict__:
      v = obj.__dict__[self.propertyName]
    if self.operator == "=":
      return v == self.value
    if self.operator == "!=":
      return v < self.value or v > self.value
    if self.operator == "<":
      return v < self.value
    if self.operator == "<=":
      return v <= self.value
    if self.operator == ">":
      return v > self.value
    if self.operator == ">=":
      return v >= self.value
    if self.operator == "in":
      return v in self.value
    raise ValueError("Could not apply filter")


# Class used to store query sort orders by property.
# You should not use it directly from outside this module.
class SortOrder:
  def __init__(self, property, reversed=False):
    self.property = property
    self.propertyName = None # Set by Query
    self.reversed = reversed


# Class used to iterate over model instances from the datastore, and optionally apply filters and sorting.
# Do not create Query instances directly; call MyModel.query() instead.
class Query:
  def __init__(self, model):
    self.model = model
    self.filters = []
    self.sortOrders = []

  # Adds filter f to the queryinstance and returns the query instance.
  # A filter can be created by comparing a model property with a value, e.g.
  # q = q.filter(UserModel.email == "test@example.com").
  # The following operators are supported: ==, !=, <, <=, >, >= and in via the Property.IN function.
  def filter(self, f):
    assert(isinstance(f, Filter))
    f.propertyName = None
    for k in self.model.__dict__:
      if isinstance(self.model.__dict__[k], Property):
        if self.model.__dict__[k] is f.property:
          f.propertyName = k
    if f.propertyName is None:
      raise ValueError("Filter does not match any property")
    self.filters.append(f)
    return self

  # Adds a property to the list of properties used to sort the results.
  # Use a unary minus in front of the property to sort in descending order.
  # Example: q = q.order(-UserModel.email)
  # Note: sorting is done in memory after all the instances have been read from the datastore. With large numbers of
  # instances, you can run OOM.
  def order(self, *sortOrderValues):
    for sortOrder in sortOrderValues:
      assert(isinstance(sortOrder, Property) or isinstance(sortOrder, SortOrder))
      if isinstance(sortOrder, Property):
        sortOrder = SortOrder(sortOrder)
      sortOrder.propertyName = None
      for k in self.model.__dict__:
        if isinstance(self.model.__dict__[k], Property):
          if self.model.__dict__[k] is sortOrder.property:
            sortOrder.propertyName = k
      if sortOrder.propertyName is None:
        raise ValueError("Sort order does not match any property")
      self.sortOrders.append(sortOrder)
    return self

  # Returns an iterator for the results of the query. The values returned by the iterator are the model instances
  # matched by the query, sorted if specified or in an arbitrary order otherwise.
  # Example: users = [user for user in UserModel.query().filter(UserModel.age > 18).order(UserModel.email)]
  def iter(self):
    if not self.sortOrders:
      return QueryIterator(self)
    else:
      return QueryOrderedIterator(self)

  # Returns True if the model instance obj matches the filters.
  def _apply_filters(self, obj):
    for f in self.filters:
      if not f._apply(obj):
        return False
    return True


# Helper class used to iterate over the query results without sorting.
# You should not have to care about it from outside the module.
class QueryIterator:
  def __init__(self, query):
    self.query = query
    self.datastoreIterator = self.query.model.get_datastore().iter(self.query.model.kind())

  def __iter__(self):
    return self

  def next(self):
    while True:
      key = self.datastoreIterator.next()
      obj = self.query.model.get_by_id(key)
      if self.query._apply_filters(obj):
        return obj


# Helper class used to iterate over the query results with sorting.
# You should not have to care about it from outside the module.
class QueryOrderedIterator:
  def __init__(self, query):
    self.query = query
    self.queryIterator = QueryIterator(self.query)
    self.sortedObjects = None
    self.sortedObjectsIterator = None

  def __iter__(self):
    return self

  def next(self):
    if self.sortedObjectsIterator is None:
      self.sortedObjects = []
      while True:
        try:
          obj = self.queryIterator.next()
          self.sortedObjects.append(obj)
        except StopIteration:
          break
      for so in reversed(self.query.sortOrders):
        self.sortedObjects.sort(key=lambda x: (x.__dict__[so.propertyName]), reverse=so.reversed)
      self.sortedObjectsIterator = self.sortedObjects.__iter__()
    return self.sortedObjectsIterator.next()


# Abstract class for defining model properties.
# You should use one of the specializations: StringProperty, IntegerProperty, BooleanProperty etc.
class Property:
  # Parameters:
  # default: when you put() a model instance with an empty() property value, it is set to default instead.
  # required: if True, an exception is raised when attempting to store an instance with an empty() property value.
  # validator: custom function used to validate property values.
  # choices: list/set of values which are valid property values.
  # indexed: ignored.
  def __init__(self, default=None, required=False, validator=None, choices=None, indexed=True):
    self.default = default
    self.required = required
    self.validator = validator
    self.choices = choices
    self.indexed = indexed
    self.name = None # set by model... at some point
    self.data_type = None # set by subclass

  # The complete validation routine for the property.
  # If value is valid, it returns the value, either unchanged or adapted to the required type.
  # Otherwise it raises an appropriate exception.
  def validate(self, value):
    if self.empty(value) and not self.empty(self.default):
      value = self.default
    if self.required and self.empty(value):
      log("Property name:", self.name)
      log("Value:", value)
      raise ValueError("Missing property marked as required")
    if value is not None:
      if not isinstance(value, self.data_type):
        log("Data type:", self.data_type)
        log("Value:", value)
        raise ValueError("Bad type for property marked as required")
    if (self.choices is not None):
      if value not in self.choices:
        log("Value:", value)
        log("Choices:", self.choices)
        raise ValueError("Bad value for property marked with strict choices")
    if self.validator:
      self.validator(value)
    return value

  # Returns True if value is considered an empty value for this property type.
  def empty(self, value):
    return value is None

  # Returns a str encoding the value, to be stored in the datastore.
  def _to_datastore(self, value):
    if self.empty(value):
      value = self.default
    return json.dumps(value)

  # Decodes, validates and returns the value, as encoded by _to_datastore.
  def _from_datastore(self, encoded):
    value = None
    if encoded:
      value = json.loads(encoded)
    return self.validate(value)

  # Filtering support.
  # These operators return a Filter instance storing the comparison.
  def __eq__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, "=", value)

  def __ne__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, "!=", value)

  def __lt__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, "<", value)

  def __le__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, "<=", value)

  def __gt__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, ">", value)

  def __ge__(self, value):
    if value is not None:
      value = self.validate(value)
    return Filter(self, ">=", value)

  def IN(self, value):
    validated = []
    for v in value:
      if v is not None:
        v = self.validate(v)
      validated.append(v)
    return Filter(self, "in", validated)

  # Sorting support.
  # Returns a SortOrder storing the property and the sort direction (descending).
  def __neg__(self):
    return SortOrder(self, reversed=True)

  # Returns a SortOrder storing the property and the sort direction (ascending).
  def __pos__(self):
    return SortOrder(self)


# Property specializations

# Stores a string (str or unicode).
# Unlike the AppEngine equivalent, there is no size limit.
class StringProperty(Property):
  def __init__(self, default=None, required=False, validator=None, choices=None, indexed=True):
    Property.__init__(self, default, required, validator, choices, indexed)
    self.data_type = basestring

TextProperty = StringProperty
BlobProperty = StringProperty

# Stores an integer (int or long).
class IntegerProperty(Property):
  def __init__(self, default=None, required=False, validator=None, choices=None, indexed=True):
    Property.__init__(self, default, required, validator, choices, indexed)
    self.data_type = (long, int)

# Stores a bool.
class BooleanProperty(Property):
  def __init__(self, default=None, required=False, validator=None, choices=None, indexed=True):
    Property.__init__(self, default, required, validator, choices, indexed)
    self.data_type = bool

# Stores a float.
class FloatProperty(Property):
  def __init__(self, default=None, required=False, validator=None, choices=None, indexed=True):
    Property.__init__(self, default, required, validator, choices, indexed)
    self.data_type = float

# Stores a datetime.datetime.
# Use auto_now_add to set it to the current time when the model instance is created in the datastore.
# Use auto_now to set it to the current time when the model instance is stored to the datastore.
# Stored in coordinated universal time (UTC) in the datastore, without timezone support.
class DateTimeProperty(Property):
  def __init__(self, auto_now_add=False, auto_now=False, default=None, required=False, validator=None, choices=None, indexed=True):
    Property.__init__(self, default, required, validator, choices, indexed)
    self.data_type = datetime.datetime
    self.auto_now_add = auto_now_add
    self.auto_now = auto_now

  def _to_datastore(self, value):
    if self.auto_now or (self.auto_now_add and value is None):
      value = datetime.datetime.now()
    if value is None:
      value = self.default
    if value is not None:
      value = (value - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return json.dumps(value)

  def _from_datastore(self, encoded):
    value = json.loads(encoded)
    if not self.empty(value):
      value = datetime.datetime.utcfromtimestamp(value)
    return self.validate(value)

initDatastoreFromParams()
