#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Web: https://code.google.com/p/ndb-py
# License: GPLv2

# Interface for the datastore.
# Outside the module you should not care about the datastore, except for creating it. See the function setDatastore
# below.
class Datastore:
  def __init__(self):
    pass

  # Inserts/updates in the datastore the object with the given kind, key and value.
  # A kind is a model name.
  def set(self, kind, key, value):
    raise NotImplementedError()

  # Returns from the datastore the object with the given kind and key, or None if it does not exist.
  def get(self, kind, key):
    raise NotImplementedError()

  # Deletes from thedatastore the object with the given kind and key.
  # Does nothing if the object does not exist.
  def delete(self, kind, key):
    raise NotImplementedError()

  # Returns an iterator over the keys of the objects with a given kind.
  def iter(self, kind):
    raise NotImplementedError()

  # Returns the path where the datastore is located on disk, or None if not applicable.
  def get_path(self):
    return None

  # Returns a sorted list of all the kinds in the datastore.
  def get_kinds(self):
    raise NotImplementedError()

  def close(self):
    pass


# In-memory, dictionary-based datastore.
class MemDatastore(Datastore):
  def __init__(self):
    self.data = {}

  def set(self, kind, key, value):
    if kind not in self.data:
      self.data[kind] = {}
    self.data[kind][key] = value

  def get(self, kind, key):
    if kind not in self.data:
      return None
    if key not in self.data[kind]:
      return None
    return self.data[kind][key]

  def delete(self, kind, key):
    if kind not in self.data:
      return
    if key not in self.data[kind]:
      return
    del self.data[kind][key]

  def iter(self, kind):
    if kind not in self.data:
      self.data[kind] = {}
    return MemDatastoreIterator(self, kind)

  def get_kinds(self):
    return sorted(self.data.keys())


class MemDatastoreIterator:
  def __init__(self, datastore, kind):
    self.datastore = datastore
    self.kind = kind
    self.datastoreIterator = self.datastore.data[kind].__iter__()

  def __iter__(self):
    return self

  def next(self):
    return self.datastoreIterator.next()
