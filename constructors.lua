local _,_,module = ...
module = module or {}
assert(type(module) == 'table', 'must provide a table to extend')

local sqlite = require('hs.sqlite3')
require('lua-utils/table')
require('lua-utils/string')
local loadmodule = require('lua-utils/loadmodule')
local marshal = require('marshal')

local uuid = (function()
  local uuidGenerator = require('uuid')
  uuidGenerator.randomseed(math.random(0,2^32))
  local uuidGeneratorSeed = uuidGenerator()
  return function()
    return uuidGenerator(uuidGeneratorSeed)
  end
end)()

--[[
  NOTE(dabrady) Non-simple datatypes are:
  - table
  - function
  - userdata
  - thread
]]
local function isSimpleValue(v)
  return table.contains({'nil', 'string', 'number', 'boolean'}, type(v))
end

local function _insertNewRow(newRecord)
  -- TODO(dabrady) Consider writing a helper for opening a DB connection w/standard pragmas
  local db,_,err = sqlite.open(newRecord.__metadata.dbFilename)
  assert(db, err)
  -- Attach pragma helpers
  loadmodule('pragmas', getmetatable(db))

  db:enable_foreign_key_constraints()

  local insertList = 'id'
  local queryParams = ':id'
  local marshaledAttrs = {}
  for columnName,value in pairs(newRecord.__attributes) do
    if columnName ~= 'id' then
      insertList = string.format('%s, %s', insertList, columnName)
      queryParams = string.format('%s, :%s', queryParams, columnName)
    end

    -- NOTE(dabrady) Complex datatypes are serialized in an encoded fashion so that they can
    -- be deserialized more accurately later on.
    if isSimpleValue(value) then
      marshaledAttrs[columnName] = value
    else
      -- NOTE(dabrady) Metatables and function environments are not serialized by this.
      marshaledAttrs[columnName] = marshal.encode(value)
    end
  end

  local queryString = string.format(
    [[
      INSERT INTO %s(%s)
      VALUES(%s)
    ]],
    newRecord.tableName,
    insertList,
    queryParams)

  local statement = db:prepare(queryString)
  assert(statement, db:error_message())

  -- Bind our query variables to our marshaled attributes.
  assert(statement:bind_names(marshaledAttrs) == sqlite.OK, db:error_message())

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())
  statement:finalize()
  db:close()
  return res
end

-- TODO(dabrady) Break this out and use `loadmodule` to extend `new_record.__reference_getters`.
local function _generate_reference_getters(newRecord, reference_columns, references)
  -- A function that attempts to lookup a record on a reference table whose primary key
  -- is the value of the given column on this row.
  local _getter_for = function(foreign_key_column, reference_table, ref_name)
    return function()
      -- Check the cache first and short-circuit the lookup if possible.
      local ref_cache = getmetatable(newRecord.__reference_getters).REFERENCE_CACHE
      local cached_ref = ref_cache[ref_name]
      if cached_ref then
        return cached_ref
      end

      local reference = assert(references[reference_table], 'unknown active record for table "'..reference_table..'"')

      -- Do nothing if the foreign key isn't populated.
      local foreign_key = newRecord[foreign_key_column]
      if foreign_key then
        -- NOTE(dabrady) Current implementation of `find` matches against the row `id` column,
        -- so the assumption here is that all foreign keys are row IDs.
        ref_cache[ref_name] = reference:find(foreign_key)
        return ref_cache[ref_name]
      else
        return nil
      end
    end
  end

  -- Generate a getter for each reference.
  for foreign_key_column, reference_table in pairs(reference_columns) do
    -- NOTE(dabrady) Assumption: foreign key columns named with '_id' suffix.
    -- TODO(dabrady) Consider making this configurable if it becomes a problem.
    local ref_name = foreign_key_column:chop('_id')

    --[[ TODO(dabrady)
      Consider making `__reference_getters` the ref cache itself, and make the cache-buster
      the `__call` event of its metatable, so you can do things like this:
          record.__reference_getters.person --> Person{}
          record.__reference_getters(true).person --> clears cache, then looks up, caches, and returns Person{}
    ]]

    newRecord.__reference_getters[ref_name] = _getter_for(foreign_key_column, reference_table, ref_name)
  end

  ---
  return
end

function module:new(valuesByField)
  -- Store an ordered (when iterated) copy of our input.
  local attrs = setmetatable(table.copy(valuesByField), { __pairs = table.orderedPairs })
  attrs.id = attrs.id or uuid()

  local metadata = getmetatable(self)
  local newRecord = setmetatable(
    {
      -- Store relevant metatable from our model on individual records.
      __metadata = {
        dbFilename = metadata.dbFilename,
        columns = table.keys(self.schema)
      },

      -- Hold onto our attributes.
      __attributes = attrs
    },
    {
      -- NOTE(dabrady) Need to `rawget` in our lookups here; if we were to
      -- access from the record directly in the index function, we'd trigger
      -- an infinite recursion.
      __index = function (t, k)
        local refs = rawget(t, '__reference_getters')
        return
          -- Check our table attributes first
          rawget(t, '__attributes')[k]
        -- Allow shorcuts like `r.reference` instead of `r.__reference_getters.reference()`
          or ( refs and refs[k] and refs[k]() )
        -- Check the table's own properties.
          or rawget(t, k)
        -- Or finally, delegate to our LUActiveRecord instance itself.
          or self[k]
      end,

      -- Prioritize attribute setting over direct key insertion.
      -- TODO If we add support for modifying existing records, ensure cached references
      -- are cleared when their corresponding reference column is modified (even on
      -- unsaved records?)
      __newindex = function (t, k, v)
        if t.__attributes[k] then
          rawset(t.__attributes, k, v)
        else
          rawset(t,k,v)
        end
      end,

      -- TODO(dabrady) Modify to support displaying nil columns.
      -- The natural behavior of Lua is that a table with a key pointing to nil
      -- means that key isn't there at all, and is ignored by its index, meaning
      -- in our case that nil columns won't be displayed at all.
      -- e.g. Person{ name = 'Daniel', address = nil } --> <persons>{ name = Daniel }
      __tostring = function(t, options)
        return string.format(
          "<%s>%s",
          -- Prefix the formatted table with the table name.
          t.tableName,
          -- Trim any leading indentation from the formatting
          table.format(t.__attributes, { depth = 2, startingIndentLvl = options and options.indent }):trim()
        )
      end
    }
  )

  -- Generate getters for any table references this record has.
  if metadata.reference_columns then
    newRecord.__reference_getters = setmetatable(
      {},
      -- A basic reference cache and cache-busting mechanism.
      {
        REFERENCE_CACHE = {},
        -- NOTE(dabrady) Opting to clear table instead of recreate to avoid breaking
        -- any pointers to the cache that might exist.
        __flush_ref_cache = function(self) for k in pairs(self.REFERENCE_CACHE) do self.REFERENCE_CACHE[k] = nil end end
      }
    )
    _generate_reference_getters(newRecord, metadata.reference_columns, metadata.references)
  end

  return newRecord
end

function module:reload()
  local me = self:find(self.id)

  -- Refresh column values
  table.merge(self.__attributes, me.__attributes)

  -- Refresh reference cache
  if self.__reference_getters then
    getmetatable(self.__reference_getters):__flush_ref_cache()
  end

  return self
end

function module:save()
  assert(_insertNewRow(self))

  -- Reloading here to pull in any changes made by database hooks (e.g. default values)
  return self:reload()
end

function module:create(valuesByField)
  return self:new(valuesByField):save()
end

-------
return module
