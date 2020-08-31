local _,_,module = ...
module = module or {}
assert(type(module) == 'table', 'must provide a table to extend')

local sqlite = require('hs.sqlite3')
require('lua-utils/table')
require('lua-utils/string')
local loadmodule = require('lua-utils/loadmodule')

local uuid = (function()
  local uuidGenerator = require('uuid')
  uuidGenerator.randomseed(math.random(0,2^32))
  uuidGeneratorSeed = uuidGenerator()
  return function()
    return uuidGenerator(uuidGeneratorSeed)
  end
end)()

local function _insertNewRow(newRecord)
  -- TODO(dabrady) Consider writing a helper for opening a DB connection w/standard pragmas
  local db,_,err = sqlite.open(newRecord.__metadata.dbFilename)
  assert(db, err)
  -- Attach pragma helpers
  loadmodule('pragmas', getmetatable(db))

  db:enable_foreign_key_constraints()

  local insertList = 'id'
  local queryParams = ':id'
  for columnName,_ in pairs(newRecord.__attributes) do
    if columnName ~= 'id' then
      insertList = string.format('%s, %s', insertList, columnName)
      queryParams = string.format('%s, :%s', queryParams, columnName)
    end
  end

  -- print(queryParams)
  local queryString = string.format(
    [[
      INSERT INTO %s(%s)
      VALUES(%s)
    ]],
    newRecord.tableName,
    insertList,
    queryParams)

  for attr,val in pairs(newRecord.__attributes) do
    if type(val) == 'string' then val = string.format("'%s'", val) end
    queryString = queryString:gsub(':'..attr, val)
  end

  print(queryString)
  local statement = db:prepare(queryString)
  assert(statement, db:error_message())

  -- TODO find out why this seems to be doing it wrong
  -- statement:bind_names(newRecord)

  -- print(statement:get_names())

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())
  statement:finalize()
  db:close()
  return res
end

local function _generate_reference_getters(foreign_keys, reference_columns, references)
  -- A function that attempts to lookup a record on a reference table whose primary key
  -- is the value of the given column on this row.
  local _getter_for = function(foreign_key_column, reference_table)
    return function()
      local reference = assert(references[reference_table], 'unknown active record for table "'..reference_table..'"')

      -- Do nothing if the foreign key isn't populated.
      local foreign_key = foreign_keys[foreign_key_column]
      if foreign_key then
        -- NOTE(dabrady) Current implementation of `find` matches against the row `id` column,
        -- so the assumption here is that all foreign keys are row IDs.
        return reference:find(foreign_key)
      else
        return nil
      end
    end
  end

  -- Generate a getter for each reference.
  local getters = {}
  for foreign_key_column, reference_table in pairs(reference_columns) do
    -- NOTE(dabrady) Assumption: foreign key columns named with '_id' suffix.
    -- TODO(dabrady) Consider making this configurable if it becomes a problem.
    local ref_name = foreign_key_column:chop('_id')
    getters[ref_name] = _getter_for(foreign_key_column, reference_table)
  end

  ---
  return getters
end

function module:new(valuesByField)
  local attrs = table.copy(valuesByField) -- don't reference our input!
  attrs.id = attrs.id or uuid()

  local metadata = getmetatable(self)
  local newRecord = table.merge(
    {
      -- Store relevant metatable from our model on individual records.
      __metadata = {
        dbFilename = metadata.dbFilename,
        columns = table.keys(self.schema)
      },

      -- Hold onto the original set of attributes.
      __attributes = attrs,
    },
    attrs
  )

  -- Generate getters for any table references this record has.
  if metadata.reference_columns then
    local reference_keys = table.slice(attrs, table.keys(metadata.reference_columns))
    newRecord.__references = _generate_reference_getters(reference_keys, metadata.reference_columns, metadata.references)
  end

  return setmetatable(
    newRecord,
    {
      -- Allow shorcuts like `r.reference()` instead of `r.__references.reference()`
      -- NOTE(dabrady) Need to `rawget` to lookup `.__references` here; if we were to
      -- access it from the record directly in the index function, we'd risk getting
      -- caught in an infinite loop if the record didn't actually have any
      -- `__references` key.
      __index = function (t, k)
        local refs = rawget(t, '__references')
        return ( refs and refs[k] ) or self[k]
      end,

      -- TODO(dabrady) Modify to support displaying nil columns.
      -- The natural behavior of Lua is that a table with a key pointing to nil
      -- means that key isn't there at all, and is ignored by its index, meaning
      -- in our case that nil columns won't be displayed at all.
      -- e.g. Person{ name = 'Daniel', address = nil } --> <persons>{ name = Daniel }
      -- TODO(dabrady) Order by attributes first.
      __tostring = function(t, curIndentLvl)
        return string.format(
          "<%s>%s",
          -- Prefix the formatted table with the table name.
          t.tableName,
          -- Trim any leading indentation from the formatting
          table.format(t, 1, curIndentLvl):trim()
        )
      end
    }
  )
end

function module:save()
  assert(_insertNewRow(self))
  return self
end

function module:create(valuesByField)
  return self:new(valuesByField):save()
end

-------
return module
