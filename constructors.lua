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
  local db,_,err = sqlite.open(newRecord.__metadata.dbFilename)
  assert(db, err)
  -- Attach pragma helpers
  loadmodule('pragmas', getmetatable(db))

  db:enable_foreign_key_constraints()

  local insertList = 'id'
  local queryParams = ':id'
  for columnName, _ in pairs(newRecord.columns) do
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

  for attr,val in pairs(newRecord) do
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

local function _attach_reference_getters(row, reference_columns, references)
  -- A function that attempts to lookup a record on a reference table whose primary key
  -- is the value of the given column on this row.
  local _getter_for = function(reference_column, reference_table)
    return function()
      local reference = assert(references[reference_table], 'unknown active record for table "'..reference_table..'"')
      local foreign_key = row[reference_column]
      if foreign_key then
        return reference:find(foreign_key)
      else
        return nil
      end
    end
  end

  -- Attach a getter for each reference.
  for reference_column, reference_table in pairs(reference_columns) do
    -- NOTE(dabrady) Assumption: foreign key columns named with '_id' suffix.
    -- TODO(dabrady) Consider making this configurable if it becomes a problem.
    local ref_name = reference_column:chop('_id')
    row[ref_name] = _getter_for(reference_column, reference_table)
  end

  ---
  return row
end

function module:new(valuesByField)
  local attrs = table.copy(valuesByField) -- don't reference our input!
  attrs.id = attrs.id or uuid()

  local newRecord = table.merge({}, attrs)
  if self.__metadata.references then
    _attach_reference_getters(newRecord, self.__metadata.reference_columns, self.__metadata.references)
  end

  return setmetatable(newRecord, {
    __index = self,
    -- TODO(dabrady) Modify to support displaying nil columns.
    -- The natural behavior of Lua is that a table with a key pointing to nil
    -- means that key isn't there at all, and is ignored by its index, meaning
    -- in our case that nil columns won't be displayed at all.
    -- e.g. Person{ name = 'Daniel', address = nil } --> <persons>{ name = Daniel }
    __tostring = function(self, curIndentLvl)
      local function trimLeadingWhitespace(s)
        return s:gsub('^\t*', '')
      end
      return string.format("<%s>%s",
        -- Prefix the formatted table with the table name.
        self.tableName,
        -- Trim any leading indentation from the formatting
        table.format(self, 1, curIndentLvl):gsub('^\t*', ''))
    end
  })
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
