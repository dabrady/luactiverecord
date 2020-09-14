local sqlite = require('hs.sqlite3')
local pragmas = require('src/util/db_pragmas')
local lmarshal = require(--[[src/bin/]]'lmarshal')

-- TODO(dabrady) Make these not do global things?
require('vendors/lua-utils/table')
require('vendors/lua-utils/string')

local management = {}

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
  -- Turn on some pragmas
  table.merge(getmetatable(db), pragmas)

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
      marshaledAttrs[columnName] = lmarshal.encode(value)
    end
  end

  local queryString = string.format(
    [[
      INSERT INTO %s(%s)
      VALUES(%s)
    ]],
    newRecord.__metadata.tableName,
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

function management.save(record)
  assert(_insertNewRow(record))

  -- Reloading here to pull in any changes made by database hooks (e.g. default values)
  return record:reload()
end


function management.reload(record)
  local me = record.__metadata.recordPlayer:find(record.id)

  -- Refresh column values
  table.merge(record.__attributes, me.__attributes)

  -- Refresh reference cache
  if record.__reference_getters then
    getmetatable(record.__reference_getters):__flush_ref_cache()
  end

  return record
end

return management
