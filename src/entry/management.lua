local sqlite = require("hs.sqlite3")
local pragmas = require("src/util/db_pragmas")
local lmarshal = require(--[[src/bin/]]"lmarshal")

-- TODO(dabrady) Make these not do global things?
require("vendors/lua-utils/table")
require("vendors/lua-utils/string")

local management = {}

--[[
  NOTE(dabrady) Non-simple datatypes are:
  - table
  - function
  - userdata
  - thread
]]
local function _is_simple_value(v)
  return table.contains({"nil", "string", "number", "boolean"}, type(v))
end

local function _insert_new_row(entry)
  -- TODO(dabrady) Consider writing a helper for opening a DB connection w/standard pragmas
  local db,_,err = sqlite.open(entry.__metadata.database_location)
  assert(db, err)
  -- Turn on some pragmas
  table.merge(getmetatable(db), pragmas)

  db:enable_foreign_key_constraints()

  local insert_list = "id"
  local query_params = ":id"
  local marshaled_attrs = {}
  for column_name,value in pairs(entry.__attributes) do
    if column_name ~= "id" then
      insert_list = string.format("%s, %s", insert_list, column_name)
      query_params = string.format("%s, :%s", query_params, column_name)
    end

    -- NOTE(dabrady) Complex datatypes are serialized in an encoded fashion so that they can
    -- be deserialized more accurately later on.
    if _is_simple_value(value) then
      marshaled_attrs[column_name] = value
    else
      -- NOTE(dabrady) Metatables and function environments are not serialized by this.
      marshaled_attrs[column_name] = lmarshal.encode(value)
    end
  end

  local query_string = string.format(
    [[
      INSERT INTO %s(%s)
      VALUES(%s)
    ]],
    entry.__metadata.table_name,
    insert_list,
    query_params)

  local statement = db:prepare(query_string)
  assert(statement, db:error_message())

  -- Bind our query variables to our marshaled attributes.
  assert(statement:bind_names(marshaled_attrs) == sqlite.OK, db:error_message())

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())
  statement:finalize()
  db:close()
  return res
end

function management.persist(entry)
  assert(_insert_new_row(entry))

  -- Refreshing here to pull in any changes made by database hooks (e.g. default values)
  return entry:refresh()
end


function management.refresh(entry)
  local me = entry.__metadata.ledger:find(entry.id)

  -- Refresh column values
  table.merge(entry.__attributes, me.__attributes)

  -- Refresh reference cache
  if entry.__reference_getters then
    getmetatable(entry.__reference_getters):__flush_ref_cache()
  end

  return entry
end

return management
