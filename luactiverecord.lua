-- NOTE(dabrady) Lua's `package.path` contains search paths relative to the
-- working directory at the time the path is searched. This makes sense, but
-- annoys me: most of the time, I want that search to prioritize the local
-- project over the rest, and this bit of ugliness gets me that behavior.
-- `require` passes the absolute path to this module as the second argument
local old_path = package.path
local old_cpath = package.cpath
local _,module_path = ...
if module_path then
  local project_dir = module_path:sub(1, module_path:find("/[^/]*$"))
  -- Temporarily modify loadpath
  package.path = ""
    ..project_dir.."?.lua;"
    ..project_dir.."?/?.lua;"
    ..project_dir.."?/init.lua;"
    ..package.path
  package.cpath = ""
    ..project_dir.."src/bin/?.so;"
    ..package.cpath
end

-- TODO(dabrady) Vendor this dependency, it ties us to a local installation of Hammerspoon
local sqlite = require("hs.sqlite3")
require("vendors/lua-utils/table")

local Ledger = require("src/ledger")

-- The base module.
local luactiverecord = {
  LEDGER_CACHE = {}
}

local function _create_table(args)
  local name = args.name
  local database_location = args.database_location
  local schema = args.schema
  local references = args.references
  local drop_first = args.drop_first

  if references then
    -- Add foreign key constraints
    -- TODO(dabrady) Research need for index creation here
    for column, reference_table in pairs(references) do
      local constraints = schema[column]
      schema[column] = constraints..string.format(" REFERENCES %s", reference_table)
    end
  end

  local db,_,err = sqlite.open(database_location, sqlite.OPEN_READWRITE + sqlite.OPEN_CREATE)
  assert(db, err)

  if drop_first then
    db:exec("DROP TABLE "..name)
  end

  -- Build query string
  local column_definitions = string.format("id %s\n", schema.id)
  for column_name, constraints in pairs(schema) do
    if column_name ~= "id" then
      -- TODO(dabrady) this whitespace is unnecessary, only nice for printing out the query
      column_definitions = string.format("%s        ,%s %s\n", column_definitions, column_name, constraints)
    end
  end
  local query_string = string.format(
    [[
      CREATE TABLE IF NOT EXISTS %s (
      %s
      ) WITHOUT ROWID;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_primary_key ON %s(id);
      ]],
    name,
    column_definitions,
    name)

  local statement = db:prepare(query_string)
  assert(statement, db:error_message())

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())

  statement:finalize()
  db:close()
  return res
end

function luactiverecord:configure(config)
  if not self.__config then
    self.__config = {}
  end

  if config.database_location then
    local db_loc = config.database_location
    assert(
      type(db_loc) == "string",
      "must provide an absolute path to an SQLite database file"
    )
    self.__config.database_location = db_loc
  end

  if config.seeds_location then
    local seeds_loc = config.seeds_location
    assert(
      type(seeds_loc) == "string",
      "must provide an absolute path to a Lua file"
    )
    self.__config.seeds_location = seeds_loc
  end
end

-- Creates entries in recognized records en masse.
function luactiverecord:seed_database(seeds_location)
  seeds_location = seeds_location or self.__config.seeds_location
  assert(
    type(seeds_location) == "string",
    "must provide an absolute path to a Lua file")

  local seeds = assert(loadfile(seeds_location)(self))

  for table_name, data in pairs(seeds) do
    -- print(string.format('[DEBUG] creating %s: %s', table_name, table.format(data, {depth=4})))
    for _,datum in ipairs(data) do
      local ledger = self.LEDGER_CACHE[table_name]
      if ledger then
        ledger:add_entry(datum)
      end
    end
  end
end

function luactiverecord:construct(args)
  assert(type(args) == "table", "expected table, given "..type(args))

  -- Required --
  local table_name = args.table_name
  assert(type(table_name) == "string", "table_name must be a string")

  local schema = args.schema
  assert(type(schema) == "table", "schema must be a table")

  -- Optional --
  -- TODO(dabrady) Verify this is a subset of the given schema.
  local references = args.references or nil
  if references then assert(type(references) == "table", "references must be a table") end

  local recreate = args.recreate or false
  if recreate then assert(type(recreate) == "boolean", "recreate must be a boolean") end

  -- Ensure row ID is a UUID
  -- TODO(dabrady) Tell users I'm doing this, don't be so sneaky.
  schema.id = "TEXT NOT NULL PRIMARY KEY"

  -- Create the backing table for this new record type.
  _create_table{
    name = table_name,
    database_location = self.__config.database_location,
    schema = schema,
    references = references,
    drop_first = recreate
  }

  local new_ledger = Ledger{
    table_name = table_name,
    schema = schema,
    database_location = self.__config.database_location,
    reference_columns = references,
    reference_ledgers = table.slice(self.LEDGER_CACHE, table.values(references))
  }

  -- Keep a reference to each constructed ledger
  self.LEDGER_CACHE[table_name] = new_ledger
  return new_ledger
end

setmetatable(
  luactiverecord,
  {
    -- Allow for this convenient syntax when creating new Ledgers:
    --   luactiverecord{ ... }
    __call = luactiverecord.construct
  }
)

-- Reset loadpath
package.path = old_path
package.cpath = old_cpath

return luactiverecord
