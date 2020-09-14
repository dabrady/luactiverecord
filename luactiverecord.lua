-- NOTE(dabrady) Lua's `package.path` contains search paths relative to the
-- working directory at the time the path is searched. This makes sense, but
-- annoys me: most of the time, I want that search to prioritize the local
-- project over the rest, and this bit of ugliness gets me that behavior.
-- `require` passes the absolute path to this module as the second argument
local withProjectInPath = function(fn) return fn() end
local projectDir = ''
local _,modulePath = ...
if modulePath then
  projectDir = modulePath:sub(1, modulePath:find('/[^/]*$'))
  withProjectInPath = assert(loadfile(projectDir..'lib/withProjectInPath.lua'))(projectDir)
end

return withProjectInPath(function()
--- START MODULE DEFINITION ---

-- TODO(dabrady) Vendor this dependency, it ties us to a local installation of Hammerspoon
local sqlite = require('hs.sqlite3')
require('vendors/lua-utils/table')

local RecordPlayer = require('src/record_player')

local function _createTable(args)
  local name = args.name
  local dbFilename = args.db
  local schema = args.schema
  local references = args.references
  local drop_first = args.drop_first

  if references then
    -- Add foreign key constraints
    -- TODO(dabrady) Research need for index creation here
    for column, reference_table in pairs(references) do
      local constraints = schema[column]
      schema[column] = constraints..string.format(' REFERENCES %s', reference_table)
    end
  end

  local db,_,err = sqlite.open(dbFilename, sqlite.OPEN_READWRITE + sqlite.OPEN_CREATE)
  assert(db, err)

  if drop_first then
    db:exec('DROP TABLE '..name)
  end

  -- Build query string
  local columnDefinitions = string.format('id %s\n', schema.id)
  for columnName, constraints in pairs(schema) do
    if columnName ~= 'id' then
      -- TODO(dabrady) this whitespace is unnecessary, only nice for printing out the query
      columnDefinitions = string.format('%s        ,%s %s\n', columnDefinitions, columnName, constraints)
    end
  end
  local queryString = string.format(
    [[
      CREATE TABLE IF NOT EXISTS %s (
      %s
      ) WITHOUT ROWID;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_primary_key ON %s(id);
      ]],
    name,
    columnDefinitions,
    name)

  local statement = db:prepare(queryString)
  assert(statement, db:error_message())

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())

  statement:finalize()
  db:close()
  return res
end

--------

-- The base module.
local luactiverecord = {
  DATABASE_LOCATION = nil,
  RECORD_PLAYER_CACHE = {}
}

function luactiverecord.setDefaultDatabase(db)
  local argType = type(db)
  assert(
    argType == 'userdata' or argType == 'string',
    'must provide an open SQLite database object or an absolute path to an SQLite database file'
  )

  if argType == 'userdata' then
    luactiverecord.DATABASE_LOCATION = db:dbFilename('main')
  else
    luactiverecord.DATABASE_LOCATION = db
  end
end

-- Creates entries in recognized records en masse.
function luactiverecord.seedDatabase(seedsFilePath)
  assert(type(seedsFilePath) == 'string', 'must provide an absolute path to your seeds file')

  local seeds = assert(loadfile(seedsFilePath)(luactiverecord))

  for tableName, data in pairs(seeds) do
    -- print(string.format('[DEBUG] creating %s: %s', tableName, table.format(data, {depth=4})))
    for _,datum in ipairs(data) do
      local recordPlayer = luactiverecord.RECORD_PLAYER_CACHE[tableName]
      if recordPlayer then
        recordPlayer:create(datum)
      end
    end
  end
end

function luactiverecord.construct(args)
  assert(type(args) == 'table', 'expected table, given '..type(args))

  -- Required --
  local tableName = args.tableName
  assert(type(tableName) == 'string', 'tableName must be a string')

  local schema = args.schema
  assert(type(schema) == 'table', 'schema must be a table')

  -- Optional --
  -- TODO(dabrady) Verify this is a subset of the given schema.
  local references = args.references or nil
  if references then assert(type(references) == 'table', 'references must be a table if given') end

  local dbFilename = args.dbFilename or luactiverecord.DATABASE_LOCATION
  if dbFilename then assert(type(dbFilename) == 'string', 'dbFilename must be a string') end

  local recreate = args.recreate or false
  if recreate then assert(type(recreate) == 'boolean', 'recreate must be a boolean') end

  -- Ensure row ID is a UUID
  -- TODO(dabrady) Tell users I'm doing this, don't be so sneaky.
  schema.id = "TEXT NOT NULL PRIMARY KEY"

  -- Create the backing table for this new record type.
  _createTable{
    name = tableName,
    db = dbFilename,
    schema = schema,
    references = references,
    drop_first = recreate
  }

  local newRecordPlayer = RecordPlayer{
    tableName = tableName,
    schema = schema,
    dbFilename = dbFilename,
    reference_columns = references,
    referenceRecordPlayers = table.slice(luactiverecord.RECORD_PLAYER_CACHE, table.values(references))
  }

  luactiverecord.RECORD_PLAYER_CACHE[tableName] = newRecordPlayer
  return newRecordPlayer
end

return setmetatable(
  luactiverecord,
  {
    -- Allow for this convenient syntax when creating new LUActiveRecords:
    --   luactiverecord( { ... } )
    __call = function(self, ...) return self.construct(...) end
  }
)

--- END MODULE DEFINITION ---
end)
