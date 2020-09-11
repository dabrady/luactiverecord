do
  local _,filepath = ...
  local parentdir = filepath:sub(1, filepath:find('/[^/]*$'))
  package.path = string.format(
    "%s;%s;%s",
    string.format("%s?.lua", parentdir),
    string.format("%svendors/?.lua", parentdir),
    package.path)
end
-------

local sqlite = require('hs.sqlite3')
local loadmodule = require('lua-utils/loadmodule')
require('lua-utils/table')

-------

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
local LUActiveRecord = setmetatable(
  {
    DATABASE_LOCATION = nil,
    RECORD_CACHE = {}
  },
  {
    -- Allow for this convenient syntax when creating new LUActiveRecords:
    --   LUActiveRecord{ ... }
    __call = function(self, ...) return self.new(...) end
  }
)

function LUActiveRecord.setDefaultDatabase(db)
  local argType = type(db)
  assert(
    argType == 'userdata' or argType == 'string',
    'must provide an open SQLite database object or an absolute path to an SQLite database file'
  )

  if argType == 'userdata' then
    LUActiveRecord.DATABASE_LOCATION = db:dbFilename('main')
  else
    LUActiveRecord.DATABASE_LOCATION = db
  end
end

function LUActiveRecord.new(args)
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

  local dbFilename = args.dbFilename or LUActiveRecord.DATABASE_LOCATION
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

  local newActiveRecord = setmetatable(
    {
      tableName = tableName,
      schema = schema
    },
    {
      -- TODO(dabrady) Evaluate if LUActiveRecord should be treated as a 'base', or not.
      -- __index = LUActiveRecord,

      -- Allow for this convenient syntax when constructing (but not saving) new records:
      --   Record{ ... }
      __call = function(R, ...) return R:new(...) end,

      dbFilename = dbFilename,
      reference_columns = references,
      -- NOTE(dabrady) We leverage this when generating accessors for our reference columns.
      -- If we were to slice this down to a more relevant subset of the cache, we'd run into
      -- problems when using said accessors if the referenced table didn't exist in the cache
      -- at the time this Record was created. We avoid this by using a handle to the entire cache.
      references = LUActiveRecord.RECORD_CACHE
    }
  )
  -- Attach some functionality.
  loadmodule('constructors', newActiveRecord)
  loadmodule('finders', newActiveRecord)


  LUActiveRecord.RECORD_CACHE[tableName] = newActiveRecord
  return newActiveRecord
end

--------
return LUActiveRecord
