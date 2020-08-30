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
  local columns = args.columns
  local references = args.references
  local drop_first = args.drop_first

  if references then
    -- Add foreign key constraints
    -- TODO(dabrady) Research need for index creation here
    for column, reference_table in pairs(references) do
      local constraints = columns[column]
      columns[column] = constraints..string.format(' REFERENCES %s', reference_table)
    end
  end

  local db,_,err = sqlite.open(dbFilename, sqlite.OPEN_READWRITE + sqlite.OPEN_CREATE)
  assert(db, err)

  if drop_first then
    print('Table recreation specified: dropping "'..name..'"')
    db:exec('DROP TABLE '..name)
  end

  -- Build query string
  local columnDefinitions = string.format('id %s\n', columns.id)
  for columnName, constraints in pairs(columns) do
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

  print('\n'..queryString)

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

  local tableName = assert(type(args.tableName) == 'string' and args.tableName, 'tableName must be a string')
  local dbFilename = assert(type(args.dbFilename) == 'string' or LUActiveRecord.DATABASE_LOCATION, 'dbFilename must be a string')
  local columns = assert(type(args.columns) == 'table' and args.columns, 'columns must be a table')
  local references = args.references and assert(type(args.references) == 'table', 'references must be a table if given')
  local recreate = args.recreate and assert(type(args.recreate) == 'boolean', 'recreate must be a boolean')
  print("Constructing new LUActiveRecord: "..tableName)

  -- Ensure row ID is a UUID
  -- TODO(dabrady) Tell users I'm doing this, don't be so sneaky.
  columns.id = "TEXT NOT NULL PRIMARY KEY"

  -- Create the backing table for this new record type.
  _createTable{
    name = tableName,
    db = dbFilename,
    columns = columns,
    references = references,
    drop_first = recreate
  }

  local newActiveRecord = setmetatable(
    { tableName = tableName, columns = columns },
    {
      -- TODO(dabrady) Evaluate if LUActiveRecord should be treated as a 'base', or not.
      -- __index = LUActiveRecord,

      dbFilename = dbFilename,
      reference_columns = references,
      -- A relevant slice of LUActiveRecord.RECORD_CACHE
      references = table.slice(LUActiveRecord.RECORD_CACHE, table.values(references))
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
