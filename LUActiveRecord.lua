local _,filepath = ...
local OLD_PATH = package.path
package.path = string.format(
  "%s;%s",
  string.format("%svendors/?.lua", filepath:sub(1, filepath:find('/[^/]*$'))),
  OLD_PATH)
-------

local sqlite = require('hs.sqlite3')
local loadmodule = require('lua-utils/loadmodule')

local assertions = require('lua-utils/assertions')
assert_type = assertions.assert_type

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

function LUActiveRecord.setMainDatabase(db)
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
  assert_type(args, 'table')

  local tableName = assert_type(args.tableName, 'string')
  local dbFilename = assert_type(args.dbFilename or LUActiveRecord.DATABASE_LOCATION, 'string')
  local columns = assert_type(args.columns, 'table')
  local references = assert_type(args.references, '?table')
  local recreate = assert_type(args.recreate, '?boolean')
  print("Constructing new LUActiveRecord: "..tableName)

  -- Ensure row ID is a UUID
  -- TODO(dabrady) Tell users I'm doing this, don't be so sneaky.
  columns.id = "TEXT NOT NULL PRIMARY KEY"

  local newActiveRecord = {
    tableName = tableName,
    columns = columns,
    -- Internal state
    __metadata = {
      dbFilename = dbFilename,
      reference_columns = references,
      -- A relevant slice of LUActiveRecord.RECORD_CACHE
      references = table.slice(ACTIVE_RECORD_CACHE, table.values(references))
    }
  }

  -- Create the backing table for this new record type.
  _createTable{
    name = tableName,
    db = dbFilename,
    columns = columns,
    references = references,
    drop_first = recreate
  }

  loadmodule('constructors', newActiveRecord)
  loadmodule('finders', newActiveRecord)

  local finalized_record = setmetatable(newActiveRecord, { __index = LUActiveRecord })
  ACTIVE_RECORD_CACHE[tableName] = finalized_record
  return finalized_record
end

--------
package.path = OLD_PATH
return LUActiveRecord
