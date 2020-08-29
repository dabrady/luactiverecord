local _,filepath = ...
local OLD_PATH = package.path
package.path = string.format(
  "%s;%s",
  string.format("%svendors/?.lua", filepath:sub(1, filepath:find('/[^/]*$'))),
  OLD_PATH)
-------

local sqlite = require('hs.sqlite3')
local loadmodule = require('lua-utils/loadmodule')
require('lua-utils/table')

local function _createTable(newActiveRecord, columns)
  local db,_,err = sqlite.open(newActiveRecord._.dbFilename, sqlite.OPEN_READWRITE + sqlite.OPEN_CREATE)
  assert(db, err)

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
    newActiveRecord.tableName,
    columnDefinitions,
    newActiveRecord.tableName)

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

-- Allow for this convenient syntax when creating new LUActiveRecords:
--   LUActiveRecord{ ... }
local LUActiveRecord = setmetatable({}, {
  __call = function(self, ...) return self.new(...) end
})

local MAIN_DATABASE_FILENAME
function LUActiveRecord.setMainDatabase(db)
  local argType = type(db)
  assert(
    argType == 'userdata' or argType == 'string',
    'must provide an open SQLite database object or an absolute path to an SQLite database file'
  )

  if argType == 'userdata' then
    MAIN_DATABASE_FILENAME = db:dbFilename('main')
  else
    MAIN_DATABASE_FILENAME = db
  end
end

function LUActiveRecord.new(args)
  assert(type(args) == 'table', 'expected table, given '..type(args))
  local tableName = args.tableName
  local dbFilename = args.dbFilename or MAIN_DATABASE_FILENAME
  local columns = args.columns

  assert(type(tableName) == 'string', 'tableName must be a string')
  assert(type(columns) == 'table', 'columns must be a table')
  assert(type(dbFilename) == 'string', 'dbFilename must be a string')
  print("Constructing new LUActiveRecord: "..tableName)

  -- Ensure row ID is a UUID
  columns.id = "TEXT NOT NULL PRIMARY KEY"

  local internalState = {}
  internalState.dbFilename = dbFilename

  local newActiveRecord = {
    tableName = tableName,
    columns = columns,
    _ = internalState
  }

  -- Create the backing table for this new record type.
  _createTable(newActiveRecord, columns)

  loadmodule('constructors', newActiveRecord)
  loadmodule('finders', newActiveRecord)

  return setmetatable(newActiveRecord, { __index = LUActiveRecord })
end

--------
package.path = OLD_PATH
return LUActiveRecord
