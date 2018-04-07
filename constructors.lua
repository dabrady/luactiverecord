local _,_,module = ...
module = module or {}
assert(type(module) == 'table', 'must provide a table to extend')

local sqlite = require('hs.sqlite3')
require('lua-utils/table')

local uuid = (function()
  local uuidGenerator = require('uuid')
  uuidGenerator.randomseed(math.random(0,2^32))
  uuidGeneratorSeed = uuidGenerator()
  return function()
    return uuidGenerator(uuidGeneratorSeed)
  end
end)()

local function _insertNewRow(newRecord)
  local db,_,err = sqlite.open(newRecord._.dbFilename)
  assert(db, err)

  local queryParams = ':id'
  for columnName, _ in pairs(newRecord.columns) do
    if columnName ~= 'id' then
      queryParams = string.format('%s, :%s', queryParams, columnName)
    end
  end
  local queryString = string.format(
    [[
      INSERT INTO %s
      VALUES(%s)
    ]],
    newRecord.tableName,
    queryParams)

  local statement = db:prepare(queryString)
  assert(statement, db:error_message())

  -- TODO Fix this: it's order agnostic and shouldn't be.
  statement:bind_names(newRecord)

  local res = statement:step()
  assert(res == sqlite.DONE, db:error_message())
  db:close()
  return res
end

function module:new(valuesByField)
  return setmetatable(valuesByField, {
    __index = self,
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

function module:create(valuesByField)
  local attrs = table.copy(valuesByField)
  attrs.id = uuid()

  local newRecord = self:new(attrs)
  assert(_insertNewRow(newRecord))

  return newRecord
end

-------
return module
