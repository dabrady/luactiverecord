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

function module:new(valuesByField)
  local attrs = table.copy(valuesByField) -- don't reference our input!
  attrs.id = attrs.id or uuid()

  return setmetatable(attrs, {
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

function module:save()
  assert(_insertNewRow(self))
  return self
end

function module:create(valuesByField)
  return self:new(valuesByField):save()
end

-------
return module
