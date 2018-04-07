local _,_,module = ...
module = module or {}
assert(type(module) == 'table', 'must provide a table to extend')

local sqlite = require('hs.sqlite3')

function module:all()
  local db,_,err = sqlite.open(self._.dbFilename, sqlite.OPEN_READONLY)
  assert(db, err)

  local records = {}
  for row in db:nrows('SELECT * FROM '..self.tableName) do
    table.insert(records, self:new(row))
  end
  db:close()

  return records
end

function module:where(attrs, addendum)
  local db,_,err = sqlite.open(self._.dbFilename, sqlite.OPEN_READONLY)
  assert(db, err)

  local attrString = ''
  for attr,val in pairs(attrs) do
    -- Wrap strings in extra quotes for the query
    if type(val) == 'string' then val = string.format("'%s'", val) end
    attrString = string.format('%s AND %s = %s', attrString, attr, val)
  end
  -- Strip leading 'AND'
  attrString = attrString:match('^ AND (.*)')

  -- Append any additional constraints
  if addendum ~= nil then
    local addendumType = type(addendum)
    if addendumType == 'string' then
      attrString = string.format('%s %s', attrString, addendum)
    elseif addendumType == 'table' then
      local addendumString = ''
      for k,v in pairs(addendum) do
        k = k:gsub('_', ' ') -- Unchain a multieword key, i.e. 'group_by' => 'group by'
        addendumString = string.format('%s %s %s', addendumString, k, v)
      end
      attrString = string.format('%s %s', attrString, addendumString)
    end
  end

  local records = {}
  local queryString = string.format("SELECT * FROM %s WHERE %s", self.tableName, attrString)

  -- DEBUG
  print(queryString)
  -- DEBUG

  for row in db:nrows(queryString) do
    table.insert(records, self:new(row))
  end

  db:close()

  return records
end

function module:find_by(attrs)
  return module:where(attrs, {limit = 1})[1]
end

function module:find(id)
  return module:find_by{id = id}
end

-------
return module
