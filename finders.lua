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

function module:find(id)
  local db,_,err = sqlite.open(self._.dbFilename, sqlite.OPEN_READONLY)
  assert(db, err)

  local attrs
  for row in db:nrows(string.format("SELECT * FROM %s WHERE id = '%s'", self.tableName, id)) do
    -- Grab the first one (should be the only one, cuz id == primary key)
    attrs = row
    break
  end
  db:close()

  if table.isEmpty(attrs) then
    return nil
  else
    return self:new(attrs)
  end
end

function module:find_by(attrs)
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

  local attrs
  for row in db:nrows(string.format("SELECT * FROM %s WHERE %s", self.tableName, attrString)) do
    -- Grab the first one (this is a single read, not a group read)
    attrs = row
    break
  end

  db:close()

  if table.isEmpty(attrs) then
    return nil
  else
    return self:new(attrs)
  end
end

-------
return module
