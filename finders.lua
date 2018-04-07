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

-------
return module
