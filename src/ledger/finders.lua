local sqlite = require('hs.sqlite3')
local lmarshal = require(--[[src/bin/]]'lmarshal')

local finders = {}

local function _unmarshal(row)
  return table.map(
    row,
    function(attr,val)
      if lmarshal.isEncoded(val) then
        val = lmarshal.decode(val)
      end
      return attr, val
    end
  )
end

function finders.all(ledger)
  local db,_,err = sqlite.open(getmetatable(ledger).database_location, sqlite.OPEN_READONLY)
  assert(db, err)

  local entries = {}
  for row in db:nrows('SELECT * FROM '..ledger.table_name) do
    -- Decode values as we read them out of the DB.
    table.insert(entries, ledger:new_entry(_unmarshal(row)))
  end
  db:close()

  return entries
end

function finders.where(ledger, attrs, addendum)
  local db,_,err = sqlite.open(getmetatable(ledger).database_location, sqlite.OPEN_READONLY)
  assert(db, err)

  local attr_string = ''
  for attr,val in pairs(attrs) do
    -- Wrap strings in extra quotes for the query
    if type(val) == 'string' then val = string.format("'%s'", val) end
    attr_string = string.format('%s AND %s = %s', attr_string, attr, val)
  end
  -- Strip leading 'AND'
  attr_string = attr_string:match('^ AND (.*)')

  -- Append any additional constraints
  if addendum ~= nil then
    local addendum_type = type(addendum)
    if addendum_type == 'string' then
      attr_string = string.format('%s %s', attr_string, addendum)
    elseif addendum_type == 'table' then
      local addendum_string = ''
      for k,v in pairs(addendum) do
        k = k:unchain() -- Unchain a multiword key, i.e. 'group_by' => 'group by'
        addendum_string = string.format('%s %s %s', addendum_string, k, v)
      end
      attr_string = string.format('%s %s', attr_string, addendum_string)
    end
  end

  local entries = {}
  local query_string = string.format("SELECT * FROM %s WHERE %s", ledger.table_name, attr_string)

  for row in db:nrows(query_string) do
    -- Decode values as we read them out of the DB.
    table.insert(entries, ledger:new_entry(_unmarshal(row)))
  end

  db:close()

  return entries
end

function finders.find_by(ledger, attrs)
  return finders.where(ledger, attrs, {limit = 1})[1]
end

function finders.find(ledger, id)
  return finders.find_by(ledger, {id = id})
end

-------
return finders
