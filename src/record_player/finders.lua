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

function finders.all(recordPlayer)
  local db,_,err = sqlite.open(getmetatable(recordPlayer).dbFilename, sqlite.OPEN_READONLY)
  assert(db, err)

  local records = {}
  for row in db:nrows('SELECT * FROM '..recordPlayer.tableName) do
    -- Decode values as we read them out of the DB.
    table.insert(records, recordPlayer:new(_unmarshal(row)))
  end
  db:close()

  return records
end

function finders.where(recordPlayer, attrs, addendum)
  local db,_,err = sqlite.open(getmetatable(recordPlayer).dbFilename, sqlite.OPEN_READONLY)
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
  local queryString = string.format("SELECT * FROM %s WHERE %s", recordPlayer.tableName, attrString)

  for row in db:nrows(queryString) do
    -- Decode values as we read them out of the DB.
    table.insert(records, recordPlayer:new(_unmarshal(row)))
  end

  db:close()

  return records
end

function finders.find_by(recordPlayer, attrs)
  return finders.where(recordPlayer, attrs, {limit = 1})[1]
end

function finders.find(recordPlayer, id)
  return finders.find_by(recordPlayer, {id = id})
end

-------
return finders
