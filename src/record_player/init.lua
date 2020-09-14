require('vendors/lua-utils/table')
local uuidGenerator = require('uuid')

-- Record player behavior
local finders = require('src/record_player/finders')

-- Record behavior
local referenceGetters = require('src/record/reference_getters')
local recordManagement = require('src/record/management')

local recordPlayer = {}
table.merge(recordPlayer, finders)

local uuid = (function()
  uuidGenerator.randomseed(math.random(0,2^32))
  local uuidGeneratorSeed = uuidGenerator()
  return function()
    return uuidGenerator(uuidGeneratorSeed)
  end
end)()

local function _newRecordPlayer(_, args)
  local self = {
    tableName = args.tableName,
    schema = args.schema
  }

  return setmetatable(
    self,
    {
      __index = recordPlayer,
      __call = recordPlayer.new,

      dbFilename = args.dbFilename,
      reference_columns = args.reference_columns,
      referenceRecordPlayers = args.referenceRecordPlayers
    }
  )
end

function recordPlayer:new(valuesByField)
  -- Store an ordered (when iterated) copy of our input.
  local attrs = setmetatable(table.copy(valuesByField), { __pairs = table.orderedPairs })
  attrs.id = attrs.id or uuid()

  local recordPlayerMetatable = getmetatable(self)
  local newRecord = {
    -- Store relevant recordPlayerMetatable from our model on individual records.
    __metadata = {
      dbFilename = recordPlayerMetatable.dbFilename,
      tableName = self.tableName,
      columns = table.keys(self.schema),
      recordPlayer = self
    },

    -- Hold onto our attributes.
    __attributes = attrs
  }

  -- Attach various record behaviors
  table.merge(newRecord, recordManagement)

  -- Generate getters for any table references this record has.
  if recordPlayerMetatable.reference_columns then
    referenceGetters.attach(
      newRecord,
      recordPlayerMetatable.reference_columns,
      recordPlayerMetatable.referenceRecordPlayers
    )
  end

  return setmetatable(
    newRecord,
    {
      -- NOTE(dabrady) Need to `rawget` in our lookups here; if we were to
      -- access from the record directly in the index function, we'd trigger
      -- an infinite recursion.
      __index = function (t, k)
        local refs = rawget(t, '__reference_getters')
        return
          -- Check our table attributes first
          rawget(t, '__attributes')[k]
        -- Allow shorcuts like `r.reference` instead of `r.__reference_getters.reference()`
        -- FIXME This will continue to the next line if `refs[k]()` returns nil. Stahp it
          or ( refs and refs[k] and refs[k]() )
        -- Or finally, delegate to our record player.
          or self[k]
      end,

      -- Prioritize attribute setting over direct key insertion.
      -- TODO If we add support for modifying existing records, ensure cached references
      -- are cleared when their corresponding reference column is modified (even on
      -- unsaved records?)
      __newindex = function (t, k, v)
        if t.__attributes[k] then
          rawset(t.__attributes, k, v)
        else
          rawset(t,k,v)
        end
      end,

      -- TODO(dabrady) Modify to support displaying nil columns.
      -- The natural behavior of Lua is that a table with a key pointing to nil
      -- means that key isn't there at all, and is ignored by its index, meaning
      -- in our case that nil columns won't be displayed at all.
      -- e.g. Person{ name = 'Daniel', address = nil } --> <persons>{ name = Daniel }
      __tostring = function(t, options)
        return string.format(
          "<%s>%s",
          -- Prefix the formatted table with the table name.
          t.tableName,
          -- Trim any leading indentation from the formatting
          table.format(t.__attributes, { depth = 2, startingIndentLvl = options and options.indent }):trim()
        )
      end
    }
  )
end

function recordPlayer:create(valuesByField)
  local record = self:new(valuesByField)
  return record:save()
end

return setmetatable(
  recordPlayer,
  {
    __call = _newRecordPlayer
  }
)
