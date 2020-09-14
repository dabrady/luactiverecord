require('vendors/lua-utils/table')
local uuidGenerator = require('uuid')

-- Ledger behavior
local finders = require('src/ledger/finders')

-- Ledger entry behavior
local referenceGetters = require('src/entry/reference_getters')
local entryManagement = require('src/entry/management')

local ledger = {}
table.merge(ledger, finders)

local uuid = (function()
  uuidGenerator.randomseed(math.random(0,2^32))
  local uuidGeneratorSeed = uuidGenerator()
  return function()
    return uuidGenerator(uuidGeneratorSeed)
  end
end)()

local function _newLedger(_, args)
  local self = {
    tableName = args.tableName,
    schema = args.schema
  }

  return setmetatable(
    self,
    {
      __index = ledger,
      __call = ledger.newEntry,

      database_location = args.database_location,
      reference_columns = args.reference_columns,
      referenceLedgers = args.referenceLedgers
    }
  )
end

function ledger:newEntry(valuesByField)
  -- Store an ordered (when iterated) copy of our input.
  local attrs = setmetatable(table.copy(valuesByField), { __pairs = table.orderedPairs })
  attrs.id = attrs.id or uuid()

  local ledgerMetatable = getmetatable(self)
  local entry = {
    -- Store relevant metadata from our ledger on individual entries.
    __metadata = {
      database_location = ledgerMetatable.database_location, -- TODO(dabrady) Do I need to do this?
      tableName = self.tableName,
      columns = table.keys(self.schema),
      ledger = self
    },

    -- Hold onto our attributes.
    __attributes = attrs
  }

  -- Attach various entry behaviors
  table.merge(entry, entryManagement)

  -- Generate getters for any table references this entry has.
  if ledgerMetatable.reference_columns then
    -- TODO(dabrady) Use table.merge instead of `attach` API
    referenceGetters.attach(
      entry,
      ledgerMetatable.reference_columns,
      ledgerMetatable.referenceLedgers
    )
  end

  return setmetatable(
    entry,
    {
      -- NOTE(dabrady) Need to `rawget` in our lookups here; if we were to
      -- access from the entry directly in the index function, we'd trigger
      -- an infinite recursion.
      __index = function (entry, k)
        local refs = rawget(entry, '__reference_getters')
        return
          -- Check our entry attributes first
          rawget(entry, '__attributes')[k]
        -- Allow shorcuts like `r.reference` instead of `r.__reference_getters.reference()`
        -- FIXME This will continue to the next line if `refs[k]()` returns nil. Stahp it
          or ( refs and refs[k] and refs[k]() )
        -- Or finally, delegate to our ledger
          or rawget(entry, '__metadata').ledger[k]
      end,

      -- Prioritize attribute setting over direct key insertion.
      -- TODO If we add support for modifying existing entries, ensure cached references
      -- are cleared when their corresponding reference column is modified (even on
      -- unsaved entries?)
      __newindex = function (entry, k, v)
        if entry.__attributes[k] then
          rawset(entry.__attributes, k, v)
        else
          rawset(entry,k,v)
        end
      end,

      -- TODO(dabrady) Modify to support displaying nil columns.
      -- The natural behavior of Lua is that a table with a key pointing to nil
      -- means that key isn't there at all, and is ignored by its index, meaning
      -- in our case that nil columns won't be displayed at all.
      -- e.g. Person{ name = 'Daniel', address = nil } --> <persons>{ name = Daniel }
      __tostring = function(entry, options)
        return string.format(
          "<%s>%s",
          -- Prefix the formatted table with the table name.
          entry.tableName,
          -- Trim any leading indentation from the formatting
          table.format(entry.__attributes, { depth = 2, startingIndentLvl = options and options.indent }):trim()
        )
      end
    }
  )
end

function ledger:addEntry(valuesByField)
  local entry = self:newEntry(valuesByField)
  return entry:persist()
end

return setmetatable(
  ledger,
  {
    __call = _newLedger
  }
)
