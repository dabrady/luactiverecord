require('vendors/lua-utils/table')
local uuid_generator = require('uuid')

-- Ledger behavior
local finders = require('src/ledger/finders')

-- Ledger entry behavior
local reference_getters = require('src/entry/reference_getters')
local entry_management = require('src/entry/management')

local ledger = {}
table.merge(ledger, finders)

local uuid = (function()
  uuid_generator.randomseed(math.random(0,2^32))
  local uuid_generator_seed = uuid_generator()
  return function()
    return uuid_generator(uuid_generator_seed)
  end
end)()

local function _new_ledger(_, args)
  local self = {
    table_name = args.table_name,
    schema = args.schema
  }

  return setmetatable(
    self,
    {
      __index = ledger,
      __call = ledger.new_entry,

      database_location = args.database_location,
      reference_columns = args.reference_columns,
      reference_ledgers = args.reference_ledgers
    }
  )
end

function ledger:new_entry(values_by_field)
  -- Store an ordered (when iterated) copy of our input.
  local attrs = setmetatable(table.copy(values_by_field), { __pairs = table.orderedPairs })
  attrs.id = attrs.id or uuid()

  local ledger_metatable = getmetatable(self)
  local entry = {
    -- Store relevant metadata from our ledger on individual entries.
    __metadata = {
      database_location = ledger_metatable.database_location, -- TODO(dabrady) Do I need to do this?
      table_name = self.table_name,
      columns = table.keys(self.schema),
      ledger = self
    },

    -- Hold onto our attributes.
    __attributes = attrs
  }

  -- Attach various entry behaviors
  table.merge(entry, entry_management)

  -- Generate getters for any table references this entry has.
  if ledger_metatable.reference_columns then
    -- TODO(dabrady) Use table.merge instead of `attach` API
    reference_getters.attach(
      entry,
      ledger_metatable.reference_columns,
      ledger_metatable.reference_ledgers
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
          entry.table_name,
          -- Trim any leading indentation from the formatting
          table.format(entry.__attributes, { depth = 2, startingIndentLvl = options and options.indent }):trim()
        )
      end
    }
  )
end

function ledger:add_entry(values_by_field)
  local entry = self:new_entry(values_by_field)
  return entry:persist()
end

return setmetatable(
  ledger,
  {
    __call = _new_ledger
  }
)
