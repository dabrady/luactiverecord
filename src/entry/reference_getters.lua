local reference_getters = {}

function reference_getters.attach(entry, reference_columns, reference_ledgers)
  if not entry.__reference_getters then
    entry.__reference_getters = {}
  end

  setmetatable(
    entry.__reference_getters,
    -- A basic reference cache and cache-busting mechanism.
    {
      REFERENCE_CACHE = {},
      -- NOTE(dabrady) Opting to clear table instead of recreate to avoid breaking
      -- any pointers to the cache that might exist.
      __flush_ref_cache = function(self)
        for k,_ in pairs(self.REFERENCE_CACHE) do
          self.REFERENCE_CACHE[k] = nil
        end
      end
    }
  )

  -- A function that attempts to lookup an entry on a reference table whose primary key
  -- is the value of the given column on this row.
  local _getter_for = function(foreign_key_column, reference_table, ref_name)
    return function()
      -- Check the cache first and short-circuit the lookup if possible.
      local ref_cache = getmetatable(entry.__reference_getters).REFERENCE_CACHE
      local cached_ref = ref_cache[ref_name]
      if cached_ref then
        return cached_ref
      end

      local reference_ledger = assert(
        reference_ledgers[reference_table],
        'no available ledger for table "'..reference_table..'"'
      )

      -- Do nothing if the foreign key isn't populated.
      local foreign_key = entry[foreign_key_column]
      if foreign_key then
        -- NOTE(dabrady) Current implementation of `find` matches against the row `id` column,
        -- so the assumption here is that all foreign keys are row IDs.
        ref_cache[ref_name] = reference_ledger:find(foreign_key)
        return ref_cache[ref_name]
      else
        return nil
      end
    end
  end

  -- Generate a getter for each reference.
  for foreign_key_column, reference_table in pairs(reference_columns) do
    -- NOTE(dabrady) Assumption: foreign key columns named with '_id' suffix.
    -- TODO(dabrady) Consider making this configurable if it becomes a problem.
    local ref_name = foreign_key_column:chop('_id')

    --[[ TODO(dabrady)
      Consider making `__reference_getters` the ref cache itself, and make the cache-buster
      the `__call` event of its metatable, so you can do things like this:
          entry.__reference_getters.person --> Person{}
          entry.__reference_getters(true).person --> clears cache, then looks up, caches, and returns Person{}
    ]]

    entry.__reference_getters[ref_name] = _getter_for(foreign_key_column, reference_table, ref_name)
  end

  ---
  return entry
end

return reference_getters
