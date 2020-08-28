local _,_,module = ...
module = module or {}
assert(type(module) == 'table', 'must provide a table to extend; gave us a '..type(module))

local sqlite = require('hs.sqlite3')

local function pragma(db, statement)
  return assert(db:exec('PRAGMA '..statement..';') == sqlite.OK, db:error_message())
end
module.pragma = pragma

function module.enable_foreign_key_constraints(db)
  -- Turn on foreign key constraints.
  return pragma(db, 'foreign_keys = ON');
end
