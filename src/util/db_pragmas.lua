local sqlite = require('hs.sqlite3')

local pragmas = {}

function pragmas.exec_pragma(db, statement)
  return assert(db:exec('PRAGMA '..statement..';') == sqlite.OK, db:error_message())
end

function pragmas.enable_foreign_key_constraints(db)
  return pragmas.exec_pragma(db, 'foreign_keys = ON');
end

return pragmas
