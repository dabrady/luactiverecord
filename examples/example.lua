local OLD_PATH = package.path
package.path = '/Users/daniel.brady/projects/LUActiveRecord/?.lua;'..OLD_PATH
-------

LUActiveRecord = require('LUActiveRecord')
LUActiveRecord.setDefaultDatabase('/Users/daniel.brady/github/LUActiveRecord/examples/example.db')

Person = LUActiveRecord{
  tableName = 'persons',
  columns = {
    name = 'TEXT NOT NULL',
    age = 'INTEGER'
  }
}

-------
package.path = OLD_PATH
