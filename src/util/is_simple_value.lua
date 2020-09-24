--[[
  NOTE(dabrady) Non-simple datatypes are:
  - table
  - function
  - userdata
  - thread
]]
return function(v)
  return table.contains({"nil", "string", "number", "boolean"}, type(v))
end
