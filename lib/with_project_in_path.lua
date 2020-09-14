-- NOTE(dabrady) Lua's `package.path` contains search paths relative to the
-- working directory at the time the path is searched. This makes sense, but
-- annoys me: most of the time, I want that search to prioritize the local
-- project over the rest, and this gets me that behavior.
local project_dir = ...
assert(project_dir and type(project_dir) == "string")
return function(fn)
  -- Temporarily modify loadpath
  local old_path = package.path
  local old_cpath = package.cpath
  package.path = ""
    ..project_dir.."?.lua;"
    ..project_dir.."?/?.lua;"
    ..project_dir.."?/init.lua;"
    ..package.path
  package.cpath = ""
    ..project_dir.."src/bin/?.so;"
    ..package.cpath

  string.format("%s?.so;%s", Flow.spoonPath, old_cpath)

  -- Call the function that might require project files
  local res = fn()

  -- Reset loadpath
  package.path = old_path
  package.cpath = old_cpath

  return res
end
