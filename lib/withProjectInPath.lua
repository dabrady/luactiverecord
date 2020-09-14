-- NOTE(dabrady) Lua's `package.path` contains search paths relative to the
-- working directory at the time the path is searched. This makes sense, but
-- annoys me: most of the time, I want that search to prioritize the local
-- project over the rest, and this gets me that behavior.
local projectDir = ...
assert(projectDir and type(projectDir) == 'string')
return function(fn)
  -- Temporarily modify loadpath
  local oldPath = package.path
  local oldCPath = package.cpath
  package.path = ''
    ..projectDir..'?.lua;'
    ..projectDir..'?/?.lua;'
    ..projectDir..'?/init.lua;'
    ..package.path
  package.cpath = ''
    ..projectDir..'src/bin/?.so;'
    ..package.cpath

  string.format('%s?.so;%s', Flow.spoonPath, oldCPath)

  -- Call the function that might require project files
  local res = fn()

  -- Reset loadpath
  package.path = oldPath
  package.cpath = oldCPath

  return res
end
