-- lua/dadbod/completion.lua
-- Completions for :DB (URLs, files, tables, databases)

local M = {}
local adapter = require('dadbod.adapter')
local url     = require('dadbod.url')

local function schemes_list()
  local list = adapter.schemes()
  table.sort(list)
  return list
end

local function glob_complete(pattern)
  local dir, rest = pattern:match('^(.*[/\\])([^/\\]*)$')
  local base = dir or ''
  local g = vim.fn.glob((dir or '') .. (rest or '') .. '*', false, true)
  local out = {}
  for _, p in ipairs(g) do
    local isdir = vim.fn.isdirectory(p) == 1
    p = p:gsub('\\','/')
    table.insert(out, base .. p .. (isdir and '/' or ''))
  end
  return out
end

local URL_PAT = [[%([abgltvw]:\w\+\|\a[%w%.-+]\+:\S*\|\$[%a_]\S*\|[.~]\=/\S*\|[.~]\|\%(type\|profile\)=\S\+\)\S\@!]]

local function cmd_split(s)
  local url_prefix = s:match('^' .. URL_PAT)
  local rest = s:gsub('^' .. URL_PAT .. '%s*', '')
  return url_prefix or '', rest
end

local function url_complete(A)
  -- If no ":" yet, complete schemes
  if not A:match(':') then
    local out = {}
    for _, s in ipairs(schemes_list()) do table.insert(out, s .. ':') end
    return out
  end
  -- For "b: g: w: t:" variable lookups, suggest keys that contain a valid URL-ish value
  if A:match('^[bgtvw]:') then
    local ns = A:sub(1,1)
    local dict = ({ b = vim.b, g = vim.g, t = vim.t, v = vim.v, w = vim.w })[ns]
    local out = {}
    for k, v in pairs(dict) do
      if type(v) == 'string' and v:match('^[%w%.%+%-]+:') then
        table.insert(out, ns .. ':' .. k)
      end
    end
    return out
  end
  -- Delegate to adapter for special completes
  if A:match('#') and adapter.supports(A, 'complete_fragment') then
    local base = A:gsub('#.*', '#')
    local list = adapter.dispatch(A, 'complete_fragment')
    return vim.tbl_map(function(x) return base .. x end, list)
  end
  local rest = A:match(':(.*)')
  if rest and rest:match('^//.*/[^?]*$') and adapter.supports(A, 'complete_database') then
    local base = A:gsub('://.-/.*', '')
    local list = adapter.dispatch(A, 'complete_database')
    return vim.tbl_map(function(x) return base .. x end, list)
  end
  if rest and rest:match('^//[^/#?]*$') then
    local base = A:gsub('.*[@/].*', '')
    return { base .. '/', base .. 'localhost/' }
  end
  if adapter.supports(A, 'complete_opaque') then
    local list = adapter.dispatch(A, 'complete_opaque')
    local scheme = A:match('^[^:]+')
    return vim.tbl_map(function(x) return scheme .. ':' .. x end, list)
  end
  -- Filesystem paths for file-like adapters
  return glob_complete(A)
end

function M.command_complete(A, L, P)
  local arg = (L:sub(1, P)):gsub('^.-DB!?%s*', '')
  -- Assignment form: "<scope>:name = <url>"
  if arg:match('^%w:%w+%s*=%s*%S*$') or arg:match('^%$%w+%s*=%s*%S*$') then
    return url_complete(A)
  end

  local url_head, tail = cmd_split(arg)
  if tail:match('^<') then
    return glob_complete(A)
  elseif A ~= arg then
    -- We have a URL already; try table name completion via adapter
    local ok, conn = pcall(require('dadbod').connect, url_head)
    if ok and conn and adapter.supports(conn, 'tables') then
      return adapter.call(conn, 'tables', { conn }, {})
    end
    return {}
  elseif A:match('^[%a]:[/\\]') or A:match('^[.%/$]') then
    return glob_complete(A)
  elseif A:match('^[%w%.%+%-]+:?$') or A == '' then
    return url_complete(A)
  end
  return {}
end

return M
