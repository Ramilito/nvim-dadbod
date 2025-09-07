-- lua/dadbod/adapter/init.lua
-- Adapter loader/dispatcher

local M = {}

-- Return canonical adapter module name from scheme (after g:db_adapters mapping)
local function adapter_name(adapter_or_url)
  local scheme
  if type(adapter_or_url) == 'string' then
    scheme = adapter_or_url:match('^([^:]+)')
  elseif type(adapter_or_url) == 'table' and adapter_or_url.db_url then
    scheme = adapter_or_url.db_url:match('^([^:]+)')
  else
    error('DB: no URL')
  end
  local mapped = (vim.g.db_adapters or {})[scheme] or scheme
  return mapped
end

local loaded = {}

local function load(adapter)
  if loaded[adapter] then return loaded[adapter] end
  local lua_mod = 'dadbod.adapter.' .. adapter
  local ok, mod = pcall(require, lua_mod)
  if ok and type(mod) == 'table' then
    loaded[adapter] = mod
    return mod
  end
  error('DB: no adapter for ' .. adapter)
end

function M.supports(adapter_or_url, fn)
  local name = adapter_name(adapter_or_url)
  local ok, mod = pcall(load, name)
  if not ok then return false end
  return type(mod[fn]) == 'function'
end

function M.call(adapter_or_url, fn, args, default)
  local name = adapter_name(adapter_or_url)
  local ok, mod = pcall(load, name)
  if not ok or type(mod[fn]) ~= 'function' then
    return default
  end
  return mod[fn](table.unpack(args or {}))
end

function M.dispatch(url_or_adapter, fn, ...)
  local name = adapter_name(url_or_adapter)
  local mod = load(name)
  if type(mod[fn]) ~= 'function' then
    error('DB: adapter does not implement ' .. fn)
  end
  return mod[fn](url_or_adapter, ...)
end

function M.schemes()
  -- Find adapters shipped on runtimepath: lua/dadbod/adapter/*.lua
  local files = vim.api.nvim_get_runtime_file('lua/dadbod/adapter/*.lua', true)
  local names = {}
  for _, f in ipairs(files) do
    local base = f:match('adapter/(.+)%.lua$')
    if base and base ~= 'init' then table.insert(names, base) end
  end
  -- Also include remapped schemes from g:db_adapters keys
  for k, _ in pairs(vim.g.db_adapters or {}) do
    if not vim.tbl_contains(names, k) then table.insert(names, k) end
  end
  table.sort(names)
  return names
end

return M
