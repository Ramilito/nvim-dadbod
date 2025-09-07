-- plugin/dadbod.lua
-- Neovim 0.11+ only

if vim.g.loaded_dadbod ~= nil then
  return
end
vim.g.loaded_dadbod = 1

-- Defaults & compatibility shims (mimic the original globals)
vim.g.db_adapters = vim.g.db_adapters or {}
for k, v in pairs({
  sqlite3 = 'sqlite',
  postgres = 'postgresql',
  trino   = 'presto',
}) do
  if vim.g.db_adapters[k] == nil then vim.g.db_adapters[k] = v end
end

-- These mirror the original "special adapter names" globals
-- (Kept as-is; adapters are loaded via Lua. These globals are harmless if unused.)
vim.g["db_adapter_mongodb#srv"] = vim.g["db_adapter_mongodb#srv"] or 'db#adapter#mongodb#'
vim.g["db_adapter_rediss"]      = vim.g["db_adapter_rediss"]      or 'db#adapter#redis#'

vim.g.dbext_schemes = vim.g.dbext_schemes or {}
for k, v in pairs({
  ASA    = 'sybase',
  MYSQL  = 'mysql',
  ORA    = 'oracle',
  PGSQL  = 'postgresql',
  SQLITE = 'sqlite',
  SQLSRV = 'sqlserver',
}) do
  if vim.g.dbext_schemes[k] == nil then vim.g.dbext_schemes[k] = v end
end

local db = require('dadbod')

-- :DB command ---------------------------------------------------------------
vim.api.nvim_create_user_command('DB', function(opts)
  db.execute_command({
    smods   = opts.smods,                 -- ex-modifiers (botright, tab, etc.)
    bang    = opts.bang,                  -- boolean
    line1   = opts.line1,                 -- range start
    line2   = opts.line2,                 -- range end / kind
    range   = opts.range,                 -- -1 (none), 0 (char), 1 (line), 2 (block), 3 (visual)
    args    = opts.args,                  -- raw argument string
  })
end, {
  bang   = true,
  nargs  = '?',
  range  = -1,
  complete = function(arglead, cmdline, cursorpos)
    return require('dadbod.completion').command_complete(arglead, cmdline, cursorpos)
  end,
})

-- Autocommands --------------------------------------------------------------
local aug = vim.api.nvim_create_augroup('dadbod', { clear = true })

-- dbext interop hooks (opt-in via g:dadbod_manage_dbext or b:dadbod_manage_dbext)
vim.api.nvim_create_autocmd('User', {
  group = aug,
  pattern = 'dbextPreConnection',
  callback = function()
    if db.manage_dbext() then
      require('dadbod.dbext').clobber_dbext()
    end
  end,
})

-- Keep identical behavior for dbext default buffer marker
vim.api.nvim_create_autocmd('BufNewFile', {
  group = aug,
  pattern = { 'Result', 'Result-*' },
  callback = function(args)
    if not db.manage_dbext() then return end
    local prev = vim.fn.getbufvar('#', 'dbext_buffer_defaulted')
    if prev ~= nil and prev ~= "" then
      vim.fn.setbufvar('#', 'dbext_buffer_defaulted', "0-by-dadbod")
    end
  end,
})

-- Match original: *.dbout gets tabstop=8
vim.api.nvim_create_autocmd('BufReadPost', {
  group = aug,
  pattern = '*.dbout',
  callback = function()
    vim.bo.tabstop = 8
  end,
})
