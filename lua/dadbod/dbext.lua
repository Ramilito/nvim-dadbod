-- lua/dadbod/dbext.lua
-- Minimal dbext interop (optional)

local url     = require('dadbod.url')
local adapter = require('dadbod.adapter')

local M = {}

local dbext_vars = {
  'type','profile','bin','user','passwd','dbname','srvname','host','port','dsnname','extra','integratedlogin','buffer_defaulted'
}

function M.clobber_dbext(u)
  local conn = u or require('dadbod').resolve('')
  local opts, parsed

  if conn:match('^dbext:') then
    -- Not implemented: db#adapter#dbext#parse (out of scope)
    opts = {}
    parsed = {}
  else
    conn = require('dadbod').resolve(conn)
    if conn == '' then
      for _, k in ipairs(dbext_vars) do vim.b['dbext_' .. k] = nil end
      return
    end
    parsed = url.parse(conn)
    opts = adapter.call(conn, 'dbext', { conn }, {})
    opts = vim.tbl_extend('keep', opts, {
      type    = (parsed.scheme or ''):upper(),
      dbname  = parsed.opaque or ((parsed.path or ''):sub(2)),
      host    = parsed.host or '',
      port    = parsed.port or '',
      user    = parsed.user or '',
      passwd  = parsed.password or '',
      buffer_defaulted = 1,
    })
    for dbext, dad in pairs(vim.g.dbext_schemes or {}) do
      if dad:upper() == opts.type then opts.type = dbext; break end
    end
  end

  for _, key in ipairs(dbext_vars) do
    local val = opts[key] or ((parsed.params or {})[key])
    vim.b['dbext_' .. key] = val
  end
  return opts
end

return M
