-- lua/dadbod/adapter/sqlserver.lua
-- Port of autoload/db/adapter/sqlserver.vim

local url = require('dadbod.url')

local M = {}

function M.canonicalize(u)
  -- Convert ;a=b;c=d into ?a=b&c=d and normalize
  local s = u
  if s:match(';.*=') and (not s:match('%?')) then
    s = (s:gsub(';', '?'):gsub('%?$', '')):gsub(';', '&')
  end
  local P = url.parse(s)
  -- Normalize param keys (first letter lower unless fully uppercase), and booleans
  local new = {}
  for k, v in pairs(P.params or {}) do
    local canonical = (k:match('%l') and (k:sub(1,1):lower() .. k:sub(2))) or k
    if type(v) ~= 'table' and (v == 1 or v == true) then
      v = 'true'
    end
    if new[canonical] == nil then new[canonical] = v end
  end
  P.params = new

  return url.absorb_params(P, {
    user            = 'user',
    userName        = 'user',
    password        = 'password',
    server          = 'host',
    serverName      = 'host',
    port            = 'port',
    portNumber      = 'port',
    database        = 'database',
    databaseName    = 'database',
  })
end

local function server(P)
  local host = P.host or 'localhost'
  local port = P.port and (',' .. P.port) or ''
  return host .. port
end

local function boolean_param_flag(P, param, flag)
  local v = P.params[param] or P.params[(param:sub(1,1):upper()..param:sub(2))] or '0'
  if type(v) ~= 'string' then v = tostring(v) end
  return v:match('^[1tTyY]') and { flag } or {}
end

function M.interactive(u)
  local P = url.parse(u)
  local argv = {}
  if P.password then
    table.insert(argv, 'env')
    table.insert(argv, 'SQLCMDPASSWORD=' .. P.password)
  end
  local encrypt = P.params.encrypt or P.params.Encrypt
  local has_auth = P.params.authentication ~= nil

  local base = { 'sqlcmd', '-S', server(P) }
  local enc = {}
  if encrypt and encrypt ~= '' then
    table.insert(enc, '-N')
    if encrypt ~= '1' then table.insert(enc, encrypt) end
  end
  local trust = boolean_param_flag(P, 'trustServerCertificate', '-C')
  local auth = {}
  if not P.user and not has_auth then
    auth = { '-E' }
  elseif has_auth then
    auth = { '--authentication-method', P.params.authentication }
  end

  local tail = url.as_argv(u, '', '', '', '-U ', '', '-d ')
  for _, arr in ipairs({ base, enc, trust, auth, tail }) do
    for _, v in ipairs(arr) do table.insert(argv, v) end
  end
  return argv
end

function M.input(u, infile)
  local argv = M.interactive(u)
  table.insert(argv, '-i')
  table.insert(argv, infile)
  return argv
end

function M.dbext(u)
  local P = url.parse(u)
  return {
    srvname          = server(P),
    host             = '',
    port             = '',
    integratedlogin  = (P.user == nil),
  }
end

local function complete(u, query)
  local cmd = M.interactive(u)
  -- -h-1: no headers; -W: remove trailing spaces
  table.insert(cmd, '-h-1'); table.insert(cmd, '-W'); table.insert(cmd, '-Q'); table.insert(cmd, 'SET NOCOUNT ON; ' .. query)
  local lines, _ = require('dadbod')._systemlist(cmd) -- unsafe internal
  local out = {}
  for _, l in ipairs(lines) do
    local m = l:match('(%S+)')
    if m then table.insert(out, m) end
  end
  return out
end

function M.complete_database(u)
  local prefix = (u:match('^[^:]+://.-/') or u)
  return complete(prefix, 'SELECT NAME FROM sys.sysdatabases')
end

function M.tables(u)
  return complete(u, 'SELECT TABLE_NAME FROM information_schema.tables ORDER BY TABLE_NAME')
end

-- Default extensions if queried
function M.input_extension() return 'sql' end
function M.output_extension() return 'dbout' end

return M
