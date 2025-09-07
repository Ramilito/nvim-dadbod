-- lua/dadbod/url.lua
-- Parse/format/encode helpers (Lua port of autoload/db/url.vim core)

local M = {}

local function pct_decode(s)
  return (s:gsub('%%(%x%x)', function(h) return string.char(tonumber(h, 16)) end))
end

local function pct_encode(s, extra)
  local pat = '[?@=&<>%%%#%s' .. (extra or '') .. ']'
  return (s:gsub(pat, function(ch) return string.format('%%%02X', string.byte(ch)) end))
end

function M.decode(s) return pct_decode(s) end
function M.encode(s) return pct_encode(s, '/:+') end

-- Normalizes filesystem-like paths (incl. triple-slash handling)
local function canonicalize_path(p)
  p = p:gsub('\\', '/')
  if p:match('^///[%./~]') or p:match('^///%w:/') then
    p = vim.fn.fnamemodify(p:sub(4), ':p')
  elseif p:match('^///') then
    p = p:sub(4)
    if vim.fn.getftype(p) ~= '' then
      p = vim.fn.fnamemodify(p, ':p')
    elseif vim.fn.has('win32') == 1 then
      p = 'C:/' .. p
    else
      p = '/' .. p
    end
  elseif p ~= '' then
    p = vim.fn.fnamemodify(p, ':p')
  end
  return p:gsub('\\', '/')
end

function M.file_path(u)
  local path = u:match('^[^:]+:(.-)%f[?#]')
  if not path then path = u:match('^[^:]+:(.+)$') or '' end
  path = canonicalize_path(M.decode(path))
  if vim.opt.shellslash:get() == false then
    return path:gsub('/', '\\')
  end
  return path
end

function M.fragment(u) return (u:match('#(.*)') or '') end

function M.path_encode(s, extra)
  return pct_encode(s, extra)
end

function M.as_argv(u, host, port, socket, user, password, db)
  local P = M.parse(u)
  local args = {}

  if (P.host or ''):match('/') and socket ~= '' then
    for _, a in ipairs(vim.split(socket, ' ', { plain = true })) do table.insert(args, a) end
    args[#args] = args[#args] .. P.host
  elseif P.host and host ~= '' then
    for _, a in ipairs(vim.split(host, ' ', { plain = true })) do table.insert(args, a) end
    args[#args] = args[#args] .. P.host
  end
  if P.port and port ~= '' then
    for _, a in ipairs(vim.split(port, ' ', { plain = true })) do table.insert(args, a) end
    args[#args] = args[#args] .. P.port
  end
  if P.user and user ~= '' then
    for _, a in ipairs(vim.split(user, ' ', { plain = true })) do table.insert(args, a) end
    args[#args] = args[#args] .. P.user
  end
  if P.password and password ~= '' then
    for _, a in ipairs(vim.split(password, ' ', { plain = true })) do table.insert(args, a) end
    args[#args] = args[#args] .. P.password
  end
  local dbname
  if (P.path or '') ~= '' and P.path ~= '/' then
    dbname = (P.path or ''):gsub('^/', '')
  elseif P.opaque then
    dbname = M.decode((P.opaque:gsub('%?.*', '')))
  end
  if dbname then
    if db == '' then table.insert(args, '') else
      for _, a in ipairs(vim.split(db, ' ', { plain = true })) do table.insert(args, a) end
    end
    args[#args] = args[#args] .. dbname
  end
  return args
end

function M.as_args(u, host, port, socket, user, password, db)
  local argv = M.as_argv(u, host, port, socket, user, password, db)
  local function shellescape(s)
    if s:match("^[%w_/:%.%-]+$") then return s end
    return vim.fn.shellescape(s)
  end
  local out = {}
  for _, a in ipairs(argv) do table.insert(out, ' "' .. shellescape(a) .. '"') end
  return table.concat(out, '')
end

function M.parse(u)
  if type(u) == 'table' then
    if u.db_url then
      u = u.db_url
    elseif u.url then
      u = u.url
    elseif u.scheme and u.params then
      return vim.deepcopy(u)
    else
      error('DB: invalid URL')
    end
  end
  local fragment = u:match('#(.*)') or ''
  local s = u:gsub('#.*', '')

  local params = {}
  local query = (s:match('%?(.*)') or '')
  for item in query:gmatch('[^&;]+') do
    if not item:match('=') and params[item] == nil then
      params[item] = 1
    else
      local k, v = item:match('^([^=]*)=(.*)$')
      if k then
        v = M.decode((v:gsub('+', ' ')))
        if params[k] and params[k] ~= 1 then
          params[k] = params[k] .. '\f' .. v
        else
          params[k] = v
        end
      end
    end
  end
  s = s:gsub('%?.*', '')

  local scheme = '^([%w%.%+%-]+)'
  local m = { s:match(scheme .. '://([^@/:]*):?([^@/]*)@(%[[%x:]+%]|[^:/;,]*):?(%d*)(/*.*)$') }
  if #m > 0 then
    local user, pass, host, port, path = table.unpack(m)
    if host:match('^%[.*%]$') then host = host:sub(2, -2) end
    return vim.tbl_filter(function(v) return v ~= '' and v ~= nil end, {
      scheme  = s:match('^([^:]+)'),
      user    = M.decode(user or ''),
      password= M.decode(pass or ''),
      host    = M.decode(host or ''),
      port    = port ~= '' and port or nil,
      path    = M.decode((path == '' or path == nil) and '/' or path),
      params  = params,
      fragment= fragment,
    })
  end
  local m2 = { s:match(scheme .. ':(.*)') }
  if #m2 > 0 then
    return vim.tbl_filter(function(v) return v ~= '' and v ~= nil end, {
      scheme  = s:match('^([^:]+)'),
      opaque  = m2[1],
      params  = params,
      fragment= fragment,
    })
  end
  error('DB: invalid URL ' .. u)
end

function M.absorb_params(u, map)
  local P = M.parse(u)
  if not P.params then return u end
  for k, dest in pairs(map) do
    if P.params[k] ~= nil then
      if dest == 'database' then
        P.path = '/' .. P.params[k]
        P.params[k] = nil
      elseif dest == '' then
        P.params[k] = nil
      else
        P[dest] = P.params[k]
        P.params[k] = nil
      end
    end
  end
  return M.format(P)
end

function M.format(P)
  if type(P) == 'string' then return P end
  local out
  if P.opaque or not P.path then
    out = P.scheme .. ':' .. (P.opaque or '')
  else
    out = P.scheme .. '://'
  end
  if P.user then out = out .. M.encode(P.user) end
  if P.password then out = out .. ':' .. M.encode(P.password) end
  if P.user or P.password then out = out .. '@' end
  if P.host and P.host:match('^[%x:]*:[%x:]*$') then
    out = out .. '[' .. P.host .. ']'
  else
    out = out .. M.encode(P.host or '')
  end
  if P.port then out = out .. ':' .. P.port end
  if P.path then out = out .. M.path_encode(P.path) end
  if P.params and next(P.params) ~= nil then
    local keys = vim.tbl_keys(P.params); table.sort(keys)
    local parts = {}
    for _, k in ipairs(keys) do
      local v = P.params[k]
      if v == 1 or v == true then
        table.insert(parts, k)
      else
        local vals = type(v) == 'table' and v or vim.split(v, '\f', { plain = true })
        for _, vv in ipairs(vals) do
          table.insert(parts, k .. '=' .. (M.encode(vv):gsub('%%20', '+')))
        end
      end
    end
    out = out .. '?' .. table.concat(parts, '&')
  elseif P.query then
    out = out .. '?' .. P.query
  end
  if P.fragment then out = out .. '#' .. P.fragment end
  return out
end

function M.safe_format(u)
  local P = M.parse(u)
  P.password = nil
  return M.format(P)
end

return M
