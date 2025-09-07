-- lua/dadbod/init.lua
-- Core engine (resolution, execution, buffer wiring, cancellations)

local M = {}

local url     = require('dadbod.url')
local adapter = require('dadbod.adapter')
local job     = require('dadbod.job')

-- State mirrors the script-local dictionaries in Vimscript
local passwords = {}      -- [canonical_url] = password
local buffers   = {}      -- [bufnr] = query
local inputs    = {}      -- [input_file] = query
local current_db = nil    -- transient (for ":" passthrough)

-- Utilities -----------------------------------------------------------------

local function getenv_expand(s)
  return (s:gsub('%%(%x%x)', function(h) return string.char(tonumber(h, 16)) end))
end

local function dotenv_expand(expr)
  -- If user's environment defines a function, you can wire it here.
  -- We keep standard expand for now.
  return vim.fn.expand(expr)
end

local function expand_all(str)
  -- Simplified: use Vim's expand() on %: and env/$ (mimic Vimscript behavior)
  return (str:gsub("(\\*)%$([%w_]+)", function(bs, name)
    if #bs % 2 == 1 then
      return bs .. '$' .. name
    end
    local v = vim.fn.getenv(name)
    return bs .. (v ~= vim.NIL and v or ('$'..name))
  end):gsub("%%", function() return "%" end))
end

local function path_is_abs(p)
  return p:match('^/') or p:match('^%a:[/\\]') ~= nil
end

-- Resolution / Canonicalization --------------------------------------------

local function get_scoped_default_db()
  -- Prefer w:, t:, b:, g: in that order; fall back to s:db (current_db)
  local scopes = { vim.w, vim.t, vim.b, vim.g }
  for _, scope in ipairs(scopes) do
    local v = rawget(scope, 'db')
    if v ~= nil and v ~= '' then return v end
  end
  if current_db ~= nil then return current_db end
  return nil
end

local function resolve(input_url)
  local u = input_url
  if not u or u == '' then
    local env = expand_all('$DATABASE_URL')
    if env ~= '$DATABASE_URL' then
      u = env
    end
    if (not u or u == '') then
      u = get_scoped_default_db()
    end
  elseif type(u) == 'number' then
    local qb = buffers[u]
    if not qb then
      error('DB: buffer ' .. u .. ' does not contain a DB result')
    end
    u = qb.db_url
  end

  u = u or ''
  -- Resolve scope variables like "w:db" etc.
  local c = 5
  while c > 0 do
    c = c - 1
    if type(u) == 'table' and (u.db_url or u.url) then
      u = u.db_url or u.url
    elseif type(u) == 'table' and u.scheme then
      u = url.format(u)
    elseif type(u) == 'string' and u:match('^[gtwb]:%w') then
      local ns = u:sub(1,1)
      local key = u:match('^%w+:(.*)$')
      local dict = ({ g = vim.g, t = vim.t, w = vim.w, b = vim.b })[ns]
      if dict and rawget(dict, key) then
        u = dict[key]
      else
        error('DB: no such variable ' .. u)
      end
    else
      break
    end
  end

  if type(u) ~= 'string' then
    error('DB: URL is not a string')
  end

  if u:match('^type=') or u:match('^profile=') then
    u = 'dbext:' .. u
  end

  u = (u:gsub('^jdbc:', ''))
  u = expand_all(u)

  -- If still empty, infer from current file path via adapters
  if (not u or u == '') and not vim.fn.expand('%'):match(':[/][/]') then
    local path = vim.fn.fnamemodify(vim.fn.expand('%:p:h'), ':p')
    while not path:match('^%w:[/\\]*$') and path ~= '/' do
      for _, scheme in ipairs(adapter.schemes()) do
        local resolved = adapter.call(scheme, 'test_directory', { path }, '')
        if resolved and resolved ~= '' then
          u = scheme .. ':' .. path
          break
        end
      end
      if u ~= '' then break end
      path = vim.fn.fnamemodify(path, ':h')
    end
  end

  if u:match('^[%a][%w%.%+%-]*:[/\\]') or u:match('^[/\\]') then
    u = 'file:' .. (u:gsub('\\', '/'))
  elseif #u > 0 and not u:match('^[%w%.%+%-]+:') then
    u = 'file:' .. (vim.fn.fnamemodify(u, ':p'):gsub('\\', '/'))
  end

  if u:match('^file:') then
    local file = url.file_path(u):gsub('[\\/]+$', '')
    for _, scheme in ipairs(adapter.schemes()) do
      local fn = vim.fn.isdirectory(file) == 1 and 'test_directory' or 'test_file'
      local resolved = adapter.call(scheme, fn, { file }, '')
      if resolved and resolved ~= '' then
        u = scheme .. ':' .. file .. (u:match('[#?].*') or '')
        break
      end
    end
  end

  if u:match('^file:') then
    error('DB: no adapter for file ' .. u:sub(6))
  end
  return u
end

local function canonicalize(u)
  local cur, old = u, ''
  local limit = 20
  while limit > 0 and cur ~= old do
    if cur == '' then error('DB: could not find database') end
    old = cur
    local scheme = cur:match('^[^:]+')
    local mapped = vim.g.db_adapters and vim.g.db_adapters[scheme] or scheme
    cur = (cur:gsub('^[^:]+', mapped))
    cur = adapter.call(cur, 'canonicalize', { cur }, cur) -- default passthrough
    limit = limit - 1
  end
  if limit > 0 then return cur end
  error('DB: infinite loop resolving URL')
end

function M.resolve(u) return canonicalize(resolve(u)) end

-- Job helpers ---------------------------------------------------------------

local function last_preview_buffer()
  local id = rawget(vim.t, 'db_last_preview_buffer')
  if id == nil then return -1 end
  return id
end

local function check_job_running(bang)
  local lpb = last_preview_buffer()
  if (not bang) and lpb and lpb ~= -1 then
    local q = vim.b[lpb] and vim.b[lpb].db
    if q and q.job and not q.job.exited then
      error('DB: Query already running for this tab')
    end
  end
end

-- Public: cancel current buffer query
function M.cancel(bufnr)
  bufnr = bufnr or vim.api.nvim_get_current_buf()
  local q = buffers[bufnr] or vim.b[bufnr].db
  if q and q.job and q.job.handle then
    q.canceled = true
    q.job:kill()
  end
end

-- Systemlist-like synchronous run (used during connect/auth probing)
local function systemlist(argv)
  local env, cmd = job.normalize_cmd(argv)
  local res = vim.system(cmd, { text = true, env = env }):wait()
  local lines = {}
  local out = (res.stdout or '') .. (res.stderr or '')
  out = out:gsub('\r\n', '\n')
  for line in out:gmatch('([^\n]*)\n?') do
    table.insert(lines, line)
  end
  if #lines > 0 and lines[#lines] == '' then table.remove(lines) end
  return lines, res.code or -1
end

-- Connect/auth --------------------------------------------------------------

function M.connect(u)
  local resolved = M.resolve(u)
  local final_url = resolved

  if passwords[resolved] then
    -- inject cached password into URL
    local enc = url.encode(passwords[resolved])
    final_url = (resolved:gsub('://([^:/@]*)@', '://%1:' .. enc .. '@'))
  end

  -- Build a minimal stdin payload for auth probe if adapter wants it
  local auth_input = adapter.call(final_url, 'auth_input', {}, "\n")
  if auth_input == false then
    return final_url
  end

  -- Form command for probe
  local tmp = vim.fn.tempname()
  local f = io.open(tmp, 'wb'); if f then f:write(auth_input or ""); f:close() end
  local cmd, _ = adapter.dispatch(final_url, adapter.supports(final_url, 'filter') and 'filter' or 'interactive')
  if not cmd then
    vim.fn.delete(tmp)
    error("DB: no adapter command")
  end

  local lines, code = systemlist(cmd)
  if code ~= 0 then
    local pattern = adapter.call(final_url, 'auth_pattern', {}, 'auth|login')
    local joined = table.concat(lines, "\n")
    if joined:lower():match(pattern) and resolved:match('^[^:]*://[^:/@]*@') then
      local password = vim.fn.inputsecret('Password: ')
      local enc = url.encode(password or '')
      final_url = (resolved:gsub('://([^:/@]*)@', '://%1:' .. enc .. '@'))
      local lines2, code2 = systemlist((select(1, adapter.dispatch(final_url, adapter.supports(final_url,'filter') and 'filter' or 'interactive'))))
      if code2 == 0 then
        passwords[resolved] = password
        vim.fn.delete(tmp)
        return final_url
      end
    end
    vim.fn.delete(tmp)
    error('DB exec error: ' .. table.concat(lines, "\n"))
  end

  vim.fn.delete(tmp)
  return final_url
end

-- Query I/O -----------------------------------------------------------------

local function filter_for(url_str, infile, prefer_filter)
  if adapter.supports(url_str, 'input') and not (prefer_filter and adapter.supports(url_str, 'filter')) then
    -- Use adapter-provided "input" command (typically adds "-i <file>")
    local cmd = adapter.dispatch(url_str, 'input', infile)
    return cmd, ''
  end
  local op = adapter.supports(url_str, 'filter') and 'filter' or 'interactive'
  local cmd = adapter.dispatch(url_str, op)
  return cmd, infile
end

local function fire_user(pattern) -- fire "User {pattern}" like Vimscript
  vim.api.nvim_exec_autocmds('User', { pattern = pattern, modeline = false })
end

local function init_result_buffer(infile, outfile, q)
  -- Record buffer association
  local bufnr = vim.fn.bufnr(outfile)
  buffers[bufnr] = q
  vim.b[bufnr].db = q
  vim.b[bufnr].dadbod = q
  vim.w.db = q.db_url

  vim.bo[bufnr].modifiable   = false
  vim.bo[bufnr].readonly     = true
  vim.bo[bufnr].buflisted    = false
  vim.bo[bufnr].bufhidden    = 'delete'
  vim.wo[vim.api.nvim_win_get_buf(0) == bufnr and 0 or 0].wrap   = false
  vim.wo[0].list   = false

  -- Keys: q, gq, r, R, <C-c>
  -- q -> hint to use gq
  vim.keymap.set('n', 'q', function()
    vim.api.nvim_echo({{ 'DB: q map has been replaced by gq', 'ErrorMsg' }}, false, {})
  end, { buffer = bufnr, silent = true })

  -- gq -> close buffer
  vim.keymap.set('n', 'gq', function() vim.cmd('bdelete') end,
    { buffer = bufnr, silent = true, nowait = true })

  -- r -> rerun first line of input file as :DB <line>
  vim.keymap.set('n', 'r', function()
    local first = vim.fn.readfile(q.input, '', 1)[1] or ''
    vim.cmd('DB ' .. first)
  end, { buffer = bufnr, nowait = true })

  -- R -> re-run the same query job
  vim.keymap.set('n', 'R', function()
    -- Re-dispatch with same query
    local ok, err = pcall(function()
      fire_user(q.output .. '/DBExecutePre')
      vim.notify('DB: Running query...', vim.log.levels.INFO)
      q.job = job.run(q.cmd, q.infile_content, function(lines, code, runtime)
        q.exit_status = code
        q.runtime = runtime
        vim.fn.writefile(lines, q.output, 'b')

        local wins = vim.fn.win_findbuf(vim.fn.bufnr(q.output))
        local msg = ('DB: Query %s %s %.3fs'):format(q.output, code ~= 0 and 'aborted after' or 'finished in', runtime)
        if #wins > 0 then
          local cur = vim.api.nvim_get_current_win()
          vim.api.nvim_set_current_win(wins[1])
          vim.cmd('edit!')
          vim.api.nvim_set_current_win(cur)
        elseif not q.canceled then
          msg = msg .. ' (no window?)'
        end
        fire_user(q.output .. '/DBExecutePost')
        vim.notify(msg, code ~= 0 and vim.log.levels.WARN or vim.log.levels.INFO)
      end)
    end)
    if not ok then
      vim.notify(err, vim.log.levels.ERROR)
    end
  end, { buffer = bufnr, silent = true })

  -- Ctrl-C -> cancel
  vim.keymap.set('n', '<C-c>', function() M.cancel(bufnr) end, { buffer = bufnr, silent = true })

  -- Cancel job if buffer unloads
  vim.api.nvim_create_autocmd('BufUnload', {
    buffer = bufnr,
    once = true,
    callback = function()
      if q.job then q.job:kill() end
    end,
  })
end

-- Command parsing helpers ----------------------------------------------------

local URL_PAT = [[%([abgltvw]:\w\+\|\a[%w%.-+]\+:\S*\|\$[%a_]\S*\|[.~]\=/\S*\|[.~]\|\%(type\|profile\)=\S\+\)\S\@!]]

local function cmd_split(cmd)
  local url_prefix = cmd:match('^' .. URL_PAT)
  local rest = cmd:gsub('^' .. URL_PAT .. '%s*', '')
  return url_prefix or '', rest
end

local function compute_range_content(opts, conn, cmd_tail)
  -- Returns either:
  --   { infile = <path> } or { lines = {...} }
  -- Mirrors the original logic closely but simplified.
  local maybe_infile = cmd_tail:match('^<%s*(.*%S)')
  if opts.range >= 0 then
    if opts.line1 == 0 then
      -- Visual/Operator pending yank, reconstruct like original
      -- Simplify: use getreg('"') and selection; this is a close approximation.
      local saved_sel, saved_cb = vim.o.selection, vim.o.clipboard
      vim.o.selection = 'inclusive'
      vim.o.clipboard = vim.o.clipboard:gsub('unnamedplus', ''):gsub('unnamed', '')
      vim.cmd('silent normal! `[v`]"zy')
      local text = vim.fn.getreg('z') or ''
      vim.o.selection = saved_sel
      vim.o.clipboard = saved_cb

      local s = text
      if #cmd_tail > 0 then s = cmd_tail .. ' ' .. s end
      s = s:gsub('\n$', '')
      local lines = vim.split(adapter.call(conn, 'massage', { s }, s), '\n', { plain = true })
      return { lines = lines }
    elseif maybe_infile then
      local total = vim.fn.readfile(vim.fn.expand(maybe_infile), '', opts.line2)
      local seg = {}
      for i = opts.line1, #total do seg[#seg+1] = total[i] end
      return { lines = seg }
    elseif opts.line1 == 1 and opts.line2 == vim.fn.line('$') and cmd_tail == '' and vim.bo.modified == false and vim.fn.filereadable(vim.fn.expand('%')) == 1 then
      return { infile = vim.fn.expand('%:p') }
    else
      local lines = vim.fn.getline(opts.line1, opts.line2)
      if #cmd_tail > 0 then
        lines[1] = (cmd_tail .. ' ' .. (lines[1] or ''))
      end
      return { lines = lines }
    end
  elseif opts.line1 == 0 or maybe_infile then
    return { infile = vim.fn.expand(maybe_infile or '%') }
  else
    local s = adapter.call(conn, 'massage', { cmd_tail }, cmd_tail)
    return { lines = vim.split(s, '\n', { plain = true }) }
  end
end

-- Main entry (user command) -------------------------------------------------

function M.execute_command(opts)
  local arg = opts.args
  local mods = opts.smods and opts.smods or ''
  if not mods:match('%f[%w](aboveleft|belowright|leftabove|rightbelow|topleft|botright|tab)%f[^%w]') then
    mods = 'botright ' .. mods
  end

  -- Assignment form: "= <target>" when no range
  local url_head, tail = cmd_split(arg)
  if tail:match('^=') and opts.line2 <= 0 then
    local target = tail:gsub('^=%s*', '')
    local var = url_head
    if var == '' then var = 'w:db'
    elseif var:match('^%w:$') then var = var .. 'db'
    end
    if var:match('^[abgltwv:]') or var:match('^%$') then
      local conn = M.connect(target)
      local ns = var:sub(1,1)
      local name = var:match('^%w:(.+)$')
      if not name then error('DB: invalid variable: '..var) end
      local dict = ({ a=false, b=vim.b, g=vim.g, l=false, t=vim.t, v=false, w=vim.w })[ns]
      if not dict then error('DB: invalid variable: '..var) end
      dict[name] = conn
      return
    else
      error('DB: invalid variable: ' .. var)
    end
  end

  -- Resolve connection
  local conn = M.connect(url_head)
  if not conn or conn == '' then
    vim.api.nvim_err_writeln('DB: no URL given and no default connection')
    return
  end

  -- ":" passthrough (run an ex-command with s:db set)
  if tail:match('^:') then
    current_db = conn
    local ok, err = pcall(function() vim.cmd(tail) end)
    current_db = nil
    if not ok then vim.api.nvim_err_writeln(err) end
    return
  end

  -- Interactive mode when no SQL and no explicit range
  if tail == '' and opts.line2 < 0 then
    if not adapter.supports(conn, 'interactive') then
      local scheme = url.parse(conn).scheme
      vim.api.nvim_err_writeln('DB: interactive mode not supported for ' .. scheme)
      return
    end
    local cmd = adapter.dispatch(conn, 'interactive')
    local env, argv = job.normalize_cmd(cmd)
    -- terminal
    local termbuf = vim.api.nvim_create_buf(true, false)
    local wcmd = { mods = mods }
    vim.api.nvim_cmd({ cmd = 'botright', args = { 'split' } }, wcmd) -- open a split
    vim.api.nvim_set_current_buf(termbuf)
    vim.fn.termopen(argv, { env = env })
    vim.cmd('startinsert')
    return
  end

  -- Ensure no other job in this tab unless bang
  check_job_running(opts.bang)

  -- Build input/output files
  local tmp = vim.fn.tempname()
  local infile = tmp .. '.' .. (adapter.call(conn, 'input_extension', {}, 'sql'))
  local outfile = tmp .. '.' .. (adapter.call(conn, 'output_extension', {}, 'dbout'))

  local ro = compute_range_content(opts, conn, tail)
  infile = vim.fn.fnamemodify(infile, ':p')

  if ro.lines then
    vim.fn.writefile(ro.lines, infile)
  end

  local prefer_filter = ro.lines ~= nil
  local cmd, _ = filter_for(conn, infile, prefer_filter)
  local env, argv = job.normalize_cmd(cmd)

  local q = {
    db_url = conn,
    input  = infile,
    output = outfile,
    bang   = opts.bang,
    mods   = mods,
    prefer_filter = prefer_filter,
    cmd    = cmd,
    infile_content = nil, -- we always use files for adapters that need -i
    job    = nil,
  }

  inputs[infile] = q
  vim.fn.writefile({}, outfile, 'b')

  -- Hook result buffer initialization (after read)
  vim.api.nvim_create_autocmd('BufReadPost', {
    pattern = outfile:gsub('\\', '/'),
    once = true,
    callback = function()
      init_result_buffer(infile, outfile, q)
    end
  })

  -- Open location (pedit unless bang)
  if opts.bang then
    vim.api.nvim_cmd({ cmd = 'split', args = { outfile } }, { mods = mods })
  else
    vim.api.nvim_cmd({ cmd = 'pedit', args = { outfile } }, { mods = mods })
  end

  -- Save “last preview buffer” in this tabpage, guard future runs
  vim.t.db_last_preview_buffer = vim.fn.bufnr(outfile)

  -- Fire pre, run job
  fire_user(outfile .. '/DBExecutePre')
  vim.notify('DB: Running query...', vim.log.levels.INFO)
  q.job = job.run(cmd, q.infile_content, function(lines, code, runtime)
    q.exit_status = code
    q.runtime = runtime
    vim.fn.writefile(lines, outfile, 'b')

    local wins = vim.fn.win_findbuf(vim.fn.bufnr(outfile))
    local msg = ('DB: Query %s %s %.3fs'):format(outfile, code ~= 0 and 'aborted after' or 'finished in', runtime)
    if #wins > 0 then
      local cur = vim.api.nvim_get_current_win()
      vim.api.nvim_set_current_win(wins[1])
      vim.cmd('edit!')
      vim.api.nvim_set_current_win(cur)
    elseif not q.canceled then
      msg = msg .. ' (no window?)'
    end
    fire_user(outfile .. '/DBExecutePost')
    vim.notify(msg, code ~= 0 and vim.log.levels.WARN or vim.log.levels.INFO)
  end)
end

-- dbext interop toggle
function M.manage_dbext()
  local b = rawget(vim.b, 'dadbod_manage_dbext')
  if b ~= nil then return b end
  local g = rawget(vim.g, 'dadbod_manage_dbext')
  return g ~= nil and g or false
end

return M
