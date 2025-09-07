-- lua/dadbod/job.lua
-- Thin wrapper around vim.system for combined stdout/stderr capture and timing

local Job = {}
Job.__index = Job

function Job:kill()
  if self.handle and not self.exited then
    pcall(function() self.handle:kill('sigterm') end)
  end
end

local M = {}

-- Parse the "env" sentinel used by original adapters: {'env','K=V','X=Y', <exe>, ...}
function M.normalize_cmd(cmd)
  if type(cmd) == 'string' then
    return nil, { 'sh', '-c', cmd }
  end
  local env = nil
  local argv = vim.deepcopy(cmd)
  if argv[1] == 'env' then
    table.remove(argv, 1)
    env = {}
    while argv[1] and argv[1]:match('^%w+=') do
      local k, v = argv[1]:match('^(%w+)=(.*)$')
      env[k] = v
      table.remove(argv, 1)
    end
  end
  return env, argv
end

-- Run asynchronously, invoke cb(lines, code, runtime_seconds)
function M.run(cmd, stdin_text, cb)
  local env, argv = M.normalize_cmd(cmd)
  local start = (vim.uv or vim.loop).hrtime()
  local acc_out, acc_err = {}, {}

  local handle = vim.system(argv, {
    text = true,
    env = env,
    stdin = stdin_text,
    stdout = function(err, data)
      if data then acc_out[#acc_out+1] = data end
    end,
    stderr = function(err, data)
      if data then acc_err[#acc_err+1] = data end
    end,
  }, function(res)
    local ok = true
    local runtime = ((vim.uv or vim.loop).hrtime() - start) / 1e9
    local s = table.concat(acc_out) .. table.concat(acc_err)
    s = s:gsub('\r\n', '\n')
    local lines = {}
    for line in s:gmatch('([^\n]*)\n?') do table.insert(lines, line) end
    if #lines > 0 and lines[#lines] == '' then table.remove(lines) end
    cb(lines, res.code or -1, runtime)
  end)

  local j = setmetatable({ handle = handle, exited = false }, Job)
  -- mark exit on callback completion
  return j
end

return M
