local message = require 'resty.aerospike.message'
local general = require 'resty.aerospike.general'
local tcp = ngx.socket.tcp
local setmetatable, rawget, ipairs = setmetatable, rawget, ipairs

local _M = {}

_M._VERSION = '0.1'

local common_cmds = {
    'put', 'incr', 'get', 'delete',
}

local mt = { __index = _M }


function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ _sock = sock }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M.close(self)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


function _M.connect(self, ...)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end
    return sock:connect(...)
end


function _M.set_keepalive(self, ...)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end
    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


local function _read_reply(self, sock)
    local header_data, err = sock:receive(8)
    if not header_data then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    local header = general.AerospikeHeader()
    header:parse(header_data)

    local message_data, err = sock:receive(header.length)
    if not message_data then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    local as_message = general.AerospikeMessage()
    as_message:parse(header, message_data)

    return as_message
end


local function _do_cmd(self, cmd, ...)
    local sock = rawget(self, "_sock")
    if not sock then
        return nil, "not initialized"
    end

    local req_message = message[cmd](...)
    local req = general.AerospikeMessage(req_message):pack()

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local as_message = _read_reply(self, sock)

    local status = as_message.message.result_code

    if status == 2 then
        -- key not exists
        return nil ,nil
    end

    if status ~= 0 then
        return nil, 'Unexpected result code ' .. status
    end

    local bins = {}
    for _, op in ipairs(as_message.message.operations or {}) do
        local _k = op.data_bin.name
        local _v = op.data_bin.data.value
        bins[_k] = _v
    end
    return bins, nil
end


for _, cmd in ipairs(common_cmds) do
    _M[cmd] = function(self, ...)
        return _do_cmd(self, cmd, ...)
    end
end


return _M
