local bit = require 'bit'
local struct = require 'struct'
local admin = require 'resty.aerospike.admin'
local message = require 'resty.aerospike.message'
local byte, char, format = string.byte, string.char, string.format
local insert, concat = table.insert, table.concat
local tonumber = tonumber


local _M = {}


local MessageType = {
    INFO = 1,
    ADMIN = 2,
    MESSAGE = 3,
    COMPRESSED = 4,
}


local MESSAGE_CLASS_TO_TYPE = {
    AdminMessage = MessageType.ADMIN,
    Message = MessageType.MESSAGE,
}


local MESSAGE_TYPE_TO_CLASS = {
    [MessageType.ADMIN] = admin.AdminMessage,
    [MessageType.MESSAGE] = message.Message,
}


_M.AerospikeHeader = function(message_type, length)
    local as_header = {
        version = 2,
        message_type = message_type,
        length = length,
    }
    as_header.pack = function(self)
        local b_version = char(self.version)
        local b_message_type = char(self.message_type)
        local b_length = {}
        local len16 = format('%012x', self.length)
        for i=1,12,2 do
            insert(b_length, char(bit.tobit('0x'..len16:sub(i,i+1))))
        end
        b_length = concat(b_length, '')
        return b_version .. b_message_type .. b_length
    end
    as_header.parse = function(self, header_data)
        local version = header_data:sub(1,1)
        local message_type = header_data:sub(2,2)
        local length = {}
        for i=3,8,1 do
            local s = byte(header_data:sub(i,i))
            insert(length, s)
        end
        length = tonumber(concat(length, ''))
        self.version = byte(version)
        self.message_type = byte(message_type)
        self.length = length
    end
    return as_header
end


_M.AerospikeMessage = function(_message)
    local as_message = {
        message = _message,
    }
    as_message.pack = function(self)
        local packed_message = self.message:pack()
        local message_type = MESSAGE_CLASS_TO_TYPE[self.message._type]
        local length = #packed_message
        local header = _M.AerospikeHeader(message_type, length)
        return header:pack() .. packed_message
    end
    as_message.parse = function(self, header, message_data)
        local as_message = MESSAGE_TYPE_TO_CLASS[header.message_type]({})
        as_message:parse(message_data)
        self.message = as_message
    end
    return as_message
end


return _M
