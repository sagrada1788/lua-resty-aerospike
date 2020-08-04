local datatypes = require 'resty.aerospike.datatypes'
local struct = require 'struct'
local bit = require 'bit'

local insert, concat = table.insert, table.concat
local pairs, ipairs = pairs, ipairs


local Info1Flags = {
    EMPTY = 0,
    READ = 1,
    GET_ALL = 2,
    UNUSED = 3,
    BATCH_INDEX = 4,
    XDR = 5,
    DONT_GET_BIN_DATA = 6,
    READ_MODE_AP_ALL = 7,
}


local Info2Flags = {
    EMPTY = 0,
    WRITE = 1,
    DELETE = 2,
    GENERATION = 3,
    GENERATION_GT = 4,
    DURABLE_DELETE = 5,
    CREATE_ONLY = 6,
    UNUSED = 7,
    RESPOND_ALL_OPS = 8,
}


local Info3Flags = {
    EMPTY = 0,
    LAST = 1,
    COMMIT_MASTER = 2,
    UNUSED = 3,
    UPDATE_ONLY = 4,
    CREATE_OR_REPLACE = 5,
    REPLACE_ONLY = 6,
    SC_READ_TYPE = 7,
    SC_READ_RELAX = 8,
}


local FieldTypes = {
    NAMESPACE = 0 ,
    SETNAME = 1 ,
    KEY = 2 ,
    DIGEST = 4 ,
    TASK_ID = 7 ,
    SCAN_OPTIONS = 8 ,
    SCAN_TIMEOUT = 9 ,
    SCAN_RPS = 10,
    INDEX_RANGE = 22,
    INDEX_FILTER = 23,
    INDEX_LIMIT = 24,
    INDEX_ORDER = 25,
    INDEX_TYPE = 26,
    UDF_PACKAGE_NAME = 30,
    UDF_FUNCTION = 31,
    UDF_ARGLIST = 32,
    UDF_OP = 33,
    QUERY_BINS = 40,
    BATCH_INDEX = 41,
    BATCH_INDEX_WITH_SET = 42,
    PREDEXP = 43,
}


local OperationTypes = {
    READ = 1,
    WRITE = 2,
    CDT_READ = 3,
    CDT_MODIFY = 4,
    MAP_READ = 6,
    MAP_MODIFY = 7,
    INCR = 5,
    APPEND = 9,
    PREPEND = 10,
    TOUCH = 11,
    BIT_READ = 12,
    BIT_MODIFY = 13,
    DELETE = 14,
}


local Field = function(field_type, data)
    local field = {
        _fmt = '>IB',
        field_type = field_type,
        data = data,
    }
    field._fmt_size = struct.size(field._fmt)
    field.pack = function(self)
        local length = #self.data + 1
        return struct.pack(self._fmt, length, self.field_type) .. self.data
    end
    field.parse = function(self, field_data)
        local length, field_type = struct.unpack(self._fmt, field_data:sub(1, self._fmt_size))
        local data = field_data:sub(self._fmt_size+1, length)
        self.field_type = field_type
        self.data = data
    end
    field.len = function(self)
        return #self.data + self._fmt_size
    end
    return field
end


local Bin = function(name, data, version)
    local bin = {
        _fmt = 'BBB',
        name = name,
        data = datatypes.data_to_aerospike_type(data),
        version = version or 0,
    }
    bin._fmt_size = struct.size(bin._fmt)
    bin.pack = function(self)
        local base = struct.pack(self._fmt, self.data._type, self.version, #self.name)
        return base .. self.name .. self.data:pack()
    end
    bin.parse = function(self, bin_data)
        local btype, version, name_length = struct.unpack(self._fmt, bin_data:sub(1, self._fmt_size))
        local name = bin_data:sub(self._fmt_size+1, self._fmt_size+name_length)
        local data = bin_data:sub(self._fmt_size+name_length+1)
        self.name = name
        self.version = version
        self.data = datatypes.parse_raw(btype, data)
    end
    bin.len = function(self)
        return self._fmt_size + #self.name + self.data:len()
    end
    return bin
end


local Operation = function(operation_type, data_bin)
    local operation = {
        _fmt = '>IB',
        operation_type = operation_type,
        data_bin = data_bin,
    }
    operation._fmt_size = struct.size(operation._fmt)
    operation.pack = function(self)
        local packed_bin = self.data_bin:pack()
        local length = #packed_bin + 1
        return struct.pack(self._fmt, length, self.operation_type) .. packed_bin
    end
    operation.parse = function(self, op_data)
        local size, operation_type = struct.unpack(self._fmt, op_data:sub(1, self._fmt_size))
        local data_bin = Bin()
        data_bin:parse(op_data:sub(self._fmt_size+1, self._fmt_size+size-1))
        self.operation_type = operation_type
        self.data_bin = data_bin
    end
    operation.len = function(self)
        return self.data_bin:len() + self._fmt_size
    end
    return operation
end


local Message = function(message_data)
    local message = {
        _fmt = '>BBBBxBIIIHH',
        _type = 'Message',
        info1 = message_data.info1,
        info2 = message_data.info2,
        info3 = message_data.info3,
        transaction_ttl = message_data.transaction_ttl,
        fields = message_data.fields,
        operations = message_data.operations,
        result_code = message_data.result_code or 0,
        generation  = message_data.generation or 0,
        record_ttl = message_data.record_ttl or 0,
    }
    message._fmt_size = struct.size(message._fmt)
    message.pack = function(self)
        local base = struct.pack(self._fmt,
            self._fmt_size,
            self.info1,
            self.info2,
            self.info3,
            self.result_code,
            self.generation,
            self.record_ttl,
            self.transaction_ttl,
            #self.fields,
            #self.operations
        )

        local fields = {}
        for i, field in ipairs(self.fields) do
            fields[i] = field:pack()
        end
        fields = concat(fields, '')

        local operations = {}
        for i, operation in ipairs(self.operations) do
            operations[i] = operation:pack()
        end
        operations = concat(operations, '')

        return base .. fields .. operations
    end
    message.parse = function(self, message_data)
        local parsed = { struct.unpack(self._fmt, message_data:sub(1, self._fmt_size)) }

        local _size = parsed[1]
        local info1 = parsed[2]
        local info2 = parsed[3]
        local info3 = parsed[4]
        local result_code = parsed[5]
        local generation = parsed[6]
        local record_ttl = parsed[7]
        local transaction_ttl = parsed[8]
        local fields_count = tonumber(parsed[9])
        local operations_count = tonumber(parsed[10])

        local data_left = message_data:sub(self._fmt_size+1)

        local fields = {}
        for i=1,fields_count,1 do
            local f = Field()
            f:parse(data_left)
            fields[i] = f
            data_left = data_left:sub(f:len()+1)
            --data_left = message_data:sub(f:len()+1)
        end

        local operations = {}
        for i=1,operations_count,1 do
            local op = Operation()
            op:parse(data_left)
            operations[i] = op

            data_left = data_left:sub(op:len()+1)
        end
        self.info1 = info1
        self.info2 = info2
        self.info3 = info3
        self.result_code = result_code
        self.generation = generation
        self.record_ttl = record_ttl
        self.transaction_ttl = transaction_ttl
        self.fields = fields
        self.operations = operations
    end
    return message
end


local function gen_namespace_set_key_fields(namespace, set, key)
    local namespace_field = Field(FieldTypes.NAMESPACE, namespace)
    local set_field = Field(FieldTypes.SETNAME, set)

    local aero_key = datatypes.data_to_aerospike_type(key)

    local key_field = Field(FieldTypes.DIGEST, aero_key:digest(set))

    return { namespace_field, set_field, key_field }
end


local function _gen_message(_message_info, namespace, set, key, bin, ttl)
    local fields = gen_namespace_set_key_fields(namespace, set, key)

    local ops = {}
    if bin and _message_info.operation_type then
        for k, v in pairs(bin) do
            local op = Operation(_message_info.operation_type, Bin(k, v))
            insert(ops, op)
        end
    end

    return Message({
        info1 = _message_info.info1,
        info2 = _message_info.info2,
        info3 = _message_info.info3,
        transaction_ttl = 1000,
        fields = fields,
        operations = ops,
        record_ttl = ttl,
    })
end


local function put(...)
    local info = {
        info1 = Info1Flags.EMPTY,
        info2 = Info2Flags.WRITE,
        info3 = Info3Flags.EMPTY,
        operation_type = OperationTypes.WRITE,
    }
    return _gen_message(info, ...)
end


local function incr(...)
    local info = {
        info1 = Info1Flags.EMPTY,
        info2 = Info2Flags.WRITE,
        info3 = Info3Flags.EMPTY,
        operation_type = OperationTypes.INCR,
    }
    return _gen_message(info, ...)
end


-- optional param bin
local function get(namespace, set, key, bin)
    local info = {
        info1 = bin and Info1Flags.READ or bit.bor(Info1Flags.READ, Info1Flags.GET_ALL),
        info2 = Info2Flags.EMPTY,
        info3 = Info3Flags.EMPTY,
        operation_type = OperationTypes.READ,
    }
    return _gen_message(info, namespace, set, key, bin)
end


local function delete(...)
    local info = {
        info1 = Info1Flags.EMPTY,
        info2 = bit.bor(Info2Flags.DELETE, Info2Flags.WRITE),
        info3 = Info3Flags.EMPTY,
    }
    return _gen_message(info, ...)
end


return {
    Message = Message,
    -- methods
    put = put,
    incr = incr,
    get = get,
    delete = delete,
}
