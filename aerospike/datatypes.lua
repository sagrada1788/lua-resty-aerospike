local bit = require 'bit'
-- luarocks install moses
local moses = require 'moses'
-- luarocks install struct
local struct = require 'struct'
-- luarocks install lmd5
local ripemd160 = require 'ripemd160'
-- luarocks install lua-cmsgpack
local cmsgpack = require 'cmsgpack'

local byte, char, format = string.byte, string.char, string.format
local insert, concat = table.insert, table.concat
local pairs, ipairs, type = pairs, ipairs, type

local _M = {}

local AerospikeTypes = {
    UNDEF = 0,
    INTEGER = 1,
    DOUBLE = 2,
    STRING = 3,
    BLOB = 4,
    JAVA = 7,
    CSHARP = 8,
    PYTHON = 9,
    RUBY = 10,
    PHP = 11,
    ERLANG = 12,
    TMAP = 19,
    TLIST = 20,
    LDT = 21,
    GEOJSON = 23,
}

local TYPE_TO_AEROSPIKE = {
    --[[
    bytes: AerospikeTypes.BLOB,
    ]]
    ['string'] = AerospikeTypes.STRING,
    ['int'] = AerospikeTypes.INTEGER,
    ['float'] = AerospikeTypes.DOUBLE,
    ['list'] = AerospikeTypes.TLIST,
    ['dict'] = AerospikeTypes.TMAP,
    ['nil'] = AerospikeTypes.UNDEF,
    -- ngx.null
    ['userdata'] = AerospikeTypes.UNDEF,
}


local function digest(self, set_name)
    local ripe = ripemd160.new()
    ripe:update(set_name)
    ripe:update(struct.pack('>B', self._type) .. self.value)
    return ripe:digest('raw')
end


_M.AerospikeInteger = function(value)
    local aerospike_integer = {
        -- struct lib not supports 'Q'
        _fmt = '>Q',
        _type = AerospikeTypes.INTEGER,
        value = value,
    }
    aerospike_integer._fmt_size = 8
    aerospike_integer.pack = function(self)
        local v16 = format('%016x', self.value)
        local result = {}
        for i=1,16,2 do
            insert(result, char(bit.tobit('0x'..v16:sub(i,i+1))))
        end
        result = concat(result, '')
        return result
    end
    aerospike_integer.parse = function(self, data)
        local parsed = 0
        for i=1,8,1 do
            parsed = parsed * 256 + byte(data:sub(i,i))
        end
        self.value = parsed
    end
    aerospike_integer.len = function(self)
        return self._fmt_size
    end
    aerospike_integer.digest = digest
    return aerospike_integer
end


_M.AerospikeDouble = function(value)
    local aerospike_double = {
        _fmt = '>d',
        _type = AerospikeTypes.DOUBLE,
        value = value,
    }
    aerospike_double._fmt_size = struct.size(aerospike_double._fmt)
    aerospike_double.pack = function(self)
        return struct.pack(self._fmt, self.value)
    end
    aerospike_double.parse = function(self, data)
        local value = struct.unpack(self._fmt, data)
        self.value = value
    end
    aerospike_double.len = function(self)
        return self._fmt_size
    end
    aerospike_double.digest = digest
    return aerospike_double
end


_M.AerospikeString = function(value)
    local aerospike_string = {
        _type = AerospikeTypes.STRING,
        value = value,
    }
    aerospike_string.pack = function(self)
        return self.value
    end
    aerospike_string.parse = function(self, data)
        self.value = data
    end
    aerospike_string.len = function(self)
        return #self.value
    end
    aerospike_string.digest = digest
    return aerospike_string
end


_M.AerospikeNone = function(value)
    local aerospike_none = {
        _type = AerospikeTypes.UNDEF,
        value = value,
    }
    aerospike_none.pack = function(self)
        return ''
    end
    aerospike_none.parse = function(self, data)
        return nil
    end
    aerospike_none.len = function(self)
        return 0
    end
    return aerospike_none
end


_M.AerospikeList = function(value, size)
    local aerospike_list = {
        _type = AerospikeTypes.TLIST,
        _size = size or 0,
        value = value,
    }
    aerospike_list.pack = function(self)
        local as_list = {}
        for i, v in ipairs(self.value) do
            as_list[i] = _M.pack_native(v)
        end
        local data = cmsgpack.pack(as_list)
        self._size = #data
        return data
    end
    aerospike_list.parse = function(self, data)
        local raw_values = cmsgpack.unpack(data)
        local parsed_values = {}
        for i, v in ipairs(raw_values) do
            parsed_values[i] = _M.unpack_aerospike(v)
        end
        self.value = parsed_values
        self._size = #data
    end
    aerospike_list.len = function(self)
        if self._size then
            return self._size
        end
        return #(self:pack())
    end
    return aerospike_list
end


_M.AerospikeMap = function(value, size)
    local aerospike_map = {
        _type = AerospikeTypes.TMAP,
        _size = size or 0,
        value = value,
    }
    aerospike_map.pack = function(self)
        local as_dict = {}
        for k, v in pairs(self.value) do
            local packed_k = _M.pack_native(k)
            local packed_v = _M.pack_native(v)
            as_dict[packed_k] = packed_v
        end
        local data = cmsgpack.pack(as_dict)
        self._size = #data
        return data
    end
    aerospike_map.parse = function(self, data)
        local raw_values = cmsgpack.unpack(data)
        local parsed_values = {}
        for k, v in pairs(raw_values) do
            local _k = _M.unpack_aerospike(k)
            local _v = _M.unpack_aerospike(v)
            parsed_values[_k] = _v
        end
        self.value = parsed_values
        self._size = #data
    end
    aerospike_map.len = function(self)
        if self._size then
            return self._size
        end
        return #(self:pack())
    end
    return aerospike_map
end


function _M.parse_raw(aerotype, data)
    local t
    if aerotype == nil then
        t = _M.AEROTYPE_TO_DATA[AerospikeTypes.UNDEF]()
    else
        t = _M.AEROTYPE_TO_DATA[aerotype]()
    end
    t:parse(data)
    return t
end


function _M.data_to_aerospike_type(data)
    local aerotype
    local data_type = type(data)
    if TYPE_TO_AEROSPIKE[data_type] then
        -- string, nil, ngx.null
        aerotype = TYPE_TO_AEROSPIKE[data_type]
    else
        -- number, table
        if data_type == 'number' then
            if moses.isInteger(data) then
                aerotype = TYPE_TO_AEROSPIKE['int']
            else
                aerotype = TYPE_TO_AEROSPIKE['float']
            end
        else
            if moses.isArray(data) then
                -- list
                aerotype = TYPE_TO_AEROSPIKE['list']
            elseif moses.isTable(data) then
                -- dict
                aerotype = TYPE_TO_AEROSPIKE['dict']
            end
        end
    end
    return _M.AEROTYPE_TO_DATA[aerotype](data)
end


function _M.pack_native(data)
    local type_instance = _M.data_to_aerospike_type(data)
    return struct.pack('>B', type_instance._type) .. type_instance:pack()
end


function _M.unpack_aerospike(data)
    local atype = byte(data:sub(1,1))
    local parsed = _M.parse_raw(atype, data:sub(2))
    return parsed.value
end


_M.AEROTYPE_TO_DATA = {
    [AerospikeTypes.STRING] = _M.AerospikeString,
    [AerospikeTypes.INTEGER] = _M.AerospikeInteger,
    [AerospikeTypes.DOUBLE] = _M.AerospikeDouble,
    [AerospikeTypes.TLIST] = _M.AerospikeList,
    [AerospikeTypes.TMAP] = _M.AerospikeMap,
    [AerospikeTypes.UNDEF] = _M.AerospikeNone,
}


return _M
