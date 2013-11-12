var netlib = require("net"),
    Stream = require("stream").Stream,
    utillib = require("util");


var syslog  = console;
try {
    syslog = require('syslog-console').init('Gearman');
} catch (e) {
    // keep calm, carry on
    syslog.debug = syslog.log;
}

module.exports = Gearman;

if (typeof setImmediate == 'undefined') {
    var setImmediate = process.nextTick;
}

function Gearman(hosts, debug, persistent){
    Stream.call(this);

    this.shouldReconnect = true;
    this.line  = 0;
    this.hosts = hosts;
    this.debug = true; // debug !== undefined ? debug : false;

    if (this.debug) syslog.debug({action: 'gearman.hosts', hosts: this.hosts});
    this.init();
}
utillib.inherits(Gearman, Stream);

Gearman.prototype.init = function(){
    this.connections    = [];
    this.workers        = {};
    this.activeJobs     = {};
    this.activeWorker   = null;
    this.workersLocked  = false;
    
    for (var i in this.hosts) {
        this.connections.push(new this.Connection(this, this.hosts[i].HOST, this.hosts[i].PORT));
    }
};

Gearman.packetTypes = {
    CAN_DO: 1,
    CANT_DO: 2,
    RESET_ABILITIES: 3,
    PRE_SLEEP: 4,
    NOOP: 6,
    SUBMIT_JOB: 7,
    JOB_CREATED: 8,
    GRAB_JOB: 9,
    NO_JOB: 10,
    JOB_ASSIGN: 11,
    WORK_STATUS: 12,
    WORK_COMPLETE: 13,
    WORK_FAIL: 14,
    GET_STATUS: 15,
    ECHO_REQ: 16,
    ECHO_RES: 17,
    SUBMIT_JOB_BG: 18,
    ERROR: 19,
    STATUS_RES: 20,
    SUBMIT_JOB_HIGH: 21,
    SET_CLIENT_ID: 22,
    CAN_DO_TIMEOUT: 23,
    ALL_YOURS: 24,
    WORK_EXCEPTION: 25,
    OPTION_REQ: 26,
    OPTION_RES: 27,
    WORK_DATA: 28,
    WORK_WARNING: 29,
    GRAB_JOB_UNIQ: 30,
    JOB_ASSIGN_UNIQ: 31,
    SUBMIT_JOB_HIGH_BG: 32,
    SUBMIT_JOB_LOW: 33,
    SUBMIT_JOB_LOW_BG: 34,
    SUBMIT_JOB_SCHED: 35,
    SUBMIT_JOB_EPOCH: 36
};

Gearman.packetTypesReversed = {
    "1": "CAN_DO",
    "2": "CANT_DO",
    "3": "RESET_ABILITIES",
    "4": "PRE_SLEEP",
    "6": "NOOP",
    "7": "SUBMIT_JOB",
    "8": "JOB_CREATED",
    "9": "GRAB_JOB",
    "10": "NO_JOB",
    "11": "JOB_ASSIGN",
    "12": "WORK_STATUS",
    "13": "WORK_COMPLETE",
    "14": "WORK_FAIL",
    "15": "GET_STATUS",
    "16": "ECHO_REQ",
    "17": "ECHO_RES",
    "18": "SUBMIT_JOB_BG",
    "19": "ERROR",
    "20": "STATUS_RES",
    "21": "SUBMIT_JOB_HIGH",
    "22": "SET_CLIENT_ID",
    "23": "CAN_DO_TIMEOUT",
    "24": "ALL_YOURS",
    "25": "WORK_EXCEPTION",
    "26": "OPTION_REQ",
    "27": "OPTION_RES",
    "28": "WORK_DATA",
    "29": "WORK_WARNING",
    "30": "GRAB_JOB_UNIQ",
    "31": "JOB_ASSIGN_UNIQ",
    "32": "SUBMIT_JOB_HIGH_BG",
    "33": "SUBMIT_JOB_LOW",
    "34": "SUBMIT_JOB_LOW_BG",
    "35": "SUBMIT_JOB_SCHED",
    "36": "SUBMIT_JOB_EPOCH"
};

Gearman.paramCount = {
    ERROR:          ["string", "string"],
    JOB_ASSIGN:     ["string", "string", "buffer"],
    JOB_ASSIGN_UNIQ:["string", "string", "string", "buffer"],
    JOB_CREATED:    ["string"],
    WORK_COMPLETE:  ["string", "buffer"],
    WORK_EXCEPTION: ["string", "string"],
    WORK_WARNING:   ["string", "string"],
    WORK_DATA:      ["string", "buffer"],
    WORK_FAIL:      ["string"],
    WORK_STATUS:    ["string", "number", "number"]
};

Gearman.prototype.connect = function(){
    for (var i in this.connections) {
        this.connections[i].connect();
    }
};

Gearman.prototype.submitJob = function(name, payload, uniq, options){
    return new this.Job(this, name, payload, uniq, (typeof options != "object" ? {} : options));
};

Gearman.prototype.getStatus = function(handle){
    if (this.debug) syslog.debug({action: 'gearman.status', handle: handle});

    this.broadcastCommand("GET_STATUS", handle);
};

Gearman.prototype.broadcastCommand = function() {
    for (var i in this.connections) {
        this.connections[i].sendCommand.apply(this.connections[i], arguments);
    }
};

Gearman.prototype.sendCommandToNextInLine = function() {
    var iConnection = this.line % this.connections.length;
    var oConnection = this.connections[iConnection];
    oConnection.sendCommand.apply(oConnection, arguments);
    this.line++;
};

Gearman.prototype.close = function(){
    this.shouldReconnect = false;
    this._closeServers();
    this._clearJobs();
    this._clearWorkers();
    this.init();
    this.emit("close");
};

Gearman.prototype._closeServers = function(){
    for (var i in this.connections) {
        this.connections[i].close();
    }
};

Gearman.prototype._clearJobs = function(){
    for(var i in this.activeJobs){
        if(this.activeJobs[i] !== undefined){
            this.activeJobs[i].abort('Job Aborted');
        }

        delete this.activeJobs[i];
    }
};

Gearman.prototype._clearWorkers = function(){
    if(this.activeWorker !== null) {
        this.activeWorker.worker.finish();
        this.activeWorker = null;
    }
};

Gearman.prototype.unlockWorkers = function() {
    if (this.debug) syslog.debug({action: 'gearman.unlockWorkers'});
    this.workersLocked = false;
};

Gearman.prototype.lockWorkers = function() {
    if (this.debug) syslog.debug({action: 'gearman.lockWorkers'});
    this.workersLocked = true;
};

Gearman.prototype.lockedWorkers = function() {
    return this.workersLocked;
};

Gearman.prototype.registerWorker = function(name, method) {
    this.workers[name] = method;
};

Gearman.prototype._workerAssign = function(connection, handle, name, unique, payload) {
    if (typeof this.workers[name] == 'function') {
        if (this.debug) syslog.debug({action: 'gearman._workerAssign', handle: handle, name: name});

        this.lockWorkers();
        this.activeWorker = {
            handle: handle,
            worker: new this.Worker(this, connection, handle, name, unique, payload)
        };

        this.workers[name](payload, this.activeWorker.worker);
    }
};

Gearman.prototype._workerRemove = function(handle) {
    if (this.debug) syslog.debug({action: 'gearman._workerRemove', handle: handle});
    if (this.activeWorker !== null) {
        if (this.activeWorker.handle == handle) {
            if (this.debug) syslog.debug({action: 'gearman._workerRemoved', handle: handle});
            this.unlockWorkers();
            this.activeWorker = null;
            this.broadcastCommand("PRE_SLEEP");
        }
    }
};

Gearman.prototype._jobAssign = function(handle, job) {
    this.activeJobs[handle] = job;
};

Gearman.prototype._jobRemove = function(handle) {
    if (this.activeJobs[handle] !== undefined) {
        delete this.activeJobs[handle];
    }
};

Gearman.prototype._jobFail = function(handle) {
    var job = this.activeJobs[handle];
    if(job !== undefined) {
        this._jobRemove(handle);
        job.fail();
    }
};

Gearman.prototype._jobData = function(handle, payload){
    var job = this.activeJobs[handle];
    if(job !== undefined) {
        job.data(payload);
    }
};

Gearman.prototype._jobStatus = function(handle, numerator, denominator){
    var job = this.activeJobs[handle];
    if(job !== undefined) {
        job.status(numerator, denominator);
    }
};

Gearman.prototype._jobComplete = function(handle, payload){
    var job = this.activeJobs[handle];
    if(job !== undefined) {
        this._jobRemove(handle);
        job.complete(payload)
    }
};

// SERVER

Gearman.prototype.Connection = function(gearman, host, port){
    this.gearman    = gearman;
    this.host       = host;
    this.port       = port;
    this.connected  = false;
    this.socket     = false;
    this.commands   = [];
    this.callbacks  = [];
    this.processing = false;
};

Gearman.prototype.Connection.prototype.connect = function() {
    if (this.socket) return;

    if (this.gearman.debug) syslog.debug({action: 'gearman.connection.connect', host: this.host});

    this.socket = (netlib.connect || netlib.createConnection)(this.port, this.host);
    this.socket.on('connect',   this._connected.bind(this));
    this.socket.on('end',       this._ended.bind(this));
    this.socket.on('close',     this._closed.bind(this));
    this.socket.on('error',     this._error.bind(this));
    this.socket.on('data',      this._receive.bind(this));
};

Gearman.prototype.Connection.prototype.grabJob = function() {
    if (!this.gearman.lockedWorkers()) {
        if (this.gearman.debug) syslog.debug({action: 'gearman.connection.grabJob'});
        this.gearman.lockWorkers();
        this.sendCommand("GRAB_JOB_UNIQ");
    } else {
        if (this.gearman.debug) syslog.debug({action: 'gearman.connection.grabJob.busy'});
    }
};

Gearman.prototype.Connection.prototype.sendCommand = function(){
    var command = Array.prototype.slice.call(arguments);
    this.commands.push(command);

    if(!this.processing){
        this._processCommandQueue();
    }
};

Gearman.prototype.Connection.prototype.close = function() {
    if (this.gearman.debug) syslog.debug({action: 'gearman.connection.close', host: this.host});

    try {
        if (this.socket) {
            this.socket.on('close', function() {
                // Make sure we're not reconnecting
                if (this.gearman.debug) syslog.debug({action: 'gearman.connection.close.event', host: this.host});
            }.bind(this));

            this.socket.end();
        }
    } catch(e) {
        if (this.gearman.debug) syslog.error({action: 'gearman.connection.close', host: this.host, error: e});
    }
};

Gearman.prototype.Connection.prototype._connected = function() {
    this.connected = true;
    this.socket.setKeepAlive(true);

    if (this.gearman.debug) syslog.debug({action: 'gearman.connection.connected', host: this.host});

    this.gearman.emit('connect');

    this._registerWorkers();
    this._processCommandQueue();
};

Gearman.prototype.Connection.prototype._registerWorkers = function () {
    for (var name in this.gearman.workers) {
        this.sendCommand("CAN_DO", name);
    }

    this.sendCommand("PRE_SLEEP");
};

Gearman.prototype.Connection.prototype._ended = function() {
    if (this.gearman.debug) syslog.debug({action: 'gearman.connection._ended', host: this.host});
    
    this.connected  = false;
    this.socket     = false;
    this._reconnect();
};

Gearman.prototype.Connection.prototype._closed = function() {
    if (this.gearman.debug) syslog.debug({action: 'gearman.connection._closed', host: this.host});

    this.connected  = false;
    this.socket     = false;
    this._reconnect();
};

Gearman.prototype.Connection.prototype._error = function(err){
    if (this.gearman.debug) syslog.error({action: 'gearman.connection._error', host: this.host, error: err});

    this.socket.end();
    this.socket = false;
    this._reconnect();
};

Gearman.prototype.Connection.prototype._reconnect = function() {
    if (this.gearman.debug) syslog.debug({action: 'gearman.connection._reconnect', host: this.host});

    if (this.gearman.shouldReconnect) {
        setTimeout(this.connect.bind(this), 5000);
    } else {
        if (this.gearman.debug) syslog.debug({action: 'gearman.connection._reconnect.no'});
    }
};

Gearman.prototype.Connection.prototype._processCommandQueue = function(chunk){
    var command;
    if(this.commands.length){
        this.processing = true;
        command = this.commands.shift();
        this._sendCommandToServer.apply(this, command);
    } else {
        this.processing = false;
        this.gearman.emit('sent');
    }
};

Gearman.prototype.Connection.prototype._sendCommandToServer = function(){
    var body,
        args = Array.prototype.slice.call(arguments),
        commandName, commandId, commandCallback,
        i, len, bodyLength = 0, curpos = 12;

    if(!this.connected){
        this.commands.unshift(args);
        return this.connect();
    }

    commandName = (args.shift() || "").trim().toUpperCase();

    if(args.length && typeof args[args.length-1] == "function"){
        commandCallback = args.pop();
        this.callbacks.push(commandCallback);
    }

    commandId = Gearman.packetTypes[commandName] || 0;

    if(!commandId){
        // TODO: INVALID COMMAND!
    }

    for(i=0, len=args.length; i<len; i++){
        if(!(args[i] instanceof Buffer)){
            args[i] = new Buffer((args[i] || "").toString(), "utf-8");
        }
        bodyLength += args[i].length;
    }

    bodyLength += args.length>1 ? args.length - 1 : 0;

    body = new Buffer(bodyLength + 12); // packet size + 12 byte header

    body.writeUInt32BE(0x00524551, 0); // \0REQ
    body.writeUInt32BE(commandId, 4); // \0REQ
    body.writeUInt32BE(bodyLength, 8); // \0REQ

    // compose body
    for(i=0, len = args.length; i<len; i++){
        args[i].copy(body, curpos);
        curpos += args[i].length;
        if(i < args.length-1){
            body[curpos++] = 0x00;
        }
    }

    if(this.gearman.debug) syslog.debug({action: 'gearman.connection._sendCommandToServer', command: commandName, host: this.host, args: args.toString('utf8'), body: body.toString('utf8')});

    this.socket.write(body, this._processCommandQueue.bind(this));

};

Gearman.prototype.Connection.prototype._receive = function(chunk){
    var data = new Buffer((chunk && chunk.length || 0) + (this.remainder && this.remainder.length || 0)),
        commandId, commandName,
        bodyLength = 0, args = [], argTypes, curarg, i, len, argpos, curpos;

    // nothing to do here
    if(!data.length){
        return;
    }

    // if theres a remainder value, tie it together with the incoming chunk
    if(this.remainder){
        this.remainder.copy(data, 0, 0);
        if(chunk){
            chunk.copy(data, this.remainder.length, 0);
        }
    }else{
        data = chunk;
    }

    // response header needs to be at least 12 bytes
    // otherwise keep the current chunk as remainder
    if(data.length<12){
        this.remainder = data;
        return;
    }

    if(data.readUInt32BE(0) != 0x00524553){
        // OUT OF SYNC!
        return this._error(new Error("Out of sync with connection"));
    }

    // response needs to be 12 bytes + payload length
    bodyLength = data.readUInt32BE(8);
    if(data.length < 12 + bodyLength){
        this.remainder = data;
        return;
    }

    // keep the remainder if incoming data is larger than needed
    if(data.length > 12 + bodyLength){
        this.remainder = data.slice(12 + bodyLength);
        data = data.slice(0, 12 + bodyLength);
    }else{
        this.remainder = false;
    }

    commandId = data.readUInt32BE(4);
    commandName = (Gearman.packetTypesReversed[commandId] || "");
    if(!commandName){
        // TODO: UNKNOWN COMMAND!
        return;
    }

    if(bodyLength && (argTypes = Gearman.paramCount[commandName])){
        curpos = 12;
        argpos = 12;

        for(i = 0, len = argTypes.length; i < len; i++){

            if(i < len - 1){
                while(data[curpos] !== 0x00 && curpos < data.length){
                    curpos++;
                }
                curarg = data.slice(argpos, curpos);
            }else{
                curarg = data.slice(argpos);
            }

            switch(argTypes[i]){
                case "string":
                    curarg = curarg.toString("utf-8");
                    break;
                case "number":
                    curarg = Number(curarg.toString()) || 0;
                    break;
            }

            args.push(curarg);

            curpos++;
            argpos = curpos;
            if(curpos >= data.length){
                break;
            }
        }
    }

    if(this.gearman.debug) syslog.debug({action: 'gearman.connection._receive', command: commandName, args: args});

    // Run command
    if(typeof this["receive_" + commandName] == "function"){
        if(commandName == "JOB_CREATED" && this.callbacks.length){
            args = args.concat(this.callbacks.shift());
        }

        this["receive_" + commandName].apply(this, args);
    }

    // rerun receive just in case there's enough data for another command
    if(this.remainder && this.remainder.length>=12){
        setImmediate(this._receive.bind(this));
    }
};

Gearman.prototype.Connection.prototype.receive_NO_JOB = function(){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_no_job', host: this.host});

    this.gearman.unlockWorkers();
    this.sendCommand("PRE_SLEEP");
    this.gearman.emit("idle");
};

Gearman.prototype.Connection.prototype.receive_NOOP = function(){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_noop', host: this.host});
    this.grabJob();
};

Gearman.prototype.Connection.prototype.receive_ECHO_REQ = function(payload){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_echo_req', host: this.host, payload: payload});
    this.sendCommand("ECHO_RES", payload);
};

Gearman.prototype.Connection.prototype.receive_ERROR = function(code, message){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_error', host: this.host, code: code, message: message});
};

Gearman.prototype.Connection.prototype.receive_JOB_CREATED = function(handle, callback){
    if (this.gearman.debug) syslog.debug({action: 'gearman.receive_job_created', host: this.host, host: this.host, handle: handle});

    if(typeof callback == "function"){
        callback(handle);
    }
};

Gearman.prototype.Connection.prototype.receive_WORK_FAIL = function(handle){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_work_fail', host: this.host, handle: handle});

    this.gearman._jobFail(handle);
};

Gearman.prototype.Connection.prototype.receive_WORK_DATA = function(handle, payload){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_work_data', host: this.host, handle: handle, payload: payload});

    this.gearman._jobData(handle, payload);
};

Gearman.prototype.Connection.prototype.receive_WORK_STATUS = function(handle, numerator, denominator){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_work_status', host: this.host, handle: handle, numerator: numerator, denominator: denominator});

    this.gearman._jobStatus(handle, numerator, denominator);
};

Gearman.prototype.Connection.prototype.receive_WORK_COMPLETE = function(handle, payload){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_work_complete', host: this.host, handle: handle});

    this.gearman._jobComplete(handle, payload);
};

Gearman.prototype.Connection.prototype.receive_JOB_ASSIGN_UNIQ = function(handle, name, unique, payload){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_job_assign_uniq', host: this.host, handle: handle, name: name, unique: unique});

    this.gearman._workerAssign(this, handle, name, unique, payload);
};

Gearman.prototype.Connection.prototype.receive_JOB_ASSIGN = function(handle, name, payload){
    if(this.gearman.debug) syslog.debug({action: 'gearman.connection.receive_job_assign', host: this.host, handle: handle, name: name});
    this.receive_JOB_ASSIGN_UNIQ(handle, name, null, payload);
};



// WORKER

Gearman.prototype.Worker = function(gearman, connection, handle, name, unique, payload){
    Stream.call(this);

    this.gearman    = gearman;
    this.connection = connection;
    this.handle     = handle;
    this.name       = name;
    this.unique     = unique;
    this.payload    = payload;
    this.finished   = false;
    this.writable   = true;
};
utillib.inherits(Gearman.prototype.Worker, Stream);


Gearman.prototype.Worker.prototype.finish = function(){
    if(this.finished){
        return;
    }

    this.finished = true;
};

Gearman.prototype.Worker.prototype.write = function(data){
    if(this.finished){
        return;
    }

    this.connection.sendCommand("WORK_DATA", this.handle, data);
};

Gearman.prototype.Worker.prototype.status = function(numerator, denominator){
    if(this.finished){
        return;
    }

    this.connection.sendCommand("WORK_STATUS", this.handle, numerator, denominator);
};

Gearman.prototype.Worker.prototype.end = function(data, callback){
    callback = (callback !== undefined && typeof callback == 'function') ? callback : function() {};

    this.finish();
    this.connection.sendCommand("WORK_COMPLETE", this.handle, data);
    this.gearman.on('sent', function() {
        this.gearman._workerRemove(this.handle);
        callback();
    }.bind(this));
};

Gearman.prototype.Worker.prototype.error = function(error, callback){
    callback = (callback !== undefined && typeof callback == 'function') ? callback : function() {};

    this.finish();
    this.connection.sendCommand("WORK_FAIL", this.handle, error);
    this.gearman.on('sent', function() {
        this.gearman._workerRemove(this.handle);
        callback();
    }.bind(this));
};

Gearman.prototype.Worker.prototype.endAndGrabJob = function(data){
    this.end(data, function() {
        this.connection.grabJob();
    }.bind(this));
};

Gearman.prototype.Worker.prototype.errorAndGrabJob = function(error){
    this.error(error, function() {
        this.connection.grabJob();
    }.bind(this));
};

// JOB
Gearman.prototype.Job = function(gearman, name, payload, uniq, options){
    Stream.call(this);

    this.gearman    = gearman;
    this.name       = name;
    this.payload    = payload;
    this.unique     = uniq;

    this.timeoutTimer = null;

    var jobType = "SUBMIT_JOB";
    if (typeof options == "object") {
        if (typeof options.priority == "string" &&
            ['high', 'low'].indexOf(options.priority) != -1) {
            jobType += "_" + options.priority.toUpperCase();
        }
    
        if (typeof options.background == "boolean" && options.background)
            jobType += "_BG";
    }

    this.gearman.sendCommandToNextInLine(jobType, name, uniq ? uniq : false, payload, !options.background ? this.receiveHandle.bind(this) : false);
};

utillib.inherits(Gearman.prototype.Job, Stream);

Gearman.prototype.Job.prototype.setTimeout = function(timeout, timeoutCallback){
    this.timeoutValue = timeout;
    this.timeoutCallback = timeoutCallback;
    this.updateTimeout();
};

Gearman.prototype.Job.prototype.updateTimeout = function(){
    if(this.timeoutValue){
        clearTimeout(this.timeoutTimer);
        this.timeoutTimer = setTimeout(this.onTimeout.bind(this), this.timeoutValue);
    }
};

Gearman.prototype.Job.prototype.onTimeout = function(){
    if(this.handle){
        this.gearman._jobRemove(this.handle);
    }

    if(!this.aborted){
        this.abort();

        var error = new Error("Timeout exceeded for the job");
        if(typeof this.timeoutCallback == "function"){
            this.timeoutCallback(error);
        }else{
            this.emit("timeout", error);
        }
    }
};

Gearman.prototype.Job.prototype.data = function(data) {
    if(!this.aborted){
        this.emit("data", payload);
        this.updateTimeout();
    }
};

Gearman.prototype.Job.prototype.status = function(numerator, denominator) {
    if(!this.aborted){
        this.emit("status", numerator, denominator);
        this.updateTimeout();
    }
};

Gearman.prototype.Job.prototype.fail = function() {
    if(!this.aborted){
        this.abort("Job Failed");
    }
};

Gearman.prototype.Job.prototype.abort = function(sError){
    clearTimeout(this.timeoutTimer);
    this.aborted = true;

    if (sError) {
        this.emit("error", new Error(sError));
    }
};

Gearman.prototype.Job.prototype.complete = function(payload){
    clearTimeout(this.timeoutTimer);

    if(payload){
        this.emit("data", payload);
    }

    this.emit("end");
};

Gearman.prototype.Job.prototype.receiveHandle = function(handle){
    if(handle){
        this.handle = handle;
        this.gearman._jobAssign(handle, this);
        this.emit("created", handle);
    }else{
        this.emit("error", new Error("Invalid response from connection"));
    }
};
