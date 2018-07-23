// lex.li on 2018.7.18

lock_time_out = 6 * 1000;

function MysqlLock(options){
    this.options = options
    this.pool = options['pool'];
    this.lock_key_value = null;
    this.notify = null;
    this.locked = false;
}

MysqlLock.prototype.aquire = function aquire(cb){
    this.notify = cb;
    this.connect()
    .then(this.obtain_lock_record.bind(this))
    .then(this.do_lock_record.bind(this))
    .then(this.do_commit.bind(this))
    .then(this.finish.bind(this))
    .catch(this.error.bind(this));
}

MysqlLock.prototype.release = function release(){
    if(!this.locked){
        return
    }
    this.connect()
    .then(this.obtain_lock_record.bind(this))
    .then(this.do_release_record.bind(this))
    .then(this.do_commit.bind(this))
    .then(this.finish.bind(this))
    .catch(this.error.bind(this));
}

MysqlLock.prototype.error = function error(error){
    let connection = error['connection'];
    let err = error['err'];
    if(connection){
        connection.rollback();
        this.pool.releaseConnection(connection);
    }    
    if(!this.locked){
        this.notify(err, null);
    }
}

MysqlLock.prototype.finish = function finish(connection){
    this.pool.releaseConnection(connection);
    if(!this.locked){
        this.locked = true;
        this.notify(null, 'locked');
    }
}

MysqlLock.prototype.do_commit = function do_commit(connection){
    return new Promise(function(resolve, reject){
        connection.commit(function(err){
            if(err){
                reject({connection: connection, err:err});
            }else{
                resolve(connection);
            }
        });
    });
}

MysqlLock.prototype.connect = function connect(){
    let lock = this;
    return new Promise(function(resolve, reject){
        lock.pool.getConnection(function(err, connection){
            if(err){
                reject({connection: null, err: err});
            }else{
                resolve(connection);
            }
        });
    });
}

MysqlLock.prototype.obtain_lock_record = function obtain_lock_record(connection){
    let lock = this;
    return new Promise(function(resolve, reject){
        connection.beginTransaction(function(err){
            if(err){
                return reject({connection: connection, err: err});
            }
            let query = lock.gen_lock_query();
            connection.query(query['sql'], query['values'], function(err, results, fields){
                if(err){
                    return reject({connection: connection, err:err});
                }
                resolve({connection: connection, records: results});
            });
        });
    });
}

MysqlLock.prototype.do_lock_record = function do_lock_record(data){
    let connection = data['connection'];
    let records = data['records'];
    let record = records.length > 0? records[0]: null;
    let lock = this;
    return new Promise(function(resolve, reject){
        let do_insert = 0;
        if(record){
            let conditions = lock.options['conditions'];
            let locked = false;
            for(let i=0; i<conditions.length; i++){
                let c = conditions[i];
                let v = lock.string_value(record[c[0]]);
                let v2 = lock.string_value(c[2]);
                let exp = v + c[1] + v2;
                let b = eval(exp);
                // 仅支持 or
                if(b){
                    locked = b;
                    break;
                }
            }
            if(!locked){
                return reject({connection: connection, err:'had locked'});
            }
        }else{
            do_insert = 1;
        }
        let update_fields = lock.eval_field_datas(lock.options['update_fields']);
        let lock_key = lock.options['lock_key'];
        for(let i=0; i<update_fields.length; i++){
            let e = update_fields[i];
            if(lock_key == e[0]){
                lock.lock_key_value = e[1];
                break;
            }
        }
        let query = lock.gen_lock_insert_or_update(update_fields, do_insert);
        connection.query(query['sql'], query['values'], function(err, results, fields){
            if(err){
                reject({connection: connection, err:err});
            }else{
                resolve(connection);
            }                
        });
    });
}

MysqlLock.prototype.do_release_record = function do_release_record(data){
    let connection = data['connection'];
    let records = data['records'];
    let lock = this;
    let record = records.length > 0? records[0]: null;
    return new Promise(function(resolve, reject){
        if(record){
            let lock_key = lock.options['lock_key'];
            let v1 = lock.string_value(lock.lock_key_value);
            let v2 = lock.string_value(record[lock_key]);
            if(v1 != v2){
                return resolve(connection);
            }
            let release_update_fields = lock.eval_field_datas(lock.options['release_update_fields']);
            let query = lock.gen_lock_insert_or_update(release_update_fields, 0);
            connection.query(query['sql'], query['values'], function(err, results, fields){
                if(err){
                    reject({connection: connection, err:err});
                }else{
                    resolve(connection);
                }
            });
        }else{
            resolve(connection);
        }
    });
}

MysqlLock.prototype.gen_lock_query = function gen_lock_query(a){
    let table = this.options['table'];
    let primary_fields = this.options['primary_fields'];
    let conditions = this.options['conditions'];
    let select_fields = [];
    let where_clause = [];
    let where_values = [];
    for(let i=0; i<primary_fields.length; i++){
        let e = primary_fields[i];
        select_fields.push('`' + e[0] + '`');
        where_clause.push('`' + e[0] + '`=?');
        where_values.push(e[1]);
    }
    for(let i=0; i<conditions.length; i++){
        let c = conditions[i];
        select_fields.push('`' + c[0] + '`');
    }
    let lock_key = '`' + this.options['lock_key'] + '`';
    if(!select_fields.includes(lock_key)){
        select_fields.push(lock_key);
    }
    let sql = 'select ' + select_fields.join(',') + ' from ' + table + ' where ' + where_clause.join(' and ') + ' limit 1 for update;';
    return {
        sql: sql,
        values: where_values
    }
}

MysqlLock.prototype.gen_lock_insert_or_update = function gen_lock_insert_or_update(update_fields, insert){
    let table = this.options['table'];
    let primary_fields = this.eval_field_datas(this.options['primary_fields']);
    let insert_fields = [];
    let insert_values = [];
    let value_holders = [];
    let update_clause = [];
    let update_values = [];
    let where_clause = [];
    for(let i=0; i<primary_fields.length; i++){
        let e = primary_fields[i];
        insert_fields.push('`' + e[0] + '`');
        insert_values.push(e[1]);
        value_holders.push('?');
        if(!insert){
            where_clause.push('`' + e[0] + '`=?');
        }        
    }
    update_fields = this.eval_field_datas(update_fields);
    for(let i=0; i<update_fields.length; i++){
        let e = update_fields[i];
        if(insert){
            insert_fields.push('`' + e[0] + '`');   
            insert_values.push(e[1]);
            value_holders.push('?')
        }else{
            update_clause.push('`' + e[0] + '`=?');
            update_values.push(e[1]);
        }
    }
    let sql = '';
    let values = insert_values;
    if(insert){
        sql = 'insert into ' + table + ' (' + insert_fields.join(',') + ') values (' + value_holders.join(',') + ');';
    }else{
        sql = 'update ' + table + ' set ' + update_clause.join(',') + ' where ' + where_clause.join(' and ') + ';';
        for(let i=0; i<insert_values.length; i++){
            update_values.push(insert_values[i])
        }
        values = update_values
    } 
    return {
        sql: sql,
        values: values
    }
}

MysqlLock.prototype.eval_field_datas = function eval_field_datas(fields){
    let field_datas = [];
    for(let i=0; i<fields.length; i++){
        let e = fields[i];
        let v = e[1];
        if(typeof v == 'function'){
            v = v();
        }
        field_datas.push([e[0], v]);
    }
    return field_datas;
}

MysqlLock.prototype.string_value = function string_value(v){
    if(v instanceof Date){
        v = utils.LocaleDate(v);
    }
    if(typeof v == 'string'){
        v = '"' + v + '"';
    }
    return v
}

function create_user_lock(ahid, lock_type){
    let options = {
        pool: conn.pool,
        table: 'tbl_user_lock',
        primary_fields: [['ahid', ahid], ['type', lock_type]],
        conditions: [['locked', '==', 0], ['lastupdatetime', '<', utils.LocaleDate(new Date(new Date().getTime() - lock_time_out))]], // just or conditions
        update_fields: [['locked', 1], ['lastupdatetime', function(){return utils.LocaleDate(new Date())}]], 
        release_update_fields: [['locked', 0]],
        lock_key: 'lastupdatetime'
    };
    return new MysqlLock(options);
}


function test(){
    let lock = create_user_lock('1', 2);
    lock.aquire(function(err, data){
        if(err){
            console.log(err);
        }else{
            console.log('do somthing');
            lock.release();
        }
    });
}

// module.exports = test;
module.exports = {
    create_user_lock: create_user_lock,
    MysqlLock: MysqlLock
};
