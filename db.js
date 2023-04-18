const fs = require('fs')
const path = require('path')

const createDatabase = (databaseName) => {
    const paths = path.join(__dirname, databaseName);
    return new Promise((resolve, reject) => {
      if (fs.existsSync(paths)) {
        reject(new Error('Database already exists'));
      } else {
        fs.mkdir(paths, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve('Database created successfully');
          }
        });
      }
    });
};

const createCluster = (databaseName, clustername) => {
    const clusterpath = path.join(databaseName, `${clustername}.json`);
    return new Promise((resolve, reject) => {
      if (fs.existsSync(clusterpath)) {
        reject(new Error('cluster already exists'));
      } else {
        const writeStream = fs.createWriteStream(clusterpath);
        writeStream.on('finish', () => {
          resolve('cluster created successfully');
        });
        writeStream.on('error', (error) => {
          reject({ error, message: "could not create the cluster" });
        });
        writeStream.write(JSON.stringify([]));
        writeStream.end();
      }
    });
  };

function insert(databaseName, clustername, data, allowDuplicate = false) {
  return new Promise((resolve, reject) => {
    const clusterpath = path.join(databaseName, `${clustername}.json`);
    if (!fs.existsSync(clusterpath)) {
      return reject(new Error('Cluster does not exist'));
    }

    const readStream = fs.createReadStream(clusterpath, { encoding: 'utf8' });
    let dataStr = '';

    readStream.on('data', (chunk) => {
      dataStr += chunk;
    });

    readStream.on('end', () => {
      const read = JSON.parse(dataStr);
      const isduplicate = read.some((item) => {
        return JSON.stringify(item) === JSON.stringify(data);
      });

      if (!isduplicate || allowDuplicate) {
        read.push(data);

        const writeStream = fs.createWriteStream(clusterpath);
        writeStream.write(JSON.stringify(read));
        writeStream.end(() => {
          return resolve();
        });
      } else {
        return resolve();
      }
    });

    readStream.on('error', (err) => {
      return reject(new Error(`Failed to read cluster file: ${err.message}`));
    });
  });
}



  function Query(databaseName, clustername, QUERY_FUNCTION) {
    return new Promise((resolve, reject) => {
        const clusterpath = path.join(databaseName, `${clustername}.json`);
        if (!fs.existsSync(clusterpath)) {
            reject(new Error('Cluster does not exist'));
        }

        const readStream = fs.createReadStream(clusterpath);
        let readData = '';

        readStream.on('data', chunk => {
            readData += chunk;
        });

        readStream.on('end', () => {
            const read = JSON.parse(readData);
            const queries = read.filter(QUERY_FUNCTION);
            resolve(queries);
        });

        readStream.on('error', error => {
            reject(new Error(`Error reading data: ${error}`));
        });
    });
}

function update(databaseName, clustername, QUERY_FUNCTION, updatedata) {
    const clusterpath = path.join(databaseName, `${clustername}.json`);
    if (!fs.existsSync(clusterpath)) {
      throw new Error('Cluster does not exist');
    }
  
    const readStream = fs.createReadStream(clusterpath);
    let readData = '';
  
    return new Promise((resolve, reject) => {
      readStream.on('data', chunk => {
        readData += chunk;
      });
  
      readStream.on('end', () => {
        const read = JSON.parse(readData);
        read.forEach(row => {
          if (QUERY_FUNCTION(row)) {
            Object.keys(updatedata).forEach(key => {
              row[key] = updatedata[key];
            });
          }
        });
  
        const writeStream = fs.createWriteStream(clusterpath);
        writeStream.write(JSON.stringify(read));
        writeStream.end();
  
        writeStream.on('finish', () => {
          resolve('Data updated successfully');
        });
  
        writeStream.on('error', error => {
          reject({ error: error, message: 'Failed to write to cluster file' });
        });
      });
  
      readStream.on('error', error => {
        reject({ error: error, message: 'Failed to read cluster file' });
      });
    });
  }

  function deleteData(databaseName, clustername, QUERY_FUNCTION) {
    return new Promise((resolve, reject) => {
      const clusterpath = path.join(databaseName, `${clustername}.json`);
      if (!fs.existsSync(clusterpath)) {
        return reject(new Error('Cluster does not exist'));
      }
  
      const readStream = fs.createReadStream(clusterpath);
      let readData = '';
  
      readStream.on('data', chunk => {
        readData += chunk;
      });
  
      readStream.on('end', () => {
        let read = JSON.parse(readData);
        const deleteQuery = read.filter(row => !QUERY_FUNCTION(row));
        read = deleteQuery;
  
        const writeStream = fs.createWriteStream(clusterpath);
        writeStream.write(JSON.stringify(read));
        writeStream.end();
        writeStream.on('finish', () => {
          resolve('Data deleted successfully');
        });
        writeStream.on('error', error => {
          reject(new Error(`Error deleting data: ${error.message}`));
        });
      });
  
      readStream.on('error', error => {
        reject(new Error(`Error reading data: ${error.message}`));
      });
    });
  }

function searchData(databaseName, clustername, searchElement) {
    return new Promise((resolve, reject) => {
        const clusterpath = path.join(databaseName, `${clustername}.json`);
        if (!fs.existsSync(clusterpath)) {
            reject(new Error('Cluster does not exist'));
        }

        const readStream = fs.createReadStream(clusterpath);
        let readData = '';

        readStream.on('data', chunk => {
            readData += chunk;
        });

        readStream.on('end', () => {
            let read = JSON.parse(readData);
            const results = read.filter(obj => {
                const values = Object.values(obj);
                for (let i = 0; i < values.length; i++) {
                    if ((typeof values[i] === "string" || typeof values[i] === "number") && String(values[i]).includes(searchElement)) {
                        return true;
                    }
                }
                return false;
            });
            resolve(results);
        });

        readStream.on('error', error => {
            reject(new Error(`Error reading data: ${error}`));
        });
    });
}

// createDatabase('mydb').then(()=>{
//     console.log('database created')
// }).catch((err)=>{
//     console.log(err);
// })
// createCluster('mydb','users').then(()=>{console.log('cluster created')}).catch((err)=>{console.log(err);
// })
// insert('mydb','users',{name:'rehul',age:21},false).then(()=>{
//     console.log('data inserted successfully');
// }).catch((err)=>{
//     console.log(err);
// })
// const querys = Query('mydb','users',()=>true).then(queries=>{
//     queries.map(q=>{
//         console.log(q.name);
//     })
// })
// update('mydb','users',data=>data.name==='shashank',{phonenumber:9876543299}).then(()=>{
//     console.log('data updated');
// }).catch((err)=>{console.log(err);
// })
// deleteData('mydb','users',data=>data.name==='sugesh').then(()=>{
//     console.log('data deleted');
// }).catch((e)=>{
//     console.log(e);
// })
// searchData('mydb','users',21).then(result=>{
//     result.map((v)=>{
//         console.log(v);
//     })
// })

