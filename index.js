const shpjs = require('shpjs');
const fs = require('fs');
var shapefile = require('shapefile');
const { Pool, client } = require('pg');
const { features } = require('process');


// constants
const connectionString = 'postgresql://postgres:root@localhost:5432/maps';


function getPGpool() {
  return new Pool({ connectionString: connectionString });
}

function getGeoJson(filePath) {
  return new Promise((resolve, reject) => {
    const geoJson = { type: 'FeatureCollection', features: [] };
    shapefile
      .open(filePath)
      .then((source) =>
        source.read().then(function log(result) {
          if (result.done) {
            resolve(geoJson);
            return;
          }

          geoJson.features.push(result.value);
          return source.read().then(log);
        }),
      )
      .catch((error) => {
        reject(error);
        console.error(error.stack);
      });
  });
}

function getDirByType(dir, keyword) {
  return dir.filter((dir) => {
    return dir.includes(keyword);
  });
}

function getQuery(features,schema ,table) {
  return `INSERT INTO ${schema}.${table}(${Object.keys(
    features.properties,
  )},geom) 
  VALUES(${JSON.stringify(Object.values(features.properties))
    .replace(']', '')
    .replace('[', '')
    .replace(/"/g, "'")}, ST_GeomFromGeoJSON('${JSON.stringify(
    features.geometry,
  )}'))`;
}
async function addIntoDB(pool, json,schema, table) {

  const promises = []
  json.map((row) => {
    return row.features.map(async (features) => {
      try {
        const query =getQuery(features,schema ,table)  
        promises.push( pool.query(query))
      } catch (error) {
          throw error
      }
    });
  });
  return promises
}
/*
**IMP
  Table should be created for the all properties and one more column named geom
fileDirectory -> root directory,
searchKeywod -> unique indentifier in file to filter files
schema -> schema of DB
tableNmae -> name of table   
*/
async function importingShpDataToDB (fileDirectory,searchKeyword,schema,tableName) {
  try {
    console.log("processing started");
    const dir = await fs.readdirSync(fileDirectory);
    const plots = getDirByType(dir, searchKeyword);

    const geoJsonPromises = plots.map((file) => {
      return getGeoJson(`${fileDirectory}/${file}/${file}.shp`);
    });
    const geoJson = await Promise.all(geoJsonPromises);

    const pool = getPGpool();
    
    const allPromises = await addIntoDB(pool, geoJson, schema,tableName)
    return Promise.all(allPromises)
  } catch (er) {
    console.log(er);
  }
}
async function main() {
  await importingShpDataToDB("./files",'row','public','rows')
  await importingShpDataToDB("./files",'plot','public','plots')
  console.log("Done🙇‍♂️");
}
main();
