#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const path = require("path");
const turf = require("@turf/turf");
const simplify = require("@turf/simplify");

const main = async (inputFile, outFile, isPrecinctLevel) => {
  let geoJSONFile = fs.readFileSync(inputFile);
  let geoJSON = JSON.parse(geoJSONFile);

  // *********************************
  // ADD NEW PROPERTIES
  // *********************************
  geoJSON.features.forEach((precinct) => {
    // If we have a precinct id  use a different ID form
    const id = precinct.properties.PRECINCT_I
      ? `${precinct.properties.CTYNAME}##${precinct.properties.PRECINCT_I}`
      : precinct.properties.CTYNAME;

    // calculate the centroid for each point
    const centroid = turf.centerOfMass(precinct);

    // add the new properties
    precinct.properties = {
      id,
      ...precinct.properties,
      centroid: centroid.geometry.coordinates,
    };
  });

  // *********************************
  // SPLIT GEO JSON TO COUNTY LEVEL FILES
  // *********************************
  if (isPrecinctLevel) {
    // County level file geo json file
    const geoJSONBase = { ...geoJSON };
    delete geoJSONBase.features;
    const geoJSONCountyMap = new Map();

    geoJSON.features.forEach((precinct) => {
      const key = precinct.properties.CTYNAME;
      const geoForCounty = geoJSONCountyMap.has(key)
        ? geoJSONCountyMap.get(key)
        : { ...geoJSONBase, features: [] };
      geoForCounty.features.push(precinct);
      geoJSONCountyMap.set(key, geoForCounty);
    });

    geoJSONCountyMap.forEach((value, key) => {
      const newFeatures = value.features.map((precinct) =>
        simplify(precinct, { tolerance: 0.00005, highQuality: true })
      );
      value.features = newFeatures;
      const dir = path.dirname(outFile);
      fs.writeFileSync(
        `${dir}/GA_precincts_2020_${key}_simple.json`,
        JSON.stringify(value)
      );
    });
  }

  // *********************************
  // SIMPLIFY THE GEO JSON FILE
  // *********************************

  // set the tolerance for the overall file
  // using different values for county and precinct level
  let tolerance = !isPrecinctLevel ? 0.00025 : 0.001;

  // simplify the geo json
  let newFeatures = geoJSON.features.map((precinct) =>
    simplify(precinct, { tolerance: 0.001, highQuality: true })
  );
  geoJSON.features = newFeatures;

  fs.writeFileSync(outFile, JSON.stringify(geoJSON));
};

// Parse the command line options and call the results
const cliParser = new ArgumentParser({
  description: "Simplify and split the GeoJSON Files",
});
cliParser.add_argument("-i", "--inputFile", {
  required: true,
  type: "str",
  help: "The input GeoJSON file",
});
cliParser.add_argument("-o", "--outFile", {
  required: true,
  type: "str",
  help: "the output GeoJSON file",
});
cliParser.add_argument("-p", "--precinctLevel", {
  nargs: "*",
  help: "is the file precinct level",
});

const { inputFile, outFile, precinctLevel } = cliParser.parse_args();
const isPrecinctLevel = precinctLevel ? true : false;
main(inputFile, outFile, isPrecinctLevel);
