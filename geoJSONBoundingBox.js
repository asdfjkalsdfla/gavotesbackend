#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const path = require("path");
const { bbox } = require("@turf/turf");

const main = async (inputFile, outFile, isPrecinctLevel) => {
  let geoJSONFile = fs.readFileSync(inputFile);
  let geoJSON = JSON.parse(geoJSONFile);
  let boundingBoxes = {};

  geoJSON.features.forEach((county) => {
    // calculate the centroid for each point
    const boundaries = bbox(county);
    const countyID =  county.properties.CTYNAME;
    boundingBoxes[countyID]=boundaries;
  });

  boundingBoxes.STATE = bbox(geoJSON);
  fs.writeFileSync(outFile, JSON.stringify(boundingBoxes));
};

// Parse the command line options and call the results
const cliParser = new ArgumentParser({
  description: "Calculate bounding boxes for each shape in the GeoJSON Files",
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

const { inputFile, outFile } = cliParser.parse_args();
main(inputFile, outFile);
