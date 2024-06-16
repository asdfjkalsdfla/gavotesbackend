#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const path = require("path");
const { centerOfMass } = require("@turf/turf");
const simplify = require("@turf/simplify");

const main = async (inputFile, outFile, isPrecinctLevel) => {
  let geoJSONFile = fs.readFileSync(inputFile);
  let geoJSON = JSON.parse(geoJSONFile);

  let precinctIDtoLabelMap = ["county,precinct,precinctName"];
  let precinctLevelDemographics = [];
  let countyLevelDemographics = new Map();

  // *********************************
  // ADD NEW PROPERTIES
  // *********************************
  geoJSON.features.forEach((precinct) => {
    // updates the county name to uppercase
    if(precinct.properties && precinct.properties.CTYNAME)
      precinct.properties.CTYNAME = precinct.properties.CTYNAME.toUpperCase();

    // If we have a precinct id  use a different ID form
    const id = precinct.properties.PRECINCT_I
      ? `${precinct.properties.CTYNAME.toUpperCase()}##${precinct.properties.PRECINCT_I.toUpperCase()}`
      : precinct.properties.CTYNAME;

    // calculate the centroid for each point
    const centroid = centerOfMass(precinct);

    const propertiesPrior = precinct.properties;

    // get the properties to carry forward
    const properties = {
      id,
      ID: propertiesPrior.ID,
      CNTY: propertiesPrior.CNTY,
      CTYNAME: propertiesPrior.CTYNAME,
      PRECINCT_I: propertiesPrior.PRECINCT_I,
      PRECINCT_N: propertiesPrior.PRECINCT_N,
      centroid: centroid.geometry.coordinates,
    };

    // update the object with the new properties
    precinct.properties = properties;

    // add the info to a map file
    precinctIDtoLabelMap.push(
      `"${precinct.properties.CTYNAME}","${precinct.properties.PRECINCT_I}","${precinct.properties.PRECINCT_N}"`
    );

    // create a demographics object
    const precinctDemographics = {
      id,
      REG20: propertiesPrior.REG20,
      WMREG20: propertiesPrior.WMREG20,
      WFMREG20: propertiesPrior.WFMREG20,
      WUKNREG20: propertiesPrior.WUKNREG20,
      BLMREG20: propertiesPrior.BLMREG20,
      BLFREG20: propertiesPrior.BLFREG20,
      BLUKNREG20: propertiesPrior.BLUKNREG20,
      ASIANMREG2: propertiesPrior.ASIANMREG2,
      ASIANFMREG: propertiesPrior.ASIANFMREG,
      ASIANUKNRE: propertiesPrior.ASIANUKNRE,
      HISPMREG20: propertiesPrior.HISPMREG20,
      HISPFMREG2: propertiesPrior.HISPFMREG2,
      HSPUKNREG2: propertiesPrior.HSPUKNREG2,
      OTHERMREG2: propertiesPrior.OTHERMREG2,
      OTHERFMREG: propertiesPrior.OTHERFMREG,
      OTHERUKNRE: propertiesPrior.OTHERUKNRE,
      UKNMALEREG: propertiesPrior.UKNMALEREG,
      UKNFMREG20: propertiesPrior.UKNFMREG20,
      UKNOWNREG2: propertiesPrior.UKNOWNREG2,
    };
    precinctLevelDemographics.push(precinctDemographics);

    let countyDemographics = {};
    if (!countyLevelDemographics.has(propertiesPrior.CTYNAME)) {
      countyDemographics = {
        ...precinctDemographics,
        id: propertiesPrior.CTYNAME,
        CTYNAME: propertiesPrior.CTYNAME,
      };
    } else {
      prevDemoData = countyLevelDemographics.get(propertiesPrior.CTYNAME);
      countyDemographics = {
        id: propertiesPrior.CTYNAME,
        CTYNAME: propertiesPrior.CTYNAME,
        REG20: (prevDemoData.REG20 || 0) + (propertiesPrior.REG20 || 0),
        WMREG20: (prevDemoData.WMREG20 || 0) + (propertiesPrior.WMREG20 || 0),
        WFMREG20:
          (prevDemoData.WFMREG20 || 0) + (propertiesPrior.WFMREG20 || 0),
        WUKNREG20:
          (prevDemoData.WUKNREG20 || 0) + (propertiesPrior.WUKNREG20 || 0),
        BLMREG20:
          (prevDemoData.BLMREG20 || 0) + (propertiesPrior.BLMREG20 || 0),
        BLFREG20:
          (prevDemoData.BLFREG20 || 0) + (propertiesPrior.BLFREG20 || 0),
        BLUKNREG20:
          (prevDemoData.BLUKNREG20 || 0) + (propertiesPrior.BLUKNREG20 || 0),
        ASIANMREG2:
          (prevDemoData.ASIANMREG2 || 0) + (propertiesPrior.ASIANMREG2 || 0),
        ASIANFMREG:
          (prevDemoData.ASIANFMREG || 0) + (propertiesPrior.ASIANFMREG || 0),
        ASIANUKNRE:
          (prevDemoData.ASIANUKNRE || 0) + (propertiesPrior.ASIANUKNRE || 0),
        HISPMREG20:
          (prevDemoData.HISPMREG20 || 0) + (propertiesPrior.HISPMREG20 || 0),
        HISPFMREG2:
          (prevDemoData.HISPFMREG2 || 0) + (propertiesPrior.HISPFMREG2 || 0),
        HSPUKNREG2:
          (prevDemoData.HSPUKNREG2 || 0) + (propertiesPrior.HSPUKNREG2 || 0),
        OTHERMREG2:
          (prevDemoData.OTHERMREG2 || 0) + (propertiesPrior.OTHERMREG2 || 0),
        OTHERFMREG:
          (prevDemoData.OTHERFMREG || 0) + (propertiesPrior.OTHERFMREG || 0),
        OTHERUKNRE:
          (prevDemoData.OTHERUKNRE || 0) + (propertiesPrior.OTHERUKNRE || 0),
        UKNMALEREG:
          (prevDemoData.UKNMALEREG || 0) + (propertiesPrior.UKNMALEREG || 0),
        UKNFMREG20:
          (prevDemoData.UKNFMREG20 || 0) + (propertiesPrior.UKNFMREG20 || 0),
        UKNOWNREG2:
          (prevDemoData.UKNOWNREG2 || 0) + (propertiesPrior.UKNOWNREG2 || 0),
      };
    }
    countyLevelDemographics.set(propertiesPrior.CTYNAME, countyDemographics);
  });

  // *********************************
  // SPLIT GEO JSON TO COUNTY LEVEL FILES
  // *********************************
  if (isPrecinctLevel) {
    // County level file geo json file
    const dir = path.dirname(outFile);
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
      fs.writeFileSync(
        `${dir}/GA_precincts_2022_${key}_simple.json`,
        JSON.stringify(value)
      );
    });
    fs.writeFileSync(
      `${dir}/GA_precincts_id_to_name.csv`,
      precinctIDtoLabelMap.join("\n")
    );
    fs.writeFileSync(
      `${dir}/demographics-precinct-2020.json`,
      JSON.stringify(precinctLevelDemographics)
    );
    fs.writeFileSync(
      `${dir}/demographics-county-2020.json`,
      JSON.stringify([...countyLevelDemographics.values()])
    );
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
