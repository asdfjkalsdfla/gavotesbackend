#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const path = require("path");
const http = require("https");
const AdmZip = require("adm-zip");
const xml2js = require("xml2js");
const { exit } = require("process");
// const fetch = require("node-fetch");

const fetchOptions = {
  headers: {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0",
    Accept: "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate,",
    Referer: "https://results.enr.clarityelections.com/GA/116564/web.307039/",
  },
  method: "GET",
};

// Code for the XML Parser
const xmlParser = new xml2js.Parser();

// TODO: find a better way to do this
// Part of the challenges is parties aren't setup consistently in the results
const candidateNameToParty = (candidate) => {
  const candidateCaps = candidate.toUpperCase();
  return candidateCaps.includes("LOEFFLER") ||
    candidateCaps.includes("TRUMP") ||
    candidateCaps.includes("PERDUE") ||
    candidateCaps.includes("KEMP") ||
    candidateCaps.includes("WALKER")
    ? "republican"
    : candidateCaps.includes("BIDEN") ||
      candidateCaps.includes("OSSOFF") ||
      candidateCaps.includes("WARNOCK") ||
      candidateCaps.includes("ABRAMS") ||
      candidateCaps.includes("CLINTON")
    ? "democratic"
    : "other";
};

// Promisify the XML parser
const parseXml = (xml) => {
  return new Promise((resolve, reject) => {
    xmlParser.parseString(xml, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });
  });
};

// ******************************************************
// From the overall election page, get the list of counties and their versions
// ******************************************************
const getListOfCountyResultFiles = async (electionID, state) => {
  // get the current version of the overall result
  const versionRequest = await fetch(
    `https://results.enr.clarityelections.com/${state}/${electionID}/current_ver.txt`,
    fetchOptions
  );

  const version = await versionRequest.text();
  // console.log(version);
  // exit(-1);

  // then pull the settings file which list all counties and their files/versions
  const esRequest = await fetch(
    `https://results.enr.clarityelections.com/${state}/${electionID}/${version}/json/en/electionsettings.json`,
    fetchOptions
  );

  if (esRequest.ok) {
    const esData = await esRequest.json();

    // now download all those files
    // const counties = esData.settings.electiondetails.participatingcounties.slice(59,60);
    const counties = esData.settings.electiondetails.participatingcounties;
    // console.log(counties);
    // exit();

    counties.forEach(async (county) => {
      const split = county.split("|");
      await getXmlPrecinctResultFileForACounty(
        state,
        split[0],
        split[1],
        split[2]
      );
    });
  } else {
    // if we don't get a result, we're likely on the old version.
    // we can get that data too!
    const electionDetailsRequest = await fetch(
      `https://results.enr.clarityelections.com/${state}/${electionID}/${version}/json/details.json`,
      fetchOptions
    );
    const electionDetails = await electionDetailsRequest.json();

    electionDetails.Contests[0].Eid.forEach(async (countyElectionID, index) => {
      const county = electionDetails.Contests[0].P[index].replace(" ", "_");
      const url = `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/current_ver.txt`;
      const versionCountyRequest = await fetch(url, fetchOptions);
      const versionCounty = await versionCountyRequest.text();
      await getXmlPrecinctResultFileForACounty(
        state,
        county,
        countyElectionID,
        versionCounty
      );
    });
  }
};

// ******************************************************
// Download the compressed result file
// ******************************************************
const getXmlPrecinctResultFileForACounty = async (
  state,
  county,
  countyElectionID,
  version
) => {
  // get the latest Version of the county results
  const versionRequest = await fetch(
    `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/current_ver.txt`,
    fetchOptions
  );
  const versionLatest = await versionRequest.text();

  const fileURL = `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/${versionLatest}/reports/detailxml.zip`;
  try {
    const dataFileRequest = await fetch(fileURL, fetchOptions);
    if (!dataFileRequest.ok) {
      // if we get an error here, it's likely because the default results aren't published; go to the JSON method
      getServiceResultsForCounty(state, county, countyElectionID, version);
      return;
    }

    const arrayBufferOfZipFile = await dataFileRequest.arrayBuffer();
    const buffer = Buffer.from(arrayBufferOfZipFile);
    const zip = new AdmZip(buffer);
    const zipEntries = zip.getEntries();
    zipEntries.forEach((file) => {
      if (file.entryName.match(/detail\.xml/))
        parsePrecinctResultFileForACounty(zip.readAsText(file));
    });
  } catch (ex) {
    console.error(ex);
  }
};

// ******************************************************
// Parse the precinct level results from the xml file
// ******************************************************
const parsePrecinctResultFileForACounty = async (fileXML) => {
  const file = await parseXml(fileXML);
  const county = file.ElectionResult.Region[0];

  // console.log(file.ElectionResult.Contest);
  // Go through each contest
  file.ElectionResult.Contest.filter(
    (contest) =>
      contest["$"].text.includes("US Senate") ||
      contest["$"].text.includes("President of the United States") ||
      contest["$"].text.includes("Governor")
  ).forEach((contest) => {
    const resultSet = [];
    if (!contest.Choice) {
      console.log(`ERROR: Did not find candidates in ${county}`);
      return;
    }

    // Hacky way to fix some counties using different names on contest
    const contestName = contest["$"].text
      .split("/")[0]
      .replace("Special Election", "Special");
    // console.log(contestName);

    // pull the results
    contest.Choice.forEach((candidateObject) => {
      const candidate = candidateObject["$"].text;
      // matching based upon candidate name vs. trying to us the rep/dem that sometimes is included in GA
      const party = candidateNameToParty(candidate);
      if (!candidateObject.VoteType) {
        console.log(`ERROR: Did not find vote information in ${county}`);
        return;
      }
      candidateObject.VoteType.forEach((voterTypeObject) => {
        const mode = voterTypeObject["$"].name;
        voterTypeObject.Precinct.forEach((precinctObject) => {
          const countyPrecinct = precinctObject["$"].name;
          const votes = precinctObject["$"].votes;
          resultSet.push({
            contestName,
            county,
            countyPrecinct,
            candidate,
            party,
            mode,
            votes,
          });
        });
      });
    });

    // Add in under votes
    if (!contest.VoteType) {
      console.log(`ERROR: Did not find under/over votes in ${county}`);
      return;
    }
    contest.VoteType.forEach((underOverVotesObject) => {
      underOverVotesObject.Precinct.forEach((precinctObject) => {
        const countyPrecinct = precinctObject["$"].name;
        const votes = precinctObject["$"].votes;
        resultSet.push({
          contestName,
          county,
          countyPrecinct,
          candidate: "Under/Over Votes",
          party: "other",
          mode: "Absentee by Mail Votes",
          votes,
        });
      });
    });

    resultSet
      .filter((result) => result.votes > 0)
      .forEach((result) => {
        resultFileWriter.write(
          `"${result.contestName}","${result.county}","${result.countyPrecinct}","${result.candidate}","${result.party}","${result.mode}",${result.votes}\n`
        );
      });
    // const value = resultSet;
    // console.log(value);
  });
};

const getServiceResultsForCounty = async (
  state,
  county,
  countyElectionID,
  version
) => {
  const responseSummary = await fetch(
    `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/${version}/json/en/summary.json`,
    fetchOptions
  );
  if (!responseSummary.ok) {
    throw Error(responseSummary.statusText);
  }
  const summary = await responseSummary.json();
  const contestMap = new Map();
  const contestByKey = summary.forEach((contest, index) => {
    if (
      contest.C.includes("US Senate") ||
      contest.C.includes("President of the United States") ||
      contest.C.includes("Governor")
    ) {
      contest.C = contest.C.split("/")[0].replace(
        "Special Election",
        "Special"
      );
      contestMap.set(contest.K, contest);
    }
  });

  const resultSet = [];
  const modes = [
    "Provisional Votes",
    "Election Day Votes",
    "Absentee by Mail Votes",
    "Advanced Voting Votes",
  ];
  // const modes = ["Election Day Votes"];
  const modePromises = modes.map(async (mode) => {
    if (mode === "Advanced Voting Votes" && countyElectionID === 114929)
      mode = "Advance Voting Votes";
    const url = `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/${version}/json/${mode.replace(
      /\s/g,
      "_"
    )}.json`;
    const responseByMode = await fetch(url, fetchOptions);
    if (!responseByMode.ok) {
      console.log(`Failed to load ${url}`);
      try {
        console.log(await responseByMode.text());
      } catch (error) {
        console.log(error);
      }
    }
    const resultsByMode = await responseByMode.json();
    resultsByMode.Contests.forEach((precinct) => {
      countyPrecinct = precinct.A;
      // Check if this is the hidden total line
      if (countyPrecinct === "-1") return;
      const contestIDMap = precinct.C;
      precinct.V.forEach((contest, index) => {
        const contestID = contestIDMap[index];
        if (!contestMap.has(contestID)) return;
        const contestInfo = contestMap.get(contestID);
        const contestName = contestInfo.C;
        const contestResults = precinct.V[index];
        contestResults.forEach((votes, candidateIndex) => {
          const candidate = contestInfo.CH[candidateIndex];
          const party = candidateNameToParty(candidate);
          resultSet.push({
            contestName,
            county,
            countyPrecinct,
            candidate,
            party,
            mode,
            votes,
          });
        });
      });
    });
  });
  await Promise.all(modePromises);
  resultSet
    .filter((result) => result.votes > 0)
    .forEach((result) => {
      resultFileWriter.write(
        `"${result.contestName}","${result.county}","${result.countyPrecinct}","${result.candidate}","${result.party}","${result.mode}","${result.votes}"\n`
      );
    });
};

// Parse the command line options and call the results
const cliParser = new ArgumentParser({
  description: "Pull Election Results from Clarity",
});
cliParser.add_argument("-e", "--electionNumber", {
  required: true,
  type: "int",
  help: "The election number from Clarity",
});
cliParser.add_argument("-s", "--state", {
  type: "str",
  default: "GA",
  help: "The State code for Clarity",
});
cliParser.add_argument("-r", "--races", {
  metavar: "N",
  nargs: "+",
  help: "The list of races to pull",
});
cliParser.add_argument("-o", "--outFile", {
  type: "str",
  help: "the output file",
});

const { electionNumber, state, races, outFile } = cliParser.parse_args();

const fileName = outFile
  ? outFile
  : `electionResults_${state}_${electionNumber}.csv`;
fs.mkdirSync(path.dirname(fileName), { recursive: true });
const resultFileWriter = fs.createWriteStream(fileName);

resultFileWriter.write("race,county,precinct,candidate,party,mode,votes\n");
getListOfCountyResultFiles(electionNumber, state);
