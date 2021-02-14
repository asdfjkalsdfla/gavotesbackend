#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const http = require("https");
const AdmZip = require("adm-zip");
const xml2js = require("xml2js");
const fetch = require("node-fetch");
const { exit } = require("process");

// Code for the XML Parser
const xmlParser = new xml2js.Parser();

// TODO: find a better way to do this
// Part of the challenges is parties aren't setup consistently in the results
const candidateNameToParty = (candidate) => {
  const candidateCaps = candidate.toUpperCase();
  return candidateCaps.includes("LOEFFLER") ||
    candidateCaps.includes("TRUMP") ||
    candidateCaps.includes("PERDUE") ||
    candidateCaps.includes("KEMP")
    ? "republican"
    : candidateCaps.includes("BIDEN") ||
      candidateCaps.includes("OSSOFF") ||
      candidateCaps.includes("WARNOCK") ||
      candidateCaps.includes("ABRAMS")
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
    `https://results.enr.clarityelections.com/${state}/${electionID}/current_ver.txt`
  );
  const version = await versionRequest.text();

  // then pull the settings file which list all counties and their files/versions
  const esRequest = await fetch(
    `https://results.enr.clarityelections.com/${state}/${electionID}/${version}/json/en/electionsettings.json`
  );

  if (esRequest.ok) {
    const esData = await esRequest.json();

    // now download all those files
    esData.settings.electiondetails.participatingcounties.forEach((county) => {
      const split = county.split("|");
      getXmlPrecinctResultFileForACounty(state, split[0], split[1], split[2]);
    });
  } else {
    // if we don't get a result, we're likely on the old verison.
    // we can get that data too!
    const electionDetailsRequest = await fetch(
      `https://results.enr.clarityelections.com/${state}/${electionID}/${version}/json/details.json`
    );
    const electionDetails = await electionDetailsRequest.json();
    electionDetails.Contests[0].Eid.forEach(async (countyElectionID,index) => {
      const county = electionDetails.Contests[0].P[index].replace(" ","_");
      const url = `https://results.enr.clarityelections.com/${state}/${county}/${countyElectionID}/current_ver.txt`; 
      const versionCountyRequest = await fetch(url);
      const versionCounty = await versionCountyRequest.text();
      getXmlPrecinctResultFileForACounty(state, county, countyElectionID, versionCounty);
    });
  }
};

// ******************************************************
// Download the compressed result file
// ******************************************************
const getXmlPrecinctResultFileForACounty = (
  state,
  county,
  election,
  version
) => {
  const fileURL = `https://results.enr.clarityelections.com/${state}/${county}/${election}/${version}/reports/detailxml.zip`;
  try {
    // should really re-evaluate suing http get vs. fetch
    http.get(fileURL, function (res) {
      const { statusCode } = res;
      if (statusCode !== 200) {
        // if we get an error here, it's likely because the default results aren't published; go to the JSON method
        getServiceResultsForCounty(state, county, election, version);
        return;
      }
      var data = [],
        dataLen = 0;

      res
        .on("data", function (chunk) {
          data.push(chunk);
          dataLen += chunk.length;
        })
        .on("end", function () {
          var buf = Buffer.alloc(dataLen);

          for (var i = 0, len = data.length, pos = 0; i < len; i++) {
            data[i].copy(buf, pos);
            pos += data[i].length;
          }

          // Unzip the downloaded file
          var zip = new AdmZip(buf);
          var zipEntries = zip.getEntries();
          for (var i = 0; i < zipEntries.length; i++) {
            if (zipEntries[i].entryName.match(/detail\.xml/))
              parsePrecinctResultFileForACounty(zip.readAsText(zipEntries[i]));
          }
        });
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
          `${result.contestName},${result.county},${result.countyPrecinct},${result.candidate},${result.party},${result.mode},${result.votes}\n`
        );
      });
    // const value = resultSet;
    // console.log(value);
  });
};

const getServiceResultsForCounty = async (state, county, election, version) => {
  const responseSummary = await fetch(
    `https://results.enr.clarityelections.com/${state}/${county}/${election}/${version}/json/en/summary.json`
  );
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
    const responseByMode = await fetch(
      `https://results.enr.clarityelections.com/${state}/${county}/${election}/${version}/json/${mode.replace(
        /\s/g,
        "_"
      )}.json`
    );
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
        `${result.contestName},${result.county},${result.countyPrecinct},${result.candidate},${result.party},${result.mode},${result.votes}\n`
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
const resultFileWriter = fs.createWriteStream(fileName);

resultFileWriter.write("race,county,precinct,candidate,party,mode,votes\n");
getListOfCountyResultFiles(electionNumber, state);
