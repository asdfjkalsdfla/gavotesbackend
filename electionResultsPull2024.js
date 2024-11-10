#!/usr/bin/env node
const { ArgumentParser } = require("argparse");
const fs = require("fs");
const path = require("path");

// ******************************************************
// From the overall election page, get the list of counties
// ******************************************************
const getListOfCountyResultFiles = async (electionID, state) => {
  // get the current version of the overall result
  const esRequest = await fetch(
    `https://results.sos.ga.gov/results/public/api/jurisdictions/${state}/${electionID}`
  );

  if (!esRequest.ok) {
    console.error("Failed to get summary file");
  }

  const esData = await esRequest.json();
  const counties = esData.childLocalities;
  counties.forEach((county) => {
    const countySlug = county.shortName;
    const countyName = county.name[0].text.replace(" County", "");
    getRelevantContest(electionID, countySlug, countyName);
  });
};

// ******************************************************
// Find the contest id for each country
// ******************************************************
const getRelevantContest = async (electionID, countySlug, county) => {
  // get the current version of the overall result
  const esRequest = await fetch(
    `https://results.sos.ga.gov/results/public/api/elections/${countySlug}/${electionID}/ballot-items`
  );

  if (!esRequest.ok) {
    console.error("Failed to get county file");
  }

  const esData = await esRequest.json();
  const ballotContest = esData.data;
  const contestOfInterest = ballotContest.filter((contest) =>
    contest.name[0].text.includes(races)
  );
  contestOfInterest.slice(0, 1).forEach((contest) => {
    const contestID = contest.id;
    getContestResults(electionID, countySlug, county, contestID);
  });
};

// ******************************************************
// Get contest Results
// ******************************************************
const getContestResults = async (electionID, countySlug, county, contestID) => {
  // get the current version of the overall result
  const esRequest = await fetch(
    `https://results.sos.ga.gov/results/public/api/elections/${countySlug}/${electionID}/ballot-items/${contestID}`
  );

  if (!esRequest.ok) {
    console.error("Failed to get results file");
  }

  const esData = await esRequest.json();
  const precinctResults = esData.breakdownResults;
  precinctResults.forEach((precinct) => {
    const precinctId = precinct.precinct.name[0].text;
    const ballotOptions = precinct.ballotOptions;
    ballotOptions.forEach((ballotOption) => {
      const candidate = ballotOption.name[0].text;
      const partyOriginal = ballotOption.party.name[0].text;
      let party = partyOriginal
        .replace("Rep", "republican")
        .replace("Dem", "democratic");

      const resultByMode = ballotOption.groupResults;
      resultByMode.forEach((result) => {
        const modeOriginal = result.groupName[0].text;
        const mode = modeOriginal
          .replace("Absentee by Mail", "Absentee by Mail Votes")
          .replace("Advanced Voting", "Advance Voting Votes")
          .replace("Election Day", "Election Day Votes")
          .replace("Provisional", "Provisional Votes");
        const votesOG = result.voteCount;
        const votes = votesOG > 0 ? votesOG : 0;
        resultFileWriter.write(
          `"${races}","${county}","${precinctId}","${candidate}","${party}","${mode}",${votes}\n`
        );
      });
    });
  });
};

// Parse the command line options and call the results
const cliParser = new ArgumentParser({
  description: "Pull Election Results from Clarity",
});
cliParser.add_argument("-e", "--electionShortCode", {
  required: true,
  type: "str",
  help: "The election short code",
});
cliParser.add_argument("-s", "--state", {
  type: "str",
  default: "Georgia",
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

const { electionShortCode, state, races, outFile } = cliParser.parse_args();

const fileName = outFile
  ? outFile
  : `electionResults_${state}_${electionShortCode}.csv`;
fs.mkdirSync(path.dirname(fileName), { recursive: true });
const resultFileWriter = fs.createWriteStream(fileName);

resultFileWriter.write("race,county,precinct,candidate,party,mode,votes\n");
getListOfCountyResultFiles(electionShortCode, state);
