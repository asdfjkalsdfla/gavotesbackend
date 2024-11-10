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
  const url = `https://results.sos.ga.gov/results/public/api/elections/${countySlug}/${electionID}/ballot-items`;
  // console.log(url);
  // get the current version of the overall result
  const esRequest = await fetch(url);

  if (!esRequest.ok) {
    console.error("Failed to get county file");
  }

  const esData = await esRequest.json();
  const ballotContest = esData.data;
  // console.log(ballotContest);
  const contestOfInterest = ballotContest.filter((contest) =>
    contest.name[0].text.includes(races)
  );
  // console.log(contestOfInterest);
  contestOfInterest.slice(0, 1).forEach((contest) => {
    const contestID = contest.id;
    getContestResults(electionID, countySlug, county, contestID);
  });
};

// ******************************************************
// Get contest Results
// ******************************************************
const getContestResults = async (electionID, countySlug, county, contestID) => {
  const url = `https://results.sos.ga.gov/results/public/api/elections/${countySlug}/${electionID}/ballot-items/${contestID}`;
  // console.log(url);

  // get the current version of the overall result
  const esRequest = await fetch(url);

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
        .toUpperCase()
        .replace("REP", "republican")
        .replace("DEM", "democratic");
      if (party === "") {
        party = candidate.includes("Dem")
          ? "democratic"
          : candidate.includes("Rep")
          ? "republican"
          : "unknown";
      }

      const resultByMode = ballotOption.groupResults;

      // Some counties don't include the mode
      // this tracker lets us know how many details are reported
      let totalVotesByMode = 0;
      resultByMode.forEach((result) => {
        const modeOriginal = result.groupName[0].text;
        const mode = modeOriginal
          .replace("Absentee by Mail", "Absentee by Mail Votes")
          .replace("Advanced Voting", "Advance Voting Votes")
          .replace("Election Day", "Election Day Votes")
          .replace("Provisional", "Provisional Votes");
        const votes = result.voteCount;
        totalVotesByMode = totalVotesByMode + votes; // update counter to get total votes
        if (votes > 0)
          resultFileWriter.write(
            `"${races}","${county}","${precinctId}","${candidate}","${party}","${mode}",${votes}\n`
          );
      });

      if (totalVotesByMode === 0) {
        const votes = ballotOption.voteCount;
        const mode = "Unknown";
        if (votes > 0)
          resultFileWriter.write(
            `"${races}","${county}","${precinctId}","${candidate}","${party}","${mode}",${votes}\n`
          );
      }
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
